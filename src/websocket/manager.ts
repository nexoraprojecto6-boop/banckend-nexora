// ============================================================
// NEXORA FOREX — WebSocketManager v2
// ============================================================
// Correcções vs v1:
//   ✅ Respostas com `error` agora REJEITAM a Promise (DerivWsError com
//      .code). Antes resolviam, e o código fazia response.proposal.id →
//      "Cannot read properties of undefined", perdendo o erro real.
//   ✅ Socket antigo já não derruba o novo: o onclose do socket velho
//      punha isConnected=false e parava o heartbeat da ligação nova
//      (mensagens ficavam em fila para sempre após reconnect manual).
//   ✅ UM só ciclo de reconexão (antes onclose + catch lançavam dois
//      attemptReconnect em paralelo → várias ligações; limite é 5).
//   ✅ connect() inicial já não gera unhandledRejection (crashava o Node)
//   ✅ emit('error') só se houver listener (EventEmitter lança excepção
//      se não houver → processo caía)
//   ✅ Sem setInterval de 20 ms: token bucket envia NA HORA e só agenda
//      timer quando há limite (menos latência e CPU)
//   ✅ Pedidos em voo falham imediatamente quando a ligação cai
//      (antes esperavam 15 s pelo timeout)
//   ✅ resubscribeAll não duplica subscrições que ainda estavam na fila
//   ✅ forget não é enfileirado com ids de uma ligação morta
//   ✅ perMessageDeflate desligado (menos CPU/latência por mensagem)
//   ✅ backoff com tecto (30 s) e jitter
// ============================================================

import WebSocket from 'ws';
import { EventEmitter } from 'events';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';
import { nextReqId } from '@utils/req-id.js';

export class DerivWsError extends Error {
  readonly code: string;
  readonly msgType?: string;
  constructor(message: string, code: string, msgType?: string) {
    super(message);
    this.name = 'DerivWsError';
    this.code = code;
    this.msgType = msgType;
  }
}

interface WebSocketSubscription {
  id: string;
  derivSubscriptionId?: string;
  type: string;
  originalPayload: any;
  handler: (data: any) => void;
  expiresAt?: number;
  lastReqId?: number;
}

export interface WebSocketOptions {
  /** URL fixa (ex: WS público). */
  url?: string;
  /**
   * URL nova a cada tentativa de ligação. OBRIGATÓRIO para ligações com
   * OTP (de uso único). Tem prioridade sobre `url`.
   */
  getUrl?: () => Promise<string>;
  autoConnect?: boolean;
  heartbeatInterval?: number;
  reconnectMaxAttempts?: number;
  reconnectDelay?: number;
  rateLimitPerSecond?: number;
}

interface QueuedMessage {
  message: Record<string, any>;
  resolve: (value: any) => void;
  reject: (reason: any) => void;
}

const CONNECT_TIMEOUT_MS = 10_000;
const MAX_RECONNECT_DELAY_MS = 30_000;

function rawToString(data: WebSocket.RawData): string {
  if (typeof data === 'string') return data;
  if (Buffer.isBuffer(data)) return data.toString('utf8');
  if (Array.isArray(data)) return Buffer.concat(data).toString('utf8');
  return Buffer.from(data as ArrayBuffer).toString('utf8');
}

export class WebSocketManager extends EventEmitter {
  private ws: WebSocket | null = null;
  private readonly url?: string;
  private readonly getUrl?: () => Promise<string>;

  private subscriptions = new Map<string, WebSocketSubscription>();
  private derivSubToInternal = new Map<string, string>();
  private pendingSubscriptionLinks = new Map<number, string>();
  private pending = new Map<number, { resolve: (v: any) => void; reject: (e: any) => void }>();
  private queue: QueuedMessage[] = [];

  private isConnected = false;
  private disposed = false;
  private connecting: Promise<void> | null = null;
  private abortConnect: ((err: Error) => void) | null = null;

  private reconnectAttempts = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private heartbeatHandle: NodeJS.Timeout | null = null;
  private drainTimer: NodeJS.Timeout | null = null;
  private lastMessageAt = Date.now();

  // token bucket
  private tokens: number;
  private lastRefill = Date.now();

  private readonly options: Required<Omit<WebSocketOptions, 'url' | 'getUrl'>>;

  constructor(opts: WebSocketOptions) {
    super();

    if (!opts.url && !opts.getUrl) {
      throw new Error('WebSocketManager requer "url" ou "getUrl"');
    }
    this.url = opts.url;
    this.getUrl = opts.getUrl;

    this.options = {
      autoConnect: opts.autoConnect ?? true,
      heartbeatInterval: opts.heartbeatInterval ?? config.websocket.heartbeatInterval,
      reconnectMaxAttempts: opts.reconnectMaxAttempts ?? config.websocket.reconnectMaxAttempts,
      reconnectDelay: opts.reconnectDelay ?? config.websocket.reconnectDelay,
      rateLimitPerSecond: opts.rateLimitPerSecond ?? 80, // Deriv: 100/s
    };
    this.tokens = this.options.rateLimitPerSecond;

    if (this.options.autoConnect) {
      this.connect().catch((err) => {
        logger.error('Ligação inicial falhou — a agendar nova tentativa', { error: err?.message });
        this.scheduleReconnect();
      });
    }
  }

  // ─── Ligação ──────────────────────────────────────────────

  connect(): Promise<void> {
    this.disposed = false;
    if (this.isConnected) return Promise.resolve();
    if (this.connecting) return this.connecting;
    this.connecting = this.doConnect().finally(() => { this.connecting = null; });
    return this.connecting;
  }

  private async doConnect(): Promise<void> {
    // OTP é de uso único: URL nova em CADA tentativa.
    const url = this.getUrl ? await this.getUrl() : this.url!;
    if (this.disposed) throw new Error('WebSocketManager foi desligado');

    return new Promise<void>((resolve, reject) => {
      logger.info('Connecting to WebSocket', { url: url.replace(/otp=[^&]+/, 'otp=***') });

      const socket = new WebSocket(url, {
        perMessageDeflate: false,
        handshakeTimeout: CONNECT_TIMEOUT_MS,
      });
      this.ws = socket;

      let settled = false;
      let timer: NodeJS.Timeout | null = null;

      const finish = () => {
        settled = true;
        if (timer) clearTimeout(timer);
        this.abortConnect = null;
      };
      const fail = (err: Error) => {
        if (settled) return;
        finish();
        socket.on('error', () => {});
        try { socket.terminate(); } catch { /* ignorar */ }
        if (this.ws === socket) this.ws = null;
        reject(err);
      };

      this.abortConnect = fail;
      timer = setTimeout(() => fail(new Error('WebSocket connection timeout')), CONNECT_TIMEOUT_MS + 1_000);

      socket.on('open', () => {
        if (this.ws !== socket) return;
        finish();
        this.isConnected = true;
        this.reconnectAttempts = 0;
        this.lastMessageAt = Date.now();
        this.tokens = this.options.rateLimitPerSecond;
        this.lastRefill = Date.now();
        logger.info('WebSocket connected');
        this.startHeartbeat();
        this.drain();
        this.emit('connected');
        resolve();
      });

      socket.on('message', (data) => {
        if (this.ws !== socket) return;
        this.lastMessageAt = Date.now();
        this.handleMessage(rawToString(data));
      });

      socket.on('error', (err) => {
        logger.error('WebSocket error', { message: err.message, code: (err as any).code });
        if (this.ws !== socket) return;
        if (!settled) fail(err);
        if (this.listenerCount('error') > 0) this.emit('error', err);
      });

      socket.on('close', (code, reasonBuf) => {
        if (this.ws !== socket) return; // socket antigo: ignorar
        const reason = reasonBuf ? reasonBuf.toString() : '';
        if (!settled) {
          fail(new Error(`Ligação fechada antes de abrir (code ${code}${reason ? `: ${reason}` : ''})`));
          return;
        }
        // 1000 normal | 1006 rede/proxy | 1008 OTP inválido/expirado | 1011 erro Deriv
        logger.warn('WebSocket disconnected', { code, reason });
        this.ws = null;
        this.isConnected = false;
        this.stopHeartbeat();
        this.rejectInFlight(new Error(`Ligação fechada (code ${code})`));
        this.emit('disconnected', { code, reason });
        if (!this.disposed) this.scheduleReconnect();
      });
    });
  }

  private scheduleReconnect(delayOverride?: number): void {
    if (this.reconnectTimer || this.disposed) return;

    if (this.reconnectAttempts >= this.options.reconnectMaxAttempts) {
      logger.error('Max reconnection attempts reached', { attempts: this.reconnectAttempts });
      this.emit('reconnect_failed');
      return;
    }

    const base = Math.min(
      this.options.reconnectDelay * Math.pow(2, this.reconnectAttempts),
      MAX_RECONNECT_DELAY_MS,
    );
    const delay = delayOverride ?? Math.round(base * (0.8 + Math.random() * 0.4));
    this.reconnectAttempts++;

    logger.info('Attempting to reconnect', {
      attempt: this.reconnectAttempts,
      delay: `${delay}ms`,
      usingFreshUrl: Boolean(this.getUrl),
    });

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect()
        .then(() => {
          this.resubscribeAll();
          this.emit('reconnected');
        })
        .catch((err: any) => {
          logger.error('Reconnection failed', { error: err?.message, attempt: this.reconnectAttempts });
          this.scheduleReconnect();
        });
    }, delay);
  }

  /** Reconexão manual (ex: heartbeat falhou). */
  reconnect(): void {
    this.dropSocket('reconnect manual');
    this.reconnectAttempts = 0;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.scheduleReconnect(0);
  }

  private dropSocket(reason: string): void {
    const socket = this.ws;
    this.ws = null;
    this.isConnected = false;
    this.stopHeartbeat();
    this.abortConnect?.(new Error(`Ligação abortada: ${reason}`));
    this.rejectInFlight(new Error(`Ligação fechada: ${reason}`));
    if (!socket) return;
    socket.removeAllListeners();
    socket.on('error', () => {}); // evita uncaught se ainda CONNECTING
    try { socket.terminate(); } catch { /* ignorar */ }
  }

  private rejectInFlight(err: Error): void {
    for (const [, p] of this.pending) p.reject(err);
    this.pending.clear();
    this.pendingSubscriptionLinks.clear();
  }

  // ─── Recepção ─────────────────────────────────────────────

  private handleMessage(data: string): void {
    let message: any;
    try {
      message = JSON.parse(data);
    } catch (error) {
      logger.error('Failed to parse WebSocket message', { error });
      return;
    }

    const reqId = typeof message.req_id === 'number' ? message.req_id : undefined;
    let handledAsPending = false;

    if (reqId !== undefined) {
      const entry = this.pending.get(reqId);
      if (entry) {
        this.pending.delete(reqId);
        handledAsPending = true;
        if (message.error) {
          this.pendingSubscriptionLinks.delete(reqId);
          entry.reject(new DerivWsError(
            message.error.message ?? 'Erro Deriv',
            String(message.error.code ?? 'UNKNOWN'),
            message.msg_type,
          ));
        } else {
          if (message.subscription?.id) this.linkDerivSubscriptionId(reqId, message.subscription.id);
          entry.resolve(message);
        }
      }
    }

    if (message.error) {
      logger.warn('[Deriv] Mensagem de erro recebida', {
        code: message.error.code,
        message: message.error.message,
        req_id: message.req_id,
        msg_type: message.msg_type,
      });
    }

    // ping não solicitado: ignorar (lastMessageAt já actualizado)
    if (!handledAsPending && message.msg_type === 'ping') return;

    if (message.subscription?.id) {
      const internalId = this.derivSubToInternal.get(message.subscription.id);
      const sub = internalId ? this.subscriptions.get(internalId) : undefined;
      if (sub) {
        try {
          sub.handler(message);
        } catch (error) {
          logger.error('Subscription handler failed', { type: sub.type, error });
        }
      }
    }

    if (this.listenerCount('message') > 0) this.emit('message', message);
  }

  private linkDerivSubscriptionId(reqId: number, derivSubId: string): void {
    const internalId = this.pendingSubscriptionLinks.get(reqId);
    if (!internalId) return;
    this.pendingSubscriptionLinks.delete(reqId);
    const sub = this.subscriptions.get(internalId);
    if (sub) {
      sub.derivSubscriptionId = derivSubId;
      this.derivSubToInternal.set(derivSubId, internalId);
    }
  }

  // ─── Envio ────────────────────────────────────────────────

  send(payload: any, timeout = 15_000): Promise<any> {
    return new Promise((resolve, reject) => {
      const reqId: number = Number.isInteger(payload.req_id) && payload.req_id > 0
        ? payload.req_id
        : nextReqId();
      const message = { ...payload, req_id: reqId };

      const timer = setTimeout(() => {
        this.pending.delete(reqId);
        this.queue = this.queue.filter((q) => q.message.req_id !== reqId);
        reject(new Error(`Request ${reqId} timed out${this.isConnected ? '' : ' (ligação indisponível)'}`));
      }, timeout);

      const entry: QueuedMessage = {
        message,
        resolve: (v) => { clearTimeout(timer); resolve(v); },
        reject: (e) => { clearTimeout(timer); reject(e); },
      };

      this.queue.push(entry);
      if (this.isConnected) this.drain();
      else logger.warn('WebSocket not connected, message queued', { reqId });
    });
  }

  private refill(): void {
    const now = Date.now();
    const rate = this.options.rateLimitPerSecond;
    this.tokens = Math.min(rate, this.tokens + ((now - this.lastRefill) / 1000) * rate);
    this.lastRefill = now;
  }

  private drain(): void {
    if (!this.isConnected || !this.ws) return;
    this.refill();

    while (this.queue.length > 0 && this.tokens >= 1) {
      const item = this.queue.shift()!;
      this.tokens -= 1;
      this.dispatch(item);
    }

    if (this.queue.length > 0 && !this.drainTimer) {
      const wait = Math.max(1, Math.ceil(((1 - this.tokens) / this.options.rateLimitPerSecond) * 1000));
      this.drainTimer = setTimeout(() => {
        this.drainTimer = null;
        this.drain();
      }, wait);
    }
  }

  private dispatch(item: QueuedMessage): void {
    const socket = this.ws;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      this.queue.unshift(item); // volta à frente, envia na reconexão
      return;
    }
    const reqId = item.message.req_id as number;
    this.pending.set(reqId, { resolve: item.resolve, reject: item.reject });
    try {
      socket.send(JSON.stringify(item.message));
    } catch (error) {
      this.pending.delete(reqId);
      logger.error('Failed to send WebSocket message', { error });
      item.reject(error);
    }
  }

  // ─── Subscrições ──────────────────────────────────────────

  subscribe(type: string, payload: any, handler: (data: any) => void): string {
    const internalId = `sub_${nextReqId()}`;
    const reqId = nextReqId();

    const subscription: WebSocketSubscription = {
      id: internalId,
      type,
      handler,
      originalPayload: { ...payload, subscribe: 1 },
      expiresAt: Date.now() + 24 * 60 * 60 * 1000,
      lastReqId: reqId,
    };

    this.subscriptions.set(internalId, subscription);
    this.pendingSubscriptionLinks.set(reqId, internalId);

    this.send({ ...payload, subscribe: 1, req_id: reqId }).catch((error: any) => {
      logger.error('Subscription failed', { error: error?.message, type });
      this.pendingSubscriptionLinks.delete(reqId);
      // Só descarta se a Deriv a rejeitou. Falha de ligação/timeout:
      // mantém e o resubscribeAll() refaz na reconexão.
      if (error instanceof DerivWsError) this.subscriptions.delete(internalId);
    });

    logger.info('WebSocket subscription created', { internalId, type });
    return internalId;
  }

  async unsubscribe(subscriptionId: string): Promise<void> {
    const sub = this.subscriptions.get(subscriptionId);
    if (!sub) {
      logger.warn('Subscription not found', { subscriptionId });
      return;
    }

    this.subscriptions.delete(subscriptionId);
    const serverId = sub.derivSubscriptionId;
    if (serverId) this.derivSubToInternal.delete(serverId);

    // Só envia forget se a ligação está viva (ids da ligação antiga não valem)
    if (serverId && this.isConnected) {
      try {
        await this.send({ forget: serverId });
      } catch (error) {
        logger.error('Failed to unsubscribe', { error, subscriptionId });
        throw error;
      }
    }
    logger.info('WebSocket subscription removed', { subscriptionId });
  }

  async unsubscribeAll(type?: string): Promise<void> {
    const all = Array.from(this.subscriptions.values());
    const targets = type ? all.filter((s) => s.type === type) : all;
    const types = type ? [type] : Array.from(new Set(all.map((s) => s.type)));

    targets.forEach((s) => {
      if (s.derivSubscriptionId) this.derivSubToInternal.delete(s.derivSubscriptionId);
      this.subscriptions.delete(s.id);
    });

    if (types.length > 0 && this.isConnected) {
      try {
        await this.send({ forget_all: types });
      } catch (error) {
        logger.error('Failed to unsubscribe all', { error });
        throw error;
      }
    }
    logger.info('WebSocket subscriptions removed', { types, count: targets.length });
  }

  private resubscribeAll(): void {
    if (this.subscriptions.size === 0) return;
    this.derivSubToInternal.clear();
    logger.info('Re-subscribing all active subscriptions', { count: this.subscriptions.size });

    for (const [internalId, sub] of this.subscriptions) {
      // pedido original ainda na fila/em voo → não duplicar
      const inFlight = sub.lastReqId !== undefined && (
        this.pending.has(sub.lastReqId) ||
        this.queue.some((q) => q.message.req_id === sub.lastReqId)
      );
      if (inFlight && !sub.derivSubscriptionId) continue;

      sub.derivSubscriptionId = undefined;
      const reqId = nextReqId();
      sub.lastReqId = reqId;
      this.pendingSubscriptionLinks.set(reqId, internalId);

      this.send({ ...sub.originalPayload, req_id: reqId }).catch((err: any) => {
        logger.error('Re-subscribe failed', { internalId, type: sub.type, error: err?.message });
        this.pendingSubscriptionLinks.delete(reqId);
        if (err instanceof DerivWsError) this.subscriptions.delete(internalId);
      });
    }
  }

  // ─── Heartbeat ────────────────────────────────────────────

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeatHandle = setInterval(() => {
      if (!this.isConnected) return;
      const before = this.lastMessageAt;
      this.send({ ping: 1 }, 8_000).catch((error: any) => {
        // chegaram outras mensagens entretanto → ligação viva, ping perdido
        if (this.lastMessageAt > before + 1) return;
        logger.warn('Heartbeat failed, reconnecting...', {
          error: error?.message,
          idleMs: Date.now() - this.lastMessageAt,
        });
        this.reconnect();
      });
    }, this.options.heartbeatInterval);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatHandle) {
      clearInterval(this.heartbeatHandle);
      this.heartbeatHandle = null;
    }
  }

  // ─── Estado / limpeza ─────────────────────────────────────

  disconnect(): void {
    this.disposed = true;
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    if (this.drainTimer) { clearTimeout(this.drainTimer); this.drainTimer = null; }
    this.dropSocket('disconnect');
    const err = new Error('WebSocketManager desligado');
    this.queue.splice(0).forEach((q) => q.reject(err));
    logger.info('WebSocket disconnected');
  }

  getStatus() {
    return {
      isConnected: this.isConnected,
      subscriptions: this.subscriptions.size,
      pendingRequests: this.pending.size,
      messageQueue: this.isConnected ? 0 : this.queue.length,
      sendQueueDepth: this.isConnected ? this.queue.length : 0,
      reconnectAttempts: this.reconnectAttempts,
      idleMs: Date.now() - this.lastMessageAt,
    };
  }

  getActiveSubscriptions(): Array<{ id: string; type: string; derivSubscriptionId?: string }> {
    return Array.from(this.subscriptions.values()).map((s) => ({
      id: s.id,
      type: s.type,
      derivSubscriptionId: s.derivSubscriptionId,
    }));
  }

  cleanupExpired(): void {
    const now = Date.now();
    const expired = Array.from(this.subscriptions.values())
      .filter((s) => s.expiresAt && s.expiresAt < now)
      .map((s) => s.id);

    // unsubscribe envia forget (antes só apagava localmente e a
    // subscrição ficava viva na Deriv)
    expired.forEach((id) => { this.unsubscribe(id).catch(() => {}); });

    if (expired.length > 0) logger.info('Expired subscriptions cleaned up', { count: expired.length });
  }
}
