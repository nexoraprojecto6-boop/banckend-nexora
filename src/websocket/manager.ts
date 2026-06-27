import WebSocket from 'ws';
import { EventEmitter } from 'events';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';

// ─── req_id numérico incremental ─────────────────────────────
// CRÍTICO: a Deriv rejeita req_id como string (UUID).
// Usar contador global garante unicidade entre manager e adapter.
// Começa em 1 para evitar colisão com o nextReqId do server.ts
// (que começa em 1000) — mas o range é diferente o suficiente.
let _globalReqId = 0;
function nextManagerReqId(): number {
  return ++_globalReqId;
}

interface WebSocketSubscription {
  id: string;
  derivSubscriptionId?: string;
  type: string;
  // originalPayload guardado para re-subscribe após reconexão
  originalPayload: any;
  handler: (data: any) => void;
  expiresAt?: number;
}

export interface WebSocketOptions {
  /**
   * URL fixa para a ligação. Use isto quando a URL não expira /
   * não depende de OTP de uso único (ex: WS público da Deriv).
   */
  url?: string;
  /**
   * Callback assíncrono que devolve uma URL fresca para cada
   * tentativa de ligação (inicial ou reconnect). OBRIGATÓRIO para
   * ligações autenticadas via OTP da Deriv — o OTP é de uso único,
   * por isso reutilizar a mesma URL num reconnect faz a Deriv
   * aceitar o handshake e fechar a ligação imediatamente a seguir
   * (sintoma: "autentica e cai logo depois", em ciclo).
   * Se fornecido, tem prioridade sobre `url`.
   */
  getUrl?: () => Promise<string>;
  autoConnect?: boolean;
  heartbeatInterval?: number;
  reconnectMaxAttempts?: number;
  reconnectDelay?: number;
  rateLimitPerSecond?: number;
}

interface QueuedSend {
  message: any;
  resolve: (value: any) => void;
  reject: (reason: any) => void;
  timeout: number;
}

export class WebSocketManager extends EventEmitter {
  private ws: WebSocket | null = null;
  private url?: string;
  private getUrl?: () => Promise<string>;
  private subscriptions = new Map<string, WebSocketSubscription>();
  private derivSubToInternal = new Map<string, string>();
  private pendingRequests = new Map<string, { resolve: Function; reject: Function }>();
  private messageQueue: Array<{ message: any; resolve: (v: any) => void; reject: (e: any) => void; timeout: number }> = [];
  private isConnected = false;
  private reconnectAttempts = 0;
  private isManualReconnect = false;
  private heartbeatIntervalHandle: NodeJS.Timeout | null = null;
  private connectionTimeout: NodeJS.Timeout | null = null;
  private lastMessageAt = Date.now();
  private readonly options: Required<Omit<WebSocketOptions, 'rateLimitPerSecond' | 'url' | 'getUrl'>> & { rateLimitPerSecond: number };

  private sendQueue: QueuedSend[] = [];
  private rateLimiterInterval: NodeJS.Timeout | null = null;
  private rateLimiterDrainInterval: NodeJS.Timeout | null = null;
  private sentInCurrentWindow = 0;

  // Mapa reqId → internalSubscriptionId (para linkar serverId)
  private pendingSubscriptionLinks = new Map<string, string>();

  constructor(options: WebSocketOptions) {
    super();

    if (!options.url && !options.getUrl) {
      throw new Error('WebSocketManager requer "url" ou "getUrl"');
    }

    this.url = options.url;
    this.getUrl = options.getUrl;

    this.options = {
      autoConnect: options.autoConnect ?? true,
      heartbeatInterval: options.heartbeatInterval ?? config.websocket.heartbeatInterval,
      reconnectMaxAttempts: options.reconnectMaxAttempts ?? config.websocket.reconnectMaxAttempts,
      reconnectDelay: options.reconnectDelay ?? config.websocket.reconnectDelay,
      rateLimitPerSecond: options.rateLimitPerSecond ?? 50,
    };

    this.startRateLimiter();

    if (this.options.autoConnect) {
      this.connect();
    }
  }

  async connect(): Promise<void> {
    // Resolve a URL a usar NESTA tentativa. Se getUrl foi fornecido,
    // chamamo-lo SEMPRE — inclusive em reconnects — porque um OTP da
    // Deriv é de uso único: reutilizar a URL antiga faria a Deriv
    // aceitar o handshake e fechar a ligação de imediato a seguir.
    let connectUrl: string;
    try {
      connectUrl = this.getUrl ? await this.getUrl() : this.url!;
    } catch (error) {
      logger.error('Falha ao obter URL de ligação (getUrl)', { error });
      throw error;
    }

    return new Promise((resolve, reject) => {
      try {
        logger.info('Connecting to WebSocket', { url: connectUrl.replace(/otp=[^&]+/, 'otp=***') });

        this.ws = new WebSocket(connectUrl);

        this.connectionTimeout = setTimeout(() => {
          if (!this.isConnected) {
            logger.error('WebSocket connection timeout — fechando socket pendente');
            this.ws?.close();
            reject(new Error('WebSocket connection timeout'));
          }
        }, 10_000);

        this.ws.onopen = () => {
          clearTimeout(this.connectionTimeout!);
          this.isConnected = true;
          this.reconnectAttempts = 0;
          this.lastMessageAt = Date.now();
          logger.info('WebSocket connected');
          this.startHeartbeat();
          this.flushMessageQueue();
          this.emit('connected');
          resolve();
        };

        this.ws.onmessage = (event) => {
          this.lastMessageAt = Date.now();
          const raw = typeof event.data === 'string'
            ? event.data
            : event.data instanceof Buffer
              ? event.data.toString()
              : Buffer.from(event.data as ArrayBuffer).toString();
          this.handleMessage(raw);
        };

        this.ws.onerror = (event) => {
          logger.error('WebSocket error', {
            message: (event as any).message,
            error: (event as any).error?.message,
            code: (event as any).error?.code,
          });
          this.emit('error', event);
        };

        // ── DIAGNÓSTICO CRÍTICO ──────────────────────────────
        // Códigos comuns que explicam "desconecta logo a seguir":
        //   1000  Normal closure (servidor decidiu fechar)
        //   1006  Abnormal closure (rede/proxy cortou, sem close frame)
        //   1008  Policy violation (OTP já consumido/expirado)
        //   1011  Internal server error (do lado da Deriv)
        (this.ws as any).onclose = (event: any) => {
          const code = event?.code;
          const reason = event?.reason ? event.reason.toString() : '';
          this.isConnected = false;
          this.stopHeartbeat();
          logger.warn('WebSocket disconnected', {
            code,
            reason,
            wasClean: event?.wasClean,
            url: connectUrl.replace(/otp=[^&]+/, 'otp=***'),
          });
          this.emit('disconnected', { code, reason });

          if (this.isManualReconnect) {
            this.isManualReconnect = false;
            return;
          }

          this.attemptReconnect();
        };
      } catch (error) {
        logger.error('Failed to create WebSocket', { error });
        reject(error);
      }
    });
  }

  private handleMessage(data: string) {
    try {
      const message = JSON.parse(data);

      if (message.error) {
        logger.warn('[Deriv] Mensagem de erro recebida', {
          code: message.error.code,
          message: message.error.message,
          req_id: message.req_id,
          msg_type: message.msg_type,
        });
      }

      // ── ORDEM CRÍTICA ────────────────────────────────────
      // Resolver por req_id PRIMEIRO.
      // Se verificássemos msg_type === 'ping' antes de req_id,
      // a resposta ao nosso próprio { ping: 1 } seria desviada
      // e a Promise do heartbeat nunca resolvia → reconexão em ciclo.
      let resolvedAsPending = false;
      if (message.req_id != null) {
        const key = String(message.req_id);
        const pending = this.pendingRequests.get(key);
        if (pending) {
          if (message.subscription?.id) {
            this.linkDerivSubscriptionId(key, message.subscription.id);
          }
          pending.resolve(message);
          this.pendingRequests.delete(key);
          resolvedAsPending = true;
        }
      }

      // ── PING DA DERIV ────────────────────────────────────
      // A Deriv privada envia { msg_type: 'ping' } para verificar
      // se a ligação está viva. Não precisamos responder com pong —
      // a Deriv não espera isso. Basta ignorar (o lastMessageAt já
      // foi actualizado acima). NÃO enviar { pong: 1 } aqui.
      if (!resolvedAsPending && message.msg_type === 'ping') {
        // Não faz nada — só actualizar lastMessageAt (já feito em onmessage)
        return;
      }

      if (message.subscription?.id) {
        const internalId = this.derivSubToInternal.get(message.subscription.id);
        const subscription = internalId ? this.subscriptions.get(internalId) : undefined;
        if (subscription) {
          subscription.handler(message);
        }
      }

      this.emit('message', message);
    } catch (error) {
      logger.error('Failed to parse WebSocket message', { error, data });
    }
  }

  private linkDerivSubscriptionId(reqId: string, derivSubId: string) {
    const internalId = this.pendingSubscriptionLinks.get(reqId);
    if (!internalId) return;
    const sub = this.subscriptions.get(internalId);
    if (sub) {
      sub.derivSubscriptionId = derivSubId;
      this.derivSubToInternal.set(derivSubId, internalId);
    }
    this.pendingSubscriptionLinks.delete(reqId);
  }

  send(payload: any, timeout = 15_000): Promise<any> {
    return new Promise((resolve, reject) => {
      // CRÍTICO: req_id DEVE ser numérico inteiro positivo.
      // A Deriv rejeita UUIDs e strings — nunca usar uuidv4() aqui.
      const reqId = (typeof payload.req_id === 'number' && payload.req_id > 0)
        ? payload.req_id
        : nextManagerReqId();

      const message = { ...payload, req_id: reqId };

      const timeoutId = setTimeout(() => {
        this.pendingRequests.delete(String(reqId));
        this.messageQueue = this.messageQueue.filter((m) => m.message.req_id !== reqId);
        this.sendQueue = this.sendQueue.filter((q) => q.message.req_id !== reqId);
        reject(new Error(`Request ${reqId} timed out${this.isConnected ? '' : ' (ligação indisponível)'}`));
      }, timeout);

      const wrappedResolve = (data: any) => { clearTimeout(timeoutId); resolve(data); };
      const wrappedReject  = (err: any)  => { clearTimeout(timeoutId); reject(err);   };

      if (!this.isConnected) {
        this.messageQueue.push({ message, resolve: wrappedResolve, reject: wrappedReject, timeout });
        logger.warn('WebSocket not connected, message queued', { reqId });
        return;
      }

      this.sendQueue.push({ message, resolve: wrappedResolve, reject: wrappedReject, timeout });
    });
  }

  private startRateLimiter() {
    const intervalMs = Math.max(1, Math.floor(1000 / this.options.rateLimitPerSecond));

    this.rateLimiterInterval = setInterval(() => {
      this.sentInCurrentWindow = 0;
      this.drainSendQueue();
    }, 1000);

    this.rateLimiterDrainInterval = setInterval(() => this.drainSendQueue(), intervalMs);
  }

  private drainSendQueue() {
    while (
      this.sendQueue.length > 0 &&
      this.sentInCurrentWindow < this.options.rateLimitPerSecond &&
      this.isConnected
    ) {
      const item = this.sendQueue.shift()!;
      this.dispatchSend(item);
      this.sentInCurrentWindow++;
    }
  }

  private dispatchSend(item: QueuedSend) {
    const { message, resolve, reject } = item;
    try {
      this.ws?.send(JSON.stringify(message));
      this.pendingRequests.set(String(message.req_id), { resolve, reject });
    } catch (error) {
      logger.error('Failed to send WebSocket message', { error });
      reject(error);
    }
  }

  subscribe(
    type: string,
    payload: any,
    handler: (data: any) => void,
  ): string {
    // CORRECÇÃO: req_id numérico, gerado AQUI e passado ao send()
    // para que o pendingSubscriptionLinks use o mesmo key que
    // o pendingRequests vai resolver.
    const internalId = `sub_${nextManagerReqId()}`;
    const reqId = nextManagerReqId();

    const subscription: WebSocketSubscription = {
      id: internalId,
      type,
      handler,
      originalPayload: { ...payload, subscribe: 1 },
      expiresAt: Date.now() + 24 * 60 * 60 * 1000,
    };

    this.subscriptions.set(internalId, subscription);
    // Registar ANTES de send() para que linkDerivSubscriptionId
    // encontre a entrada quando a resposta chegar.
    this.pendingSubscriptionLinks.set(String(reqId), internalId);

    this.send({ ...payload, subscribe: 1, req_id: reqId }).catch((error: any) => {
      logger.error('Subscription failed', { error, type });
      this.subscriptions.delete(internalId);
      this.pendingSubscriptionLinks.delete(String(reqId));
    });

    logger.info('WebSocket subscription created', { internalId, type });
    return internalId;
  }

  async unsubscribe(subscriptionId: string): Promise<void> {
    const subscription = this.subscriptions.get(subscriptionId);
    if (!subscription) {
      logger.warn('Subscription not found', { subscriptionId });
      return;
    }

    try {
      if (subscription.derivSubscriptionId) {
        await this.send({ forget: subscription.derivSubscriptionId });
        this.derivSubToInternal.delete(subscription.derivSubscriptionId);
      }
      this.subscriptions.delete(subscriptionId);
      logger.info('WebSocket subscription removed', { subscriptionId });
    } catch (error) {
      logger.error('Failed to unsubscribe', { error, subscriptionId });
      throw error;
    }
  }

  async unsubscribeAll(type?: string): Promise<void> {
    try {
      const allSubs = Array.from(this.subscriptions.values());
      const targets = type ? allSubs.filter(s => s.type === type) : allSubs;
      const types = type
        ? [type]
        : Array.from(new Set(allSubs.map((s) => s.type)));

      if (types.length > 0) {
        await this.send({ forget_all: types });
      }

      targets.forEach(s => {
        if (s.derivSubscriptionId) this.derivSubToInternal.delete(s.derivSubscriptionId);
        this.subscriptions.delete(s.id);
      });

      logger.info('WebSocket subscriptions removed', { types, count: targets.length });
    } catch (error) {
      logger.error('Failed to unsubscribe all', { error });
      throw error;
    }
  }

  // ── Re-subscribe após reconexão ───────────────────────────
  // Reenvia todos os originalPayload das subscrições activas com
  // novos req_ids. Necessário depois de abrir novo WS com novo OTP.
  private resubscribeAll() {
    if (this.subscriptions.size === 0) return;

    logger.info('Re-subscribing all active subscriptions', { count: this.subscriptions.size });

    for (const [internalId, sub] of this.subscriptions) {
      // Limpar serverId antigo — vai chegar um novo
      sub.derivSubscriptionId = undefined;

      const reqId = nextManagerReqId();
      this.pendingSubscriptionLinks.set(String(reqId), internalId);

      this.send({ ...sub.originalPayload, req_id: reqId }).catch((err: any) => {
        logger.error('Re-subscribe failed', { internalId, type: sub.type, error: err.message });
        this.pendingSubscriptionLinks.delete(String(reqId));
      });
    }
  }

  private startHeartbeat() {
    this.heartbeatIntervalHandle = setInterval(() => {
      if (this.isConnected) {
        const idleMs = Date.now() - this.lastMessageAt;
        logger.debug('Heartbeat tick', { idleMs });

        this.send({ ping: 1 }, 8_000).catch((error: any) => {
          logger.warn('Heartbeat failed, reconnecting...', {
            error: error?.message,
            idleMs: Date.now() - this.lastMessageAt,
          });
          this.reconnect();
        });
      }
    }, this.options.heartbeatInterval);

    logger.debug('Heartbeat started', { interval: this.options.heartbeatInterval });
  }

  private stopHeartbeat() {
    if (this.heartbeatIntervalHandle) {
      clearInterval(this.heartbeatIntervalHandle);
      this.heartbeatIntervalHandle = null;
    }
  }

  private attemptReconnect() {
    if (this.reconnectAttempts >= this.options.reconnectMaxAttempts) {
      logger.error('Max reconnection attempts reached', { attempts: this.reconnectAttempts });
      this.emit('reconnect_failed');
      return;
    }

    const delay = this.options.reconnectDelay * Math.pow(2, this.reconnectAttempts);
    this.reconnectAttempts++;

    logger.info('Attempting to reconnect', {
      attempt: this.reconnectAttempts,
      delay: `${delay}ms`,
      usingFreshUrl: Boolean(this.getUrl),
    });

    setTimeout(() => {
      this.connect().then(() => {
        // Reenviar subscrições activas com novos req_ids
        this.resubscribeAll();
        this.emit('reconnected');
      }).catch((error: any) => {
        logger.error('Reconnection failed', { error: error?.message, attempt: this.reconnectAttempts });
        this.attemptReconnect();
      });
    }, delay);
  }

  reconnect() {
    if (this.ws) {
      this.isManualReconnect = true;
      this.ws.close();
    }
    this.reconnectAttempts = 0;
    this.connect().then(() => {
      this.resubscribeAll();
      this.emit('reconnected');
    }).catch((err: any) => {
      logger.error('Manual reconnect failed', { error: err?.message });
      this.attemptReconnect();
    });
  }

  private flushMessageQueue() {
    while (this.messageQueue.length > 0 && this.isConnected) {
      const item = this.messageQueue.shift()!;
      this.sendQueue.push({
        message: item.message,
        resolve: item.resolve,
        reject: item.reject,
        timeout: item.timeout,
      });
      logger.debug('Queued message moved to send queue', { reqId: item.message.req_id });
    }
  }

  disconnect() {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.stopHeartbeat();
    if (this.rateLimiterInterval) {
      clearInterval(this.rateLimiterInterval);
      this.rateLimiterInterval = null;
    }
    if (this.rateLimiterDrainInterval) {
      clearInterval(this.rateLimiterDrainInterval);
      this.rateLimiterDrainInterval = null;
    }
    this.isConnected = false;
    logger.info('WebSocket disconnected');
  }

  getStatus() {
    return {
      isConnected: this.isConnected,
      subscriptions: this.subscriptions.size,
      pendingRequests: this.pendingRequests.size,
      messageQueue: this.messageQueue.length,
      sendQueueDepth: this.sendQueue.length,
      reconnectAttempts: this.reconnectAttempts,
      idleMs: Date.now() - this.lastMessageAt,
    };
  }

  getActiveSubscriptions(): Array<{ id: string; type: string; derivSubscriptionId?: string }> {
    return Array.from(this.subscriptions.values()).map(s => ({
      id: s.id,
      type: s.type,
      derivSubscriptionId: s.derivSubscriptionId,
    }));
  }

  cleanupExpired() {
    const now = Date.now();
    const expired = Array.from(this.subscriptions.entries())
      .filter(([_, sub]) => sub.expiresAt && sub.expiresAt < now)
      .map(([id]) => id);

    expired.forEach(id => {
      const sub = this.subscriptions.get(id);
      if (sub?.derivSubscriptionId) this.derivSubToInternal.delete(sub.derivSubscriptionId);
      this.subscriptions.delete(id);
    });

    if (expired.length > 0) {
      logger.info('Expired subscriptions cleaned up', { count: expired.length });
    }
  }
}
