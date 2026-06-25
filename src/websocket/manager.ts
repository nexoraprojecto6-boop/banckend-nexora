// ============================================================
// NEXORA FOREX — WebSocket Manager (Deriv Private WS)
// ============================================================
// Segue exactamente o fluxo recomendado pela Deriv:
//   1. OAuth2+PKCE → access_token
//   2. POST /otp → { data: { url: "wss://...?otp=..." } }
//   3. Conectar ao WS usando essa URL (sem enviar authorize depois)
//   4. Ping a cada ~30s — apenas {"ping":1}
//   5. Na reconexão: SEMPRE gerar novo OTP e nova URL
//
// Capacidade: 30+ utilizadores simultâneos
//   - 1 WS por conta (bots partilham a mesma conexão)
//   - Máx 100 subscrições por WS (limite Deriv)
//   - Throttle: máx 100 req/s
//   - Backoff exponencial com tecto de 30s
// ============================================================

import WebSocket from 'ws';
import { EventEmitter } from 'events';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';

// ─── Tipos ────────────────────────────────────────────────────

/**
 * Registry de uma subscrição activa.
 * - key:             identificador único local (tipo+payload)
 * - originalPayload: payload original com subscribe:1 para re-subscribe
 * - serverId:        id retornado pelo servidor Deriv (usado em forget)
 * - handler:         callback que recebe cada mensagem do stream
 */
interface SubscriptionEntry {
  key: string;
  originalPayload: Record<string, unknown>;
  serverId: string | null;
  handler: (data: unknown) => void;
}

export interface WebSocketOptions {
  /** Função que gera uma nova URL autenticada (com OTP) para cada conexão */
  getAuthUrl: () => Promise<string>;
  heartbeatInterval?: number;   // ms — padrão: 30_000
  reconnectMaxAttempts?: number; // padrão: 10
  reconnectBaseDelay?: number;  // ms — padrão: 1_000
  reconnectMaxDelay?: number;   // ms — padrão: 30_000
  maxSubscriptions?: number;    // padrão: 100 (limite Deriv)
  maxRequestsPerSecond?: number; // padrão: 100 (limite Deriv)
}

// ─── WebSocketManager ─────────────────────────────────────────

export class WebSocketManager extends EventEmitter {
  private ws: WebSocket | null = null;

  // req_id numérico incremental (nunca UUID)
  private nextId = 1;

  // Mapa de req_id → {resolve, reject} para pedidos com resposta única
  private pendingRequests = new Map<number, {
    resolve: (data: unknown) => void;
    reject: (err: Error) => void;
    timer: NodeJS.Timeout;
  }>();

  // Registry de subscrições: chave local → entry com serverId
  private subscriptions = new Map<string, SubscriptionEntry>();

  // Fila de mensagens offline (pares req_id → payload)
  private messageQueue: Array<{ reqId: number; payload: string }> = [];

  private isConnected = false;
  private isDestroyed = false;
  private reconnectAttempts = 0;

  private heartbeatTimer: NodeJS.Timeout | null = null;
  private heartbeatDeadlineTimer: NodeJS.Timeout | null = null;
  private connectionTimeoutTimer: NodeJS.Timeout | null = null;

  // Throttle: token bucket simples
  private sendTokens: number;
  private tokenRefillTimer: NodeJS.Timeout | null = null;

  private readonly opts: Required<WebSocketOptions>;

  constructor(options: WebSocketOptions) {
    super();
    this.opts = {
      getAuthUrl:            options.getAuthUrl,
      heartbeatInterval:     options.heartbeatInterval     ?? 30_000,
      reconnectMaxAttempts:  options.reconnectMaxAttempts  ?? 10,
      reconnectBaseDelay:    options.reconnectBaseDelay    ?? 1_000,
      reconnectMaxDelay:     options.reconnectMaxDelay     ?? 30_000,
      maxSubscriptions:      options.maxSubscriptions      ?? 100,
      maxRequestsPerSecond:  options.maxRequestsPerSecond  ?? 100,
    };
    this.sendTokens = this.opts.maxRequestsPerSecond;
  }

  // ─── Conexão ────────────────────────────────────────────────

  /**
   * Liga ao WebSocket Deriv privado.
   * Obtém sempre uma URL fresca com novo OTP antes de conectar.
   */
  async connect(): Promise<void> {
    if (this.isDestroyed) throw new Error('WebSocketManager foi destruído');

    // Gera nova URL autenticada (novo OTP obrigatório em cada conexão)
    let url: string;
    try {
      url = await this.opts.getAuthUrl();
    } catch (err) {
      logger.error('[WSManager] Falha a obter URL autenticada', { err });
      throw err;
    }

    return new Promise((resolve, reject) => {
      logger.info('[WSManager] A conectar...', { url: url.replace(/otp=[^&]+/, 'otp=***') });

      const ws = new WebSocket(url);
      this.ws = ws;

      // Timeout de conexão: 15s
      this.connectionTimeoutTimer = setTimeout(() => {
        if (!this.isConnected) {
          logger.error('[WSManager] Timeout de conexão');
          ws.terminate();
          reject(new Error('WebSocket connection timeout'));
        }
      }, 15_000);

      ws.once('open', () => {
        clearTimeout(this.connectionTimeoutTimer!);
        this.isConnected = true;
        this.reconnectAttempts = 0;
        logger.info('[WSManager] Conectado');
        this.startHeartbeat();
        this.startTokenRefill();
        this.flushQueue();
        this.emit('connected');
        resolve();
      });

      ws.on('message', (raw) => {
        const str = Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw);
        this.handleMessage(str);
      });

      ws.once('error', (err) => {
        logger.error('[WSManager] Erro de WebSocket', { message: err.message });
        this.emit('error', err);
        // se ainda não conectou, rejeita a promise
        if (!this.isConnected) {
          clearTimeout(this.connectionTimeoutTimer!);
          reject(err);
        }
      });

      ws.once('close', (code, reason) => {
        this.isConnected = false;
        this.stopHeartbeat();
        this.stopTokenRefill();
        logger.warn('[WSManager] Desconectado', { code, reason: reason?.toString() });
        this.emit('disconnected', { code, reason: reason?.toString() });
        if (!this.isDestroyed) {
          this.scheduleReconnect();
        }
      });
    });
  }

  // ─── Tratamento de mensagens ─────────────────────────────────

  private handleMessage(raw: string): void {
    let message: Record<string, unknown>;
    try {
      message = JSON.parse(raw);
    } catch {
      logger.error('[WSManager] Mensagem não é JSON válido', { raw: raw.slice(0, 200) });
      return;
    }

    // ── Ping: apenas resetar o deadline, NÃO enviar pong ───────
    if (message.msg_type === 'ping') {
      this.resetHeartbeatDeadline();
      return;
    }

    // ── Resposta ao nosso ping ──────────────────────────────────
    if (message.msg_type === 'pong') {
      this.resetHeartbeatDeadline();
      // resolver o pending request do ping, se existir
    }

    // ── Erros globais da Deriv ──────────────────────────────────
    if (message.error) {
      const err = message.error as Record<string, unknown>;
      const code = String(err.code ?? 'UNKNOWN');
      logger.warn('[WSManager] Erro recebido da Deriv', { code, message: err.message });

      // Rate limit: esperar 1s antes de continuar a enviar
      if (code === 'RateLimit') {
        logger.warn('[WSManager] Rate limit atingido, a pausar envios 1s');
        this.sendTokens = 0;
        setTimeout(() => { this.sendTokens = this.opts.maxRequestsPerSecond; }, 1_000);
      }
    }

    // ── Resolver pedido pendente (req_id numérico) ──────────────
    const reqId = typeof message.req_id === 'number' ? message.req_id : null;
    if (reqId !== null) {
      const pending = this.pendingRequests.get(reqId);
      if (pending) {
        clearTimeout(pending.timer);
        this.pendingRequests.delete(reqId);

        if (message.error) {
          const err = message.error as Record<string, unknown>;
          pending.reject(new Error(String(err.message ?? 'Erro Deriv')));
        } else {
          pending.resolve(message);
        }

        // Subscriptions: a primeira mensagem de um stream também
        // passa pelo pending (para capturar serverId). Depois disso,
        // as mensagens subsequentes chegam pelo subscription handler.
        // Por isso NÃO fazemos return aqui — continuamos para verificar
        // se é também uma mensagem de subscrição.
      }
    }

    // ── Despachar para subscription handlers ───────────────────
    // Identificar pelo serverId (subscription.id do servidor Deriv)
    const serverId = (message.subscription as Record<string, unknown> | undefined)?.id;
    if (serverId) {
      // Actualizar serverId no registry se ainda não estava definido
      for (const [localKey, entry] of this.subscriptions.entries()) {
        if (entry.serverId === null) {
          // Tentar identificar pelo req_id da primeira mensagem
          if (reqId !== null) {
            // a primeira mensagem tem o mesmo req_id que enviámos
            const payloadReqId = entry.originalPayload.req_id as number | undefined;
            if (payloadReqId === reqId) {
              entry.serverId = String(serverId);
              this.subscriptions.set(localKey, entry);
              logger.debug('[WSManager] serverId capturado', { localKey, serverId });
            }
          }
        }
        if (entry.serverId === String(serverId)) {
          try {
            entry.handler(message);
          } catch (handlerErr) {
            logger.error('[WSManager] Erro no subscription handler', { handlerErr, localKey });
          }
        }
      }
    }

    this.emit('message', message);
  }

  // ─── Envio de mensagens ──────────────────────────────────────

  /**
   * Envia uma mensagem e aguarda resposta única.
   * req_id é sempre gerado aqui como número incremental.
   */
  sendRequest(payload: Record<string, unknown>, timeoutMs = 30_000): Promise<unknown> {
    const reqId = this.nextId++;
    const message = { ...payload, req_id: reqId };
    const serialized = JSON.stringify(message);

    return new Promise((resolve, reject) => {
      if (!this.isConnected || !this.ws) {
        // Enfileirar para quando ligar
        this.messageQueue.push({ reqId, payload: serialized });
        // Registar pending mesmo offline para que quando flush aconteça,
        // a promise ainda seja resolvida
        const timer = setTimeout(() => {
          this.pendingRequests.delete(reqId);
          reject(new Error(`Request ${reqId} timed out (offline queue)`));
        }, timeoutMs + 60_000); // extra 60s para dar tempo a reconectar

        this.pendingRequests.set(reqId, { resolve, reject, timer });
        logger.debug('[WSManager] Mensagem enfileirada (offline)', { reqId });
        return;
      }

      const timer = setTimeout(() => {
        this.pendingRequests.delete(reqId);
        reject(new Error(`Request ${reqId} timed out`));
      }, timeoutMs);

      this.pendingRequests.set(reqId, { resolve, reject, timer });
      this.rawSend(serialized, reqId);
    });
  }

  /**
   * Envia sem aguardar resposta (fire-and-forget).
   * Usado para ping e forget.
   */
  sendFireAndForget(payload: Record<string, unknown>): void {
    if (!this.isConnected || !this.ws) {
      logger.warn('[WSManager] sendFireAndForget ignorado: WS offline', { payload });
      return;
    }
    const reqId = this.nextId++;
    const serialized = JSON.stringify({ ...payload, req_id: reqId });
    this.rawSend(serialized, reqId);
  }

  private rawSend(serialized: string, reqId: number): void {
    // Throttle: token bucket
    if (this.sendTokens <= 0) {
      logger.warn('[WSManager] Throttle atingido, a descartar mensagem', { reqId });
      // Rejeitar o pending se existir
      const pending = this.pendingRequests.get(reqId);
      if (pending) {
        clearTimeout(pending.timer);
        this.pendingRequests.delete(reqId);
        pending.reject(new Error('Throttle: demasiados pedidos por segundo'));
      }
      return;
    }
    this.sendTokens--;

    try {
      this.ws!.send(serialized);
    } catch (err) {
      logger.error('[WSManager] Falha ao enviar mensagem', { err, reqId });
      const pending = this.pendingRequests.get(reqId);
      if (pending) {
        clearTimeout(pending.timer);
        this.pendingRequests.delete(reqId);
        pending.reject(err instanceof Error ? err : new Error(String(err)));
      }
    }
  }

  private flushQueue(): void {
    logger.info('[WSManager] A enviar fila offline', { count: this.messageQueue.length });
    while (this.messageQueue.length > 0 && this.isConnected) {
      const item = this.messageQueue.shift()!;
      this.rawSend(item.payload, item.reqId);
    }
  }

  // ─── Subscrições ─────────────────────────────────────────────

  /**
   * Cria uma subscrição persistente.
   * Sobrevive a reconexões (re-subscribe automático com novo OTP).
   *
   * @param type  Nome do tipo (ex: 'ticks', 'balance', 'proposal_open_contract')
   * @param payload Payload Deriv (sem req_id — é adicionado aqui)
   * @param handler Callback para cada mensagem do stream
   * @returns localKey para usar em unsubscribe()
   */
  subscribe(
    type: string,
    payload: Record<string, unknown>,
    handler: (data: unknown) => void,
  ): string {
    if (this.subscriptions.size >= this.opts.maxSubscriptions) {
      throw new Error(`Limite de ${this.opts.maxSubscriptions} subscrições atingido`);
    }

    const key = `${type}:${JSON.stringify(payload)}`;

    if (this.subscriptions.has(key)) {
      logger.warn('[WSManager] Subscrição duplicada ignorada', { key });
      return key;
    }

    const reqId = this.nextId++;
    const originalPayload: Record<string, unknown> = {
      ...payload,
      subscribe: 1,
      req_id: reqId,
    };

    const entry: SubscriptionEntry = {
      key,
      originalPayload,
      serverId: null,
      handler,
    };
    this.subscriptions.set(key, entry);

    this.sendFireAndForgetRaw(JSON.stringify(originalPayload));
    logger.info('[WSManager] Subscrição criada', { key, reqId });
    return key;
  }

  private sendFireAndForgetRaw(serialized: string): void {
    if (!this.isConnected || !this.ws) return;
    if (this.sendTokens <= 0) {
      logger.warn('[WSManager] Throttle: fire-and-forget descartado');
      return;
    }
    this.sendTokens--;
    try {
      this.ws.send(serialized);
    } catch (err) {
      logger.error('[WSManager] Erro em sendFireAndForgetRaw', { err });
    }
  }

  /**
   * Cancela uma subscrição pelo localKey retornado por subscribe().
   * Envia forget com o serverId do servidor Deriv.
   */
  async unsubscribe(localKey: string): Promise<void> {
    const entry = this.subscriptions.get(localKey);
    if (!entry) {
      logger.warn('[WSManager] unsubscribe: subscrição não encontrada', { localKey });
      return;
    }

    this.subscriptions.delete(localKey);

    if (entry.serverId && this.isConnected) {
      try {
        await this.sendRequest({ forget: entry.serverId }, 10_000);
        logger.info('[WSManager] Subscrição cancelada', { localKey, serverId: entry.serverId });
      } catch (err) {
        logger.warn('[WSManager] Falha ao enviar forget', { err, serverId: entry.serverId });
      }
    }
  }

  /**
   * Cancela todas as subscrições de uma vez.
   * Usa forget_all com os tipos válidos da Deriv.
   */
  async unsubscribeAll(): Promise<void> {
    if (this.subscriptions.size === 0) return;

    // Tipos válidos para forget_all conforme documentação Deriv
    const VALID_FORGET_ALL_TYPES = [
      'ticks',
      'candles',
      'proposal',
      'proposal_open_contract',
      'balance',
      'transaction',
      'p2p_order',
      'website_status',
    ] as const;

    this.subscriptions.clear();

    if (this.isConnected) {
      try {
        await this.sendRequest({ forget_all: VALID_FORGET_ALL_TYPES }, 15_000);
        logger.info('[WSManager] Todas as subscrições canceladas via forget_all');
      } catch (err) {
        logger.warn('[WSManager] Falha em forget_all', { err });
      }
    }
  }

  // ─── Re-subscribe após reconexão ────────────────────────────

  /**
   * Reenvia todos os originalPayload do registry após reconectar.
   * Reseta serverId para ser recapturado na primeira mensagem.
   */
  private resubscribeAll(): void {
    if (this.subscriptions.size === 0) return;

    logger.info('[WSManager] A re-subscrever após reconexão', {
      count: this.subscriptions.size,
    });

    for (const [key, entry] of this.subscriptions.entries()) {
      // Novo req_id para a nova conexão
      const newReqId = this.nextId++;
      const newPayload = { ...entry.originalPayload, req_id: newReqId };

      entry.serverId = null;
      entry.originalPayload = newPayload;
      this.subscriptions.set(key, entry);

      this.sendFireAndForgetRaw(JSON.stringify(newPayload));
      logger.debug('[WSManager] Re-subscrito', { key, newReqId });
    }
  }

  // ─── Heartbeat ───────────────────────────────────────────────

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      if (this.isConnected) {
        // Deriv: enviar apenas {"ping":1}, nunca {"pong":1}
        this.sendFireAndForget({ ping: 1 });
        this.setHeartbeatDeadline();
      }
    }, this.opts.heartbeatInterval);
    logger.debug('[WSManager] Heartbeat iniciado', { interval: this.opts.heartbeatInterval });
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) { clearInterval(this.heartbeatTimer); this.heartbeatTimer = null; }
    if (this.heartbeatDeadlineTimer) { clearTimeout(this.heartbeatDeadlineTimer); this.heartbeatDeadlineTimer = null; }
  }

  private setHeartbeatDeadline(): void {
    if (this.heartbeatDeadlineTimer) clearTimeout(this.heartbeatDeadlineTimer);
    // Se não receber pong em 10s após ping, a conexão está morta
    this.heartbeatDeadlineTimer = setTimeout(() => {
      logger.warn('[WSManager] Heartbeat sem resposta — a forçar reconexão');
      this.ws?.terminate();
    }, 10_000);
  }

  private resetHeartbeatDeadline(): void {
    if (this.heartbeatDeadlineTimer) {
      clearTimeout(this.heartbeatDeadlineTimer);
      this.heartbeatDeadlineTimer = null;
    }
  }

  // ─── Throttle (token bucket) ─────────────────────────────────

  private startTokenRefill(): void {
    this.stopTokenRefill();
    // Repõe todos os tokens a cada segundo
    this.tokenRefillTimer = setInterval(() => {
      this.sendTokens = this.opts.maxRequestsPerSecond;
    }, 1_000);
  }

  private stopTokenRefill(): void {
    if (this.tokenRefillTimer) { clearInterval(this.tokenRefillTimer); this.tokenRefillTimer = null; }
  }

  // ─── Reconexão ───────────────────────────────────────────────

  private scheduleReconnect(): void {
    if (this.reconnectAttempts >= this.opts.reconnectMaxAttempts) {
      logger.error('[WSManager] Máximo de tentativas de reconexão atingido', {
        attempts: this.reconnectAttempts,
      });
      this.emit('reconnect_failed');
      return;
    }

    // Backoff exponencial com tecto e jitter
    const base = Math.min(
      this.opts.reconnectBaseDelay * Math.pow(2, this.reconnectAttempts),
      this.opts.reconnectMaxDelay,
    );
    const jitter = Math.random() * 0.3 * base; // ±30% de jitter
    const delay = Math.floor(base + jitter);

    this.reconnectAttempts++;
    logger.info('[WSManager] Reconexão agendada', {
      attempt: this.reconnectAttempts,
      delayMs: delay,
    });

    setTimeout(async () => {
      try {
        // OBRIGATÓRIO: nova URL com novo OTP a cada reconexão
        await this.connect();
        // Após conexão bem-sucedida, re-subscrever tudo
        this.resubscribeAll();
      } catch (err) {
        logger.error('[WSManager] Falha na reconexão', { err });
        // scheduleReconnect será chamado novamente pelo evento 'close'
      }
    }, delay);
  }

  // ─── Ciclo de vida ───────────────────────────────────────────

  /**
   * Desconecta e liberta todos os recursos.
   * Após destroy(), este manager não pode ser reutilizado.
   */
  async destroy(): Promise<void> {
    this.isDestroyed = true;
    this.stopHeartbeat();
    this.stopTokenRefill();

    // Cancelar todos os pending requests
    for (const [reqId, pending] of this.pendingRequests.entries()) {
      clearTimeout(pending.timer);
      pending.reject(new Error('WebSocketManager destruído'));
      this.pendingRequests.delete(reqId);
    }

    this.subscriptions.clear();
    this.messageQueue = [];

    if (this.ws) {
      this.ws.removeAllListeners();
      this.ws.terminate();
      this.ws = null;
    }

    this.isConnected = false;
    logger.info('[WSManager] Destruído');
  }

  // ─── Estado / Diagnóstico ────────────────────────────────────

  getStatus() {
    return {
      isConnected:       this.isConnected,
      subscriptions:     this.subscriptions.size,
      pendingRequests:   this.pendingRequests.size,
      queuedMessages:    this.messageQueue.length,
      reconnectAttempts: this.reconnectAttempts,
      sendTokens:        this.sendTokens,
    };
  }
}
