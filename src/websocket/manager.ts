import WebSocket from 'ws';
import { EventEmitter } from 'events';
import { v4 as uuidv4 } from 'uuid';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';

interface WebSocketSubscription {
  id: string;
  derivSubscriptionId?: string;
  type: string;
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
  hasReqId: boolean;
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
  private heartbeatInterval: NodeJS.Timeout | null = null;
  private heartbeatTimeout: NodeJS.Timeout | null = null;
  private connectionTimeout: NodeJS.Timeout | null = null;
  private lastMessageAt = Date.now();
  private readonly options: Required<Omit<WebSocketOptions, 'rateLimitPerSecond' | 'url' | 'getUrl'>> & { rateLimitPerSecond: number };

  private sendQueue: QueuedSend[] = [];
  private rateLimiterInterval: NodeJS.Timeout | null = null;
  private rateLimiterDrainInterval: NodeJS.Timeout | null = null;
  private sentInCurrentWindow = 0;

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
        }, 10000);

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
        //   1008  Policy violation (payload malformado, auth inválida,
        //         ou — como descobrimos — OTP já consumido/expirado)
        //   1011  Internal server error (do lado da Deriv)
        //   4xxx  Códigos específicos de apps Deriv
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
            // Este fecho foi deliberado (reconnect() manual / heartbeat
            // timeout) — connect() já vai ser chamado a seguir pelo
            // próprio reconnect(). Evita disparar um segundo ciclo de
            // attemptReconnect em paralelo.
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

      // DIAGNÓSTICO: erros de nível de mensagem (não de ligação) vêm
      // em `message.error`, com req_id correspondente. Se isto
      // acontecer logo após auth, é a causa mais provável de
      // "autenticado mas cai logo a seguir" — ex: payload inválido no
      // primeiro pedido enviado (balance/transaction).
      if (message.error) {
        logger.warn('[Deriv] Mensagem de erro recebida', {
          code: message.error.code,
          message: message.error.message,
          req_id: message.req_id,
          msg_type: message.msg_type,
        });
      }

      if (message.msg_type === 'ping') {
        this.send({ pong: 1 }).catch(() => { /* pong não precisa de confirmação */ });
        return;
      }

      if (message.req_id) {
        const pending = this.pendingRequests.get(String(message.req_id));
        if (pending) {
          if (message.subscription?.id) {
            this.linkDerivSubscriptionId(String(message.req_id), message.subscription.id);
          }
          pending.resolve(message);
          this.pendingRequests.delete(String(message.req_id));
        }
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

  send(payload: any, timeout = 15000): Promise<any> {
    return new Promise((resolve, reject) => {
      const message = {
        ...payload,
        req_id: payload.req_id || uuidv4(),
      };

      const timeoutId = setTimeout(() => {
        this.pendingRequests.delete(String(message.req_id));
        this.messageQueue = this.messageQueue.filter((m) => m.message.req_id !== message.req_id);
        this.sendQueue = this.sendQueue.filter((q) => q.message.req_id !== message.req_id);
        reject(new Error(`Request ${message.req_id} timed out${this.isConnected ? '' : ' (ligação indisponível)'}`));
      }, timeout);

      const wrappedResolve = (data: any) => {
        clearTimeout(timeoutId);
        resolve(data);
      };
      const wrappedReject = (err: any) => {
        clearTimeout(timeoutId);
        reject(err);
      };

      if (!this.isConnected) {
        this.messageQueue.push({ message, resolve: wrappedResolve, reject: wrappedReject, timeout });
        logger.warn('WebSocket not connected, message queued (a aguardar reconexão)', { reqId: message.req_id });
        return;
      }

      this.sendQueue.push({
        message,
        resolve: wrappedResolve,
        reject: wrappedReject,
        timeout,
        hasReqId: true,
      });
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
    handler: (data: any) => void
  ): string {
    const subscriptionId = uuidv4();
    const reqId = payload.req_id || uuidv4();

    const subscription: WebSocketSubscription = {
      id: subscriptionId,
      type,
      handler,
      expiresAt: Date.now() + 24 * 60 * 60 * 1000,
    };

    this.subscriptions.set(subscriptionId, subscription);
    this.pendingSubscriptionLinks.set(String(reqId), subscriptionId);

    this.send({
      ...payload,
      subscribe: 1,
      req_id: reqId,
    }).catch((error: any) => {
      logger.error('Subscription failed', { error, type });
      this.subscriptions.delete(subscriptionId);
      this.pendingSubscriptionLinks.delete(String(reqId));
    });

    logger.info('WebSocket subscription created', { subscriptionId, type });
    return subscriptionId;
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
        : Array.from(new Set(allSubs.map((s: WebSocketSubscription) => s.type)));

      await this.send({ forget_all: types });

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

  private startHeartbeat() {
    this.heartbeatInterval = setInterval(() => {
      if (this.isConnected) {
        const idleMs = Date.now() - this.lastMessageAt;
        logger.debug('Heartbeat tick', { idleMs });

        this.send({ ping: 1 }).then(() => {
          if (this.heartbeatTimeout) {
            clearTimeout(this.heartbeatTimeout);
            this.heartbeatTimeout = null;
          }
        }).catch((error: any) => {
          logger.warn('Heartbeat failed', { error: error?.message });
        });

        this.heartbeatTimeout = setTimeout(() => {
          logger.warn('Heartbeat timeout, reconnecting...', {
            idleMs: Date.now() - this.lastMessageAt,
          });
          this.reconnect();
        }, 5000);
      }
    }, this.options.heartbeatInterval);

    logger.debug('Heartbeat started', { interval: this.options.heartbeatInterval });
  }

  private stopHeartbeat() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
    if (this.heartbeatTimeout) {
      clearTimeout(this.heartbeatTimeout);
      this.heartbeatTimeout = null;
    }
  }

  private attemptReconnect() {
    if (this.reconnectAttempts >= this.options.reconnectMaxAttempts) {
      logger.error('Max reconnection attempts reached', {
        attempts: this.reconnectAttempts,
      });
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
        this.emit('reconnected');
      }).catch((error: any) => {
        // Se getUrl() falhar (ex: token expirou, conta desativada),
        // o erro chega aqui. Tentamos de novo até ao limite, mas
        // registamos claramente para distinguir de uma falha de rede.
        logger.error('Reconnection failed', {
          error: error?.message,
          attempt: this.reconnectAttempts,
        });
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
    this.connect();
  }

  private flushMessageQueue() {
    while (this.messageQueue.length > 0 && this.isConnected) {
      const item = this.messageQueue.shift()!;
      this.sendQueue.push({
        message: item.message,
        resolve: item.resolve,
        reject: item.reject,
        timeout: item.timeout,
        hasReqId: true,
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
