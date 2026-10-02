// ============================================================
// NEXORA FOREX — Message Router (Deriv WebSocket) v2
// ============================================================
// Um único router por ligação WS. Despacho O(1):
//   • pedidos únicos  → Map<req_id, pending>
//   • streams         → Map<req_id, stream> (1ª resposta) e
//                       Map<subscription.id, stream> (restantes)
//
// Correcções vs v1:
//   ✅ Sem varrer todas as subscrições a cada mensagem (era O(n))
//   ✅ Erro na 1ª resposta de um stream (ex: símbolo inválido) chega ao
//      consumidor (antes ficava mudo e o bot esperava para sempre)
//   ✅ forget com o subscription.id guardado NO STREAM (antes ficava
//      por consumidor → subscrições órfãs → limite de 100)
//   ✅ Stream fechado antes da 1ª resposta é "forgotten" quando chega
//   ✅ Consumidores são avisados quando a ligação cai (onDisconnect)
//      para voltarem a subscrever na nova ligação
//   ✅ ws.send que lança excepção já não deixa timer/pending pendurados
//   ✅ Limite de envio (80/s) abaixo dos 100/s da Deriv
//   ✅ dispatch() público: permite parse ÚNICO por mensagem
//      (deriv-api.service faz o parse e entrega ao router)
// ============================================================

import { WebSocket } from 'ws';
import logger from '@utils/logger.js';
import { nextReqId } from '@utils/req-id.js';

export type RawMessage = Record<string, unknown>;

const DEFAULT_REQUEST_TIMEOUT_MS = 15_000;
const MAX_SENDS_PER_SECOND = 80; // Deriv: 100 pedidos/s por ligação

interface PendingRequest {
  resolve: (msg: RawMessage) => void;
  reject: (err: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

export interface StreamConsumer {
  onMessage: (msg: RawMessage) => void;
  /** Erro da Deriv na 1ª resposta (stream não arrancou) ou a meio. */
  onError?: (err: Error) => void;
  /** A ligação fechou — o consumidor deve voltar a subscrever. */
  onDisconnect?: () => void;
}

interface StreamEntry {
  key: string;
  reqId: number;
  subId: string | null;
  sent: boolean;
  consumers: Set<StreamConsumer>;
}

function derivError(raw: unknown, fallback: string): Error {
  const e = (raw ?? {}) as RawMessage;
  return Object.assign(new Error(String(e.message ?? fallback)), {
    code: String(e.code ?? 'UNKNOWN'),
  });
}

export class MessageRouter {
  private pending = new Map<number, PendingRequest>();
  private streamsByKey = new Map<string, StreamEntry>();
  private streamsByReqId = new Map<number, StreamEntry>();
  private streamsBySubId = new Map<string, StreamEntry>();
  private orphanReqIds = new Set<number>();
  private destroyed = false;
  private windowStart = 0;
  private sentInWindow = 0;

  constructor(private readonly ws: WebSocket, private readonly ownsListener = true) {
    if (ownsListener) this.ws.on('message', this.handleRaw);
  }

  // ─── Envio com limite de taxa ─────────────────────────────

  private throttled(fn: () => void): void {
    const now = Date.now();
    if (now - this.windowStart >= 1000) {
      this.windowStart = now;
      this.sentInWindow = 0;
    }
    if (this.sentInWindow < MAX_SENDS_PER_SECOND) {
      this.sentInWindow++;
      fn();
      return;
    }
    setTimeout(() => this.throttled(fn), Math.max(1, this.windowStart + 1000 - now));
  }

  // ─── Pedido único ─────────────────────────────────────────

  request(
    payload: Record<string, unknown>,
    reqId: number = nextReqId(),
    timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
  ): Promise<RawMessage> {
    if (this.destroyed) return Promise.reject(new Error('MessageRouter destruído — ligação fechada'));
    if (this.ws.readyState !== WebSocket.OPEN) {
      return Promise.reject(new Error('Deriv WebSocket não está disponível'));
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(reqId);
        reject(new Error(`Timeout aguardando resposta (req_id=${reqId})`));
      }, timeoutMs);

      this.pending.set(reqId, { resolve, reject, timer });

      this.throttled(() => {
        if (!this.pending.has(reqId)) return; // já expirou/foi rejeitado
        try {
          this.ws.send(JSON.stringify({ ...payload, req_id: reqId }));
        } catch (err) {
          clearTimeout(timer);
          this.pending.delete(reqId);
          reject(err instanceof Error ? err : new Error(String(err)));
        }
      });
    });
  }

  // ─── Streams (ticks, proposal_open_contract, ...) ─────────
  // key identifica o stream (ex: `ticks:R_50`). Vários consumidores
  // da mesma key partilham UMA subscrição na Deriv.

  openStream(
    key: string,
    payload: Record<string, unknown>,
    consumer: StreamConsumer,
  ): { reused: boolean; close: () => void } {
    let entry = this.streamsByKey.get(key);
    const reused = !!entry;

    if (!entry) {
      const created: StreamEntry = {
        key,
        reqId: nextReqId(),
        subId: null,
        sent: false,
        consumers: new Set(),
      };
      entry = created;
      this.streamsByKey.set(key, created);
      this.streamsByReqId.set(created.reqId, created);

      this.throttled(() => {
        if (this.destroyed || this.streamsByKey.get(key) !== created) return;
        try {
          this.ws.send(JSON.stringify({ ...payload, subscribe: 1, req_id: created.reqId }));
          created.sent = true;
        } catch (err) {
          this.failStream(created, derivError(
            { message: err instanceof Error ? err.message : String(err), code: 'SEND_FAILED' },
            'Falha ao enviar subscrição',
          ));
        }
      });
    }

    entry.consumers.add(consumer);
    const target = entry;
    let closed = false;

    return {
      reused,
      close: () => {
        if (closed) return;
        closed = true;
        target.consumers.delete(consumer);
        if (target.consumers.size === 0) this.dropStream(target, true);
      },
    };
  }

  /** Força nova subscrição: todos os consumidores recebem onDisconnect. */
  resetStream(key: string): void {
    const entry = this.streamsByKey.get(key);
    if (!entry) return;
    const consumers = [...entry.consumers];
    this.dropStream(entry, true);
    for (const c of consumers) {
      try { c.onDisconnect?.(); } catch (err) { logger.error('[MessageRouter] onDisconnect falhou', { error: err }); }
    }
  }

  consumerCount(key: string): number {
    return this.streamsByKey.get(key)?.consumers.size ?? 0;
  }

  isSubscribed(key: string): boolean {
    return this.streamsByKey.has(key);
  }

  private dropStream(entry: StreamEntry, sendForget: boolean): void {
    if (this.streamsByKey.get(entry.key) === entry) this.streamsByKey.delete(entry.key);
    this.streamsByReqId.delete(entry.reqId);
    if (entry.subId) {
      this.streamsBySubId.delete(entry.subId);
      if (sendForget) this.forget(entry.subId);
    } else if (sendForget && entry.sent && !this.destroyed) {
      // 1ª resposta ainda não chegou: fazer forget quando chegar
      this.orphanReqIds.add(entry.reqId);
    }
  }

  private failStream(entry: StreamEntry, err: Error): void {
    const consumers = [...entry.consumers];
    this.dropStream(entry, false);
    for (const c of consumers) {
      try { c.onError?.(err); } catch (e) { logger.error('[MessageRouter] onError falhou', { error: e }); }
    }
  }

  private forget(subId: string): void {
    if (this.ws.readyState !== WebSocket.OPEN) return;
    try {
      this.ws.send(JSON.stringify({ forget: subId, req_id: nextReqId() }));
    } catch { /* ligação a fechar */ }
  }

  private fanout(entry: StreamEntry, msg: RawMessage): void {
    if (msg.error) {
      const err = derivError(msg.error, 'Erro Deriv no stream');
      for (const c of [...entry.consumers]) {
        try { c.onError?.(err); } catch (e) { logger.error('[MessageRouter] onError falhou', { error: e }); }
      }
      return;
    }
    for (const c of entry.consumers) {
      try {
        c.onMessage(msg);
      } catch (err) {
        logger.error('[MessageRouter] Erro a processar stream', { key: entry.key, error: err });
      }
    }
  }

  // ─── Dispatcher ───────────────────────────────────────────

  private handleRaw = (raw: Buffer | string): void => {
    let msg: RawMessage;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    this.dispatch(msg);
  };

  /**
   * Entrega uma mensagem JÁ parseada (parse único por mensagem).
   * Devolve true se a mensagem foi consumida por um pedido/stream do
   * router (tráfego dos bots) — o server.ts usa isto para NÃO a
   * reencaminhar ao frontend.
   */
  dispatch(msg: RawMessage): boolean {
    const reqId = typeof msg.req_id === 'number' ? msg.req_id : undefined;

    if (reqId !== undefined) {
      // 1) pedido único
      const p = this.pending.get(reqId);
      if (p) {
        this.pending.delete(reqId);
        clearTimeout(p.timer);
        if (msg.error) p.reject(derivError(msg.error, 'Erro Deriv'));
        else p.resolve(msg);
        return true;
      }

      // 2) stream fechado antes da 1ª resposta → cancelar
      if (this.orphanReqIds.delete(reqId)) {
        const sid = (msg.subscription as { id?: string } | undefined)?.id;
        if (sid) this.forget(sid);
        return true;
      }

      // 3) 1ª resposta de um stream
      const s = this.streamsByReqId.get(reqId);
      if (s) {
        this.streamsByReqId.delete(reqId);
        if (msg.error) {
          this.failStream(s, derivError(msg.error, 'Erro ao iniciar subscrição'));
          return true;
        }
        const sid = (msg.subscription as { id?: string } | undefined)?.id;
        if (sid) {
          s.subId = sid;
          this.streamsBySubId.set(sid, s);
        }
        this.fanout(s, msg);
        return true;
      }
    }

    // 4) mensagens seguintes do stream (ecoam req_id, mas já não está em streamsByReqId)
    const subId = (msg.subscription as { id?: string } | undefined)?.id;
    if (subId) {
      const s = this.streamsBySubId.get(subId);
      if (s) {
        this.fanout(s, msg);
        return true;
      }
    }
    return false;
  }

  // ─── Cleanup ──────────────────────────────────────────────

  destroy(reason = 'connection closed'): void {
    if (this.destroyed) return;
    this.destroyed = true;

    for (const [reqId, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(new Error(`Ligação fechada antes da resposta (req_id=${reqId}): ${reason}`));
    }
    this.pending.clear();

    const entries = [...this.streamsByKey.values()];
    this.streamsByKey.clear();
    this.streamsByReqId.clear();
    this.streamsBySubId.clear();
    this.orphanReqIds.clear();

    for (const e of entries) {
      for (const c of e.consumers) {
        try { c.onDisconnect?.(); } catch (err) { logger.error('[MessageRouter] onDisconnect falhou', { error: err }); }
      }
    }

    if (this.ownsListener) this.ws.removeListener('message', this.handleRaw);
  }

  get pendingCount(): number { return this.pending.size; }
  get subscriptionCount(): number { return this.streamsByKey.size; }
}

// ─── Registo por ligação ─────────────────────────────────────

const routerRegistry = new WeakMap<WebSocket, MessageRouter>();

function register(ws: WebSocket, ownsListener: boolean): MessageRouter {
  const router = new MessageRouter(ws, ownsListener);
  routerRegistry.set(ws, router);
  ws.once('close', () => {
    router.destroy('ws closed');
    routerRegistry.delete(ws);
  });
  return router;
}

/** Router com listener próprio (faz o seu próprio parse). */
export function getMessageRouter(ws: WebSocket): MessageRouter {
  return routerRegistry.get(ws) ?? register(ws, true);
}

/**
 * Router SEM listener próprio: quem criou o WS faz o JSON.parse uma vez
 * e chama router.dispatch(msg). Usado por createAuthenticatedWebSocket.
 */
export function attachExternalRouter(ws: WebSocket): MessageRouter {
  return routerRegistry.get(ws) ?? register(ws, false);
}

/** Destrói explicitamente o router de um WS (ex: quando o server o termina sem emitir 'close'). */
export function destroyMessageRouter(ws: WebSocket): void {
  const r = routerRegistry.get(ws);
  if (r) {
    r.destroy('ws terminated');
    routerRegistry.delete(ws);
  }
}
