// ============================================================
// NEXORA FOREX — Message Router (Deriv WebSocket)
// ============================================================
// Problema que resolve:
//   Antes, cada chamada a buyContract/waitForContract/subscribeToTicks
//   registava o SEU PRÓPRIO ws.on('message', handler). Com muitos
//   bots activos no mesmo WS (uma sessão pode ter vários bots), cada
//   mensagem recebida da Deriv era testada contra TODOS os listeners
//   acumulados — custo O(n) por mensagem, e cada handler repetia o
//   próprio JSON.parse.
//
// Como resolve:
//   UM único listener 'message' por ligação WS. Faz o parse UMA vez,
//   e despacha para quem estiver interessado:
//     - by req_id  → respostas de pedido único (proposal, buy, etc.)
//       Resolve e REMOVE automaticamente após a primeira correspondência
//       (request/response clássico, sem ambiguidade entre pedidos
//       concorrentes — cada um tem o seu próprio req_id).
//     - by subscription key → streams contínuos (ticks, contratos
//       abertos em acompanhamento). Mantidos até serem cancelados
//       explicitamente (unsubscribe).
//
// Isto resolve directamente os pontos #1 (request tracking sem
// variáveis globais — cada pedido já tem o seu req_id isolado) e #2
// (router central em vez de ifs duplicados) do documento de reforço
// de arquitectura, e elimina o custo O(n) por mensagem do ponto #17.
// ============================================================

import { WebSocket } from 'ws';
import logger from '@utils/logger.js';

type RawMessage = Record<string, unknown>;

interface PendingRequest {
  resolve: (msg: RawMessage) => void;
  reject: (err: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

// ─── Subscrições com contagem de referências ──────────────────
// Inspirado no SubscriptionManager oficial da @deriv/deriv-api:
// múltiplos consumidores da MESMA subscrição (mesma key) partilham
// uma só entrada — equivalente manual ao share()+finalize() do RxJS
// que a biblioteca oficial usa. Sem isto, dois bots a pedir ticks do
// mesmo símbolo criariam duas entradas independentes no router
// (cada uma reagindo à mesma mensagem), o que é redundante mas não
// incorreto; com isto, partilham a entrada e cada consumidor pode
// cancelar a sua parte sem afectar o outro.
interface Subscription {
  // Devolve true se a mensagem foi consumida por esta subscrição.
  match: (msg: RawMessage) => boolean;
  // Um callback por consumidor activo desta key.
  consumers: Set<(msg: RawMessage) => void>;
}

const DEFAULT_REQUEST_TIMEOUT_MS = 15_000;

export class MessageRouter {
  private ws: WebSocket;
  private pending = new Map<number, PendingRequest>();
  private subscriptions = new Map<string, Subscription>();
  private destroyed = false;

  constructor(ws: WebSocket) {
    this.ws = ws;
    this.ws.on('message', this.handleRaw);
  }

  // ─── Request/response de pedido único ────────────────────────
  // Resolve com a primeira mensagem cujo req_id corresponda. Depois
  // de resolver (ou rejeitar, ou expirar), o pedido é removido —
  // não fica nenhum listener pendurado.

  request(payload: Record<string, unknown>, reqId: number, timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS): Promise<RawMessage> {
    if (this.destroyed) {
      return Promise.reject(new Error('MessageRouter destruído — ligação fechada'));
    }
    if (this.ws.readyState !== WebSocket.OPEN) {
      return Promise.reject(new Error('Deriv WebSocket não está disponível'));
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(reqId);
        reject(new Error(`Timeout aguardando resposta (req_id=${reqId})`));
      }, timeoutMs);

      this.pending.set(reqId, { resolve, reject, timer });
      this.ws.send(JSON.stringify({ ...payload, req_id: reqId }));
    });
  }

  // ─── Subscrições contínuas (com reuso por key) ────────────────
  // key deve identificar univocamente o stream Deriv (ex:
  // `ticks:${symbol}`, `poc:${contractId}`) — duas chamadas com a
  // MESMA key partilham a mesma entrada, tal como o
  // SubscriptionManager oficial faz para o mesmo request.
  //
  // Devolve uma função de unsubscribe ESPECÍFICA deste consumidor —
  // chamá-la remove só este callback. A entrada subjacente (e o
  // envio de "forget" à Deriv) só desaparece quando o ÚLTIMO
  // consumidor dessa key se desinscreve, evitando cancelar uma
  // subscrição que outra parte do código ainda está a usar.

  subscribe(
    key: string,
    match: (msg: RawMessage) => boolean,
    onMessage: (msg: RawMessage) => void,
  ): { reused: boolean; unsubscribe: () => void } {
    let sub = this.subscriptions.get(key);
    const reused = !!sub;

    if (!sub) {
      sub = { match, consumers: new Set() };
      this.subscriptions.set(key, sub);
    }
    sub.consumers.add(onMessage);

    return {
      reused,
      unsubscribe: () => {
        const current = this.subscriptions.get(key);
        if (!current) return;
        current.consumers.delete(onMessage);
        // Só remove a entrada (e quem a chama deve só então enviar
        // "forget" à Deriv) quando não restar nenhum consumidor —
        // equivalente ao finalize() do RxJS na biblioteca oficial.
        if (current.consumers.size === 0) {
          this.subscriptions.delete(key);
        }
      },
    };
  }

  // Força a remoção total de uma key, independentemente de quantos
  // consumidores ainda existam — usar só em cleanup de emergência
  // (ex: destroy() da ligação). Em uso normal, prefira o
  // unsubscribe() específico devolvido por subscribe().
  unsubscribe(key: string): void {
    this.subscriptions.delete(key);
  }

  isSubscribed(key: string): boolean {
    return this.subscriptions.has(key);
  }

  // Quantos consumidores activos existem para esta key — útil para
  // decidir se vale a pena enviar "forget" (só quando chega a 0).
  consumerCount(key: string): number {
    return this.subscriptions.get(key)?.consumers.size ?? 0;
  }

  // ─── Dispatcher central — único ws.on('message') desta ligação ──

  private handleRaw = (raw: Buffer | string): void => {
    let msg: RawMessage;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return; // mensagem não-JSON, ignorar
    }

    // 1) Request/response de pedido único, por req_id
    const reqId = msg.req_id as number | undefined;
    if (typeof reqId === 'number' && this.pending.has(reqId)) {
      const p = this.pending.get(reqId)!;
      this.pending.delete(reqId);
      clearTimeout(p.timer);
      if (msg.error) {
        p.reject(Object.assign(new Error((msg.error as RawMessage).message as string ?? 'Erro Deriv'), {
          code: (msg.error as RawMessage).code,
        }));
      } else {
        p.resolve(msg);
      }
      return; // mensagens com req_id correspondido não passam às subscrições
    }

    // 2) Subscrições contínuas — testar todas (uma mensagem pode,
    // em teoria, interessar a mais do que uma, embora seja raro).
    // Cada subscrição com match() verdadeiro notifica TODOS os seus
    // consumidores actuais.
    for (const sub of this.subscriptions.values()) {
      if (!sub.match(msg)) continue;
      for (const consumer of sub.consumers) {
        try {
          consumer(msg);
        } catch (err) {
          logger.error('[MessageRouter] Erro a processar subscrição', { error: err });
        }
      }
    }
  };

  // ─── Cleanup ───────────────────────────────────────────────────
  // Chamado quando a ligação WS fecha/é substituída. Rejeita todos
  // os pedidos pendentes (em vez de deixá-los pendurados para sempre
  // à espera de uma resposta que nunca vai chegar) e limpa o listener.

  destroy(reason = 'connection closed'): void {
    if (this.destroyed) return;
    this.destroyed = true;

    for (const [reqId, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(new Error(`Ligação fechada antes da resposta (req_id=${reqId}): ${reason}`));
    }
    this.pending.clear();
    this.subscriptions.clear();

    this.ws.removeListener('message', this.handleRaw);
  }

  get pendingCount(): number {
    return this.pending.size;
  }

  get subscriptionCount(): number {
    return this.subscriptions.size;
  }
}

// ─── Registo por ligação WS ──────────────────────────────────────
// Garante UM único MessageRouter por instância de WebSocket, mesmo
// que múltiplas partes do código (vários bots da mesma sessão)
// peçam um router para o mesmo ws.

const routerRegistry = new WeakMap<WebSocket, MessageRouter>();

export function getMessageRouter(ws: WebSocket): MessageRouter {
  let router = routerRegistry.get(ws);
  if (!router) {
    router = new MessageRouter(ws);
    routerRegistry.set(ws, router);
    ws.once('close', () => {
      router?.destroy('ws closed');
      routerRegistry.delete(ws);
    });
  }
  return router;
}
