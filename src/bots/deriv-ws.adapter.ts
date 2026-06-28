// ============================================================
// NEXORA FOREX — Deriv WS Adapter (Privado)
// ============================================================
// Adaptador que usa a conexão privada (autenticada) gerida pela
// sessão em server.ts. Não cria WebSockets próprios — recebe o WS
// da sessão via getDerivWs().
//
// ─── Arquitectura: MessageRouter central ─────────────────────
// Em vez de cada chamada (buyContract/waitForContract/
// subscribeToTicks) registar o seu próprio ws.on('message', ...),
// todas partilham UM único MessageRouter por ligação WS (ver
// message-router.ts). O router faz o parse uma única vez e despacha
// por req_id (pedidos únicos) ou por subscrição (streams contínuos),
// eliminando o custo O(n) por mensagem que existe quando múltiplos
// bots da mesma sessão têm cada um o seu próprio listener.
//
// Correcções mantidas desta versão:
//   ✅ buy usa ask_price da resposta proposal (não params.stake)
//   ✅ req_id numérico incremental (não aleatório — pode colidir)
//   ✅ waitForContract: forget usa subscription.id do servidor
//   ✅ subscribeToTicks: filtra por symbol E pela subscrição
// ============================================================

import { WebSocket } from 'ws';
import { DerivWsAdapter } from './bot.types.js';
import { getMessageRouter } from './message-router.js';
import logger from '@utils/logger.js';

// ─── Erro tipado da Deriv ────────────────────────────────────

export class DerivApiError extends Error {
  readonly code: string;
  constructor(message: string, code: string) {
    super(message);
    this.name = 'DerivApiError';
    this.code = code;
  }
}

// ─── req_id incremental (global ao módulo) ───────────────────
// Um contador por processo evita colisões quando múltiplos bots/
// sessões partilham req_ids — cada chamada ao router pede um novo.

let globalReqIdCounter = 100_000;
function nextReqId(): number {
  return ++globalReqIdCounter;
}

// Converte o erro genérico que o MessageRouter rejeita (Error com
// .code anexado quando vem de msg.error da Deriv) num DerivApiError
// tipado, preservando o código para as estratégias reagirem (ex:
// InsufficientBalance).
function toDerivApiError(err: unknown, fallbackMessage: string): Error {
  if (err instanceof Error && 'code' in err && typeof (err as { code?: unknown }).code === 'string') {
    return new DerivApiError(err.message || fallbackMessage, (err as { code: string }).code);
  }
  return err instanceof Error ? err : new Error(fallbackMessage);
}

// ─── Mapeamento durationUnit ─────────────────────────────────

const DURATION_UNIT_MAP: Record<string, string> = {
  ticks: 't',
  s: 's',
  m: 'm',
  h: 'h',
  d: 'd',
  t: 't',
};

// ─── Factory ─────────────────────────────────────────────────

export function createDerivWsAdapter(
  getDerivWs: () => WebSocket | null,
): DerivWsAdapter {

  // ──────────────────────────────────────────────────────────
  // buyContract
  // Fluxo: proposal → (captura ask_price) → buy → contractId
  // Usa o MessageRouter central — sem listeners próprios.
  // ──────────────────────────────────────────────────────────

  async function buyContract(params: {
    symbol: string;
    contractType: string;
    stake: number;
    duration: number;
    durationUnit: string;
    currency: string;
  }): Promise<{ contractId: string }> {

    const ws = getDerivWs();
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      throw new Error('[DerivAdapter] WebSocket não está disponível');
    }

    const durationUnit = DURATION_UNIT_MAP[params.durationUnit] ?? params.durationUnit;
    const router = getMessageRouter(ws);

    // ── Passo 1: proposal ───────────────────────────────────
    // Captura proposalId E ask_price para usar no buy.
    const proposalReqId = nextReqId();
    let proposalMsg: Record<string, unknown>;
    try {
      proposalMsg = await router.request({
        proposal:          1,
        amount:            params.stake,
        basis:             'stake',
        contract_type:     params.contractType,
        currency:          params.currency,
        duration:          params.duration,
        duration_unit:     durationUnit,
        underlying_symbol: params.symbol,
      }, proposalReqId, 15_000);
    } catch (err) {
      throw toDerivApiError(err, 'Erro na proposta');
    }

    const proposal = proposalMsg.proposal as Record<string, unknown> | undefined;
    if (proposalMsg.msg_type !== 'proposal' || !proposal?.id) {
      throw new Error('Resposta de proposal inválida ou sem id');
    }

    const proposalId = String(proposal.id);
    // ask_price é o preço real a pagar — NUNCA usar params.stake aqui
    const askPrice = Number(proposal.ask_price ?? params.stake);

    logger.debug('[DerivAdapter] proposal recebida', {
      reqId: proposalReqId, symbol: params.symbol, stake: params.stake,
      contractType: params.contractType, duration: params.duration, durationUnit, askPrice,
    });

    // ── Passo 2: buy ────────────────────────────────────────
    // Reler WS — pode ter mudado se a sessão trocou de conta entre
    // o passo 1 e aqui.
    const ws2 = getDerivWs();
    if (!ws2 || ws2.readyState !== WebSocket.OPEN) {
      throw new Error('[DerivAdapter] WebSocket não está disponível (após proposal)');
    }
    const router2 = getMessageRouter(ws2);
    const buyReqId = nextReqId();

    let buyMsg: Record<string, unknown>;
    try {
      // CRÍTICO: price = ask_price da resposta proposal, NÃO params.stake
      buyMsg = await router2.request({ buy: proposalId, price: askPrice }, buyReqId, 15_000);
    } catch (err) {
      throw toDerivApiError(err, 'Erro ao comprar contrato');
    }

    const buy = buyMsg.buy as Record<string, unknown> | undefined;
    if (buyMsg.msg_type !== 'buy' || !buy?.contract_id) {
      throw new Error('Resposta buy inválida');
    }

    logger.debug('[DerivAdapter] buy confirmado', { reqId: buyReqId, proposalId, askPrice, contractId: buy.contract_id });

    return { contractId: String(buy.contract_id) };
  }

  // ──────────────────────────────────────────────────────────
  // waitForContract
  // Subscreve proposal_open_contract e resolve quando fecha.
  // Stream contínuo via router.subscribe() — várias mensagens com o
  // mesmo contract_id chegam até ele fechar.
  // ──────────────────────────────────────────────────────────

  async function waitForContract(contractId: string): Promise<{
    profit: number;
    won: boolean;
    entryTick: number;
    exitTick: number;
  }> {
    const ws = getDerivWs();
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      throw new Error('[DerivAdapter] WebSocket não está disponível');
    }
    const router = getMessageRouter(ws);
    const subKey = `poc:${contractId}`;

    return new Promise((resolve, reject) => {
      let settled = false;
      let subscriptionServerId: string | null = null;
      let subHandle: { reused: boolean; unsubscribe: () => void } | null = null;

      const cleanup = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);

        // Só envia "forget" à Deriv se este for o ÚLTIMO consumidor
        // desta subscrição — se outra chamada (ex: outro bot a
        // acompanhar o mesmo contrato) ainda estiver à escuta, não
        // cancelamos o stream por baixo dela. Equivalente ao
        // finalize() do SubscriptionManager oficial da Deriv.
        const wasLastConsumer = router.consumerCount(subKey) <= 1;
        subHandle?.unsubscribe();

        if (wasLastConsumer && subscriptionServerId && ws.readyState === WebSocket.OPEN) {
          try {
            ws.send(JSON.stringify({ forget: subscriptionServerId, req_id: nextReqId() }));
          } catch { /* ignorar */ }
        }
      };

      const timer = setTimeout(() => {
        cleanup();
        reject(new Error('Timeout aguardando resultado do contrato (35s)'));
      }, 35_000);

      subHandle = router.subscribe(
        subKey,
        (msg: Record<string, unknown>) => {
          const poc = msg.proposal_open_contract as Record<string, unknown> | undefined;
          return !!poc && String(poc.contract_id) === contractId;
        },
        (msg: Record<string, unknown>) => {
          if (settled) return;

          if (msg.error) {
            const err = msg.error as Record<string, unknown>;
            cleanup();
            reject(new DerivApiError(
              String(err.message ?? 'Erro ao acompanhar contrato'),
              String(err.code ?? 'UNKNOWN'),
            ));
            return;
          }

          const poc = msg.proposal_open_contract as Record<string, unknown>;

          // Capturar serverId na primeira mensagem
          const sub = msg.subscription as Record<string, unknown> | undefined;
          if (sub?.id && !subscriptionServerId) {
            subscriptionServerId = String(sub.id);
          }

          const isClosed =
            poc.status === 'sold' ||
            poc.is_sold === 1 ||
            poc.is_expired === 1 ||
            poc.is_settleable === 1;

          if (isClosed) {
            cleanup();
            const profit = Number(poc.profit ?? 0);
            resolve({
              profit,
              won:       profit > 0,
              entryTick: Number(poc.entry_tick ?? 0),
              exitTick:  Number(poc.exit_tick  ?? 0),
            });
          }
        },
      );

      // Se a subscrição já existia (outro bot já está a acompanhar
      // este mesmo contrato), não enviamos novo pedido à Deriv — só
      // ficamos à escuta das mensagens que já estão a chegar para essa
      // key, exactamente como o SubscriptionManager oficial reutiliza
      // a fonte em vez de duplicar o pedido.
      if (subHandle.reused) {
        logger.debug('[DerivAdapter] Subscrição de contrato reutilizada', { contractId });
        return;
      }

      // Envia a mensagem de subscrição directamente (sem passar por
      // router.request()): a Deriv ecoa o req_id enviado na PRIMEIRA
      // resposta de proposal_open_contract — exactamente a mensagem
      // que a subscrição acima precisa de receber. Se usássemos
      // request() aqui, essa primeira mensagem seria capturada como
      // "resposta de pedido único" e nunca chegaria ao handler da
      // subscrição.
      const reqId = nextReqId();
      try {
        ws.send(JSON.stringify({
          proposal_open_contract: 1,
          contract_id:            Number(contractId),
          subscribe:              1,
          req_id:                 reqId,
        }));
        logger.debug('[DerivAdapter] A aguardar contrato', { reqId, contractId });
      } catch (err) {
        cleanup();
        reject(err instanceof Error ? err : new Error('Falha ao enviar subscrição'));
      }
    });
  }

  // ──────────────────────────────────────────────────────────
  // subscribeToTicks
  // Subscrição contínua de ticks para um símbolo, via router.
  // ──────────────────────────────────────────────────────────

  function subscribeToTicks(
    symbol: string,
    onTick: (price: number) => void,
  ): { unsubscribe: () => void } {
    const ws = getDerivWs();
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      logger.warn('[DerivAdapter] WebSocket indisponível para ticks', { symbol });
      return { unsubscribe: () => {} };
    }

    const router = getMessageRouter(ws);
    const subKey = `ticks:${symbol}`;
    let serverId: string | null = null;
    let unsubscribed = false;

    const handler = (msg: Record<string, unknown>) => {
      if (unsubscribed) return;
      const tick = msg.tick as Record<string, unknown>;

      if (!serverId) {
        const sub = msg.subscription as Record<string, unknown> | undefined;
        if (sub?.id) serverId = String(sub.id);
      }

      onTick(Number(tick.quote));
    };

    const subHandle = router.subscribe(
      subKey,
      (msg: Record<string, unknown>) => {
        if (msg.msg_type !== 'tick') return false;
        const tick = msg.tick as Record<string, unknown> | undefined;
        return !!tick && tick.symbol === symbol;
      },
      handler,
    );

    // Se já havia outro consumidor a seguir o mesmo símbolo (ex:
    // outro bot), reutiliza a subscrição existente — não duplica o
    // pedido à Deriv, tal como o SubscriptionManager oficial.
    if (subHandle.reused) {
      logger.debug('[DerivAdapter] Subscrição de ticks reutilizada', { symbol });
    } else {
      const reqId = nextReqId();
      try {
        ws.send(JSON.stringify({ ticks: symbol, subscribe: 1, req_id: reqId }));
        logger.debug('[DerivAdapter] Subscrito a ticks', { symbol, reqId });
      } catch (err) {
        logger.error('[DerivAdapter] Falha ao subscrever ticks', { symbol, error: err });
      }
    }

    return {
      unsubscribe: () => {
        if (unsubscribed) return;
        unsubscribed = true;

        // Só envia "forget" se este for o último consumidor desta
        // key — se outro bot ainda estiver a seguir o mesmo símbolo,
        // não cancelamos o stream por baixo dele.
        const wasLastConsumer = router.consumerCount(subKey) <= 1;
        subHandle.unsubscribe();

        if (wasLastConsumer && serverId && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ forget: serverId, req_id: nextReqId() }));
          logger.debug('[DerivAdapter] Tick unsubscribe enviado', { symbol, serverId });
        }
      },
    };
  }

  return { buyContract, waitForContract, subscribeToTicks };
}
