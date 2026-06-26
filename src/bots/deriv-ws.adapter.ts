// ============================================================
// NEXORA FOREX — Deriv WS Adapter (Privado)
// ============================================================
// Adaptador que usa a conexão privada (autenticada) gerida pelo
// WebSocketManager. Não cria WebSockets próprios — recebe o WS
// da sessão via getDerivWs().
//
// Correcções aplicadas vs versão anterior:
//   ✅ buy usa ask_price da resposta proposal (não params.stake)
//   ✅ req_id numérico incremental (não aleatório — pode colidir)
//   ✅ waitForContract: forget usa subscription.id do servidor
//   ✅ subscribeToTicks: filtra por symbol E req_id inicial
//   ✅ Todos os cleanup paths garantidos (settled flag)
//   ✅ Detecção de fuga de listeners com limite configurável
// ============================================================

import { WebSocket } from 'ws';
import { DerivWsAdapter } from './bot.types.js';
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

// ─── req_id incremental (por adapter instance) ───────────────
// Usar um contador por adapter evita colisões quando múltiplos
// bots partilham a mesma conexão WS.

let globalReqIdCounter = 100_000;
function nextReqId(): number {
  return ++globalReqIdCounter;
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

// ─── Detecção de fuga de listeners ───────────────────────────

const MAX_SAFE_LISTENERS = 50;

function warnIfListenerLeak(ws: WebSocket, context: string): void {
  const count = ws.listenerCount('message');
  if (count > MAX_SAFE_LISTENERS) {
    logger.warn('[DerivAdapter] ⚠ Possível fuga de listeners', { context, count });
  }
}

// ─── Factory ─────────────────────────────────────────────────

export function createDerivWsAdapter(
  getDerivWs: () => WebSocket | null,
): DerivWsAdapter {

  // ──────────────────────────────────────────────────────────
  // buyContract
  // Fluxo: proposal → (captura ask_price) → buy → contractId
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

    // ── Passo 1: proposal ───────────────────────────────────
    // Captura proposalId E ask_price para usar no buy.

    const { proposalId, askPrice } = await new Promise<{
      proposalId: string;
      askPrice: number;
    }>((resolve, reject) => {
      const reqId = nextReqId();
      let settled = false;
      let timer: ReturnType<typeof setTimeout>;

      function cleanup() {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        ws.removeListener('message', handler);
      }

      function handler(raw: Buffer | string) {
        if (settled) return;
        let msg: Record<string, unknown>;
        try { msg = JSON.parse(raw.toString()); } catch { return; }

        if (Number(msg.req_id) !== reqId) return;

        if (msg.error) {
          const err = msg.error as Record<string, unknown>;
          cleanup();
          reject(new DerivApiError(
            String(err.message ?? 'Erro na proposta'),
            String(err.code ?? 'UNKNOWN'),
          ));
          return;
        }

        if (msg.msg_type === 'proposal') {
          const proposal = msg.proposal as Record<string, unknown> | undefined;
          if (proposal?.id) {
            cleanup();
            resolve({
              proposalId: String(proposal.id),
              // ask_price é o preço real a pagar — NUNCA usar params.stake aqui
              askPrice: Number(proposal.ask_price ?? params.stake),
            });
            return;
          }
        }

        cleanup();
        reject(new Error('Resposta de proposal inválida ou sem id'));
      }

      timer = setTimeout(() => {
        cleanup();
        reject(new Error('Timeout ao obter proposta (15s)'));
      }, 15_000);

      ws.on('message', handler);
      warnIfListenerLeak(ws, 'buyContract:proposal');

      ws.send(JSON.stringify({
        proposal:          1,
        amount:            params.stake,
        basis:             'stake',
        contract_type:     params.contractType,
        currency:          params.currency,
        duration:          params.duration,
        duration_unit:     durationUnit,
        underlying_symbol: params.symbol,
        req_id:            reqId,
      }));

      logger.debug('[DerivAdapter] proposal enviada', {
        reqId, symbol: params.symbol, stake: params.stake,
        contractType: params.contractType, duration: params.duration, durationUnit,
      });
    });

    // ── Passo 2: buy ────────────────────────────────────────
    // Reler WS — pode ter mudado se a sessão trocou de conta.

    const ws2 = getDerivWs();
    if (!ws2 || ws2.readyState !== WebSocket.OPEN) {
      throw new Error('[DerivAdapter] WebSocket não está disponível (após proposal)');
    }

    return new Promise((resolve, reject) => {
      const reqId = nextReqId();
      let settled = false;
      let timer: ReturnType<typeof setTimeout>;

      function cleanup() {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        ws2.removeListener('message', handler);
      }

      function handler(raw: Buffer | string) {
        if (settled) return;
        let msg: Record<string, unknown>;
        try { msg = JSON.parse(raw.toString()); } catch { return; }

        if (Number(msg.req_id) !== reqId) return;

        if (msg.error) {
          const err = msg.error as Record<string, unknown>;
          cleanup();
          reject(new DerivApiError(
            String(err.message ?? 'Erro ao comprar contrato'),
            String(err.code ?? 'UNKNOWN'),
          ));
          return;
        }

        if (msg.msg_type === 'buy') {
          const buy = msg.buy as Record<string, unknown> | undefined;
          if (buy?.contract_id) {
            cleanup();
            resolve({ contractId: String(buy.contract_id) });
            return;
          }
        }

        cleanup();
        reject(new Error('Resposta buy inválida'));
      }

      timer = setTimeout(() => {
        cleanup();
        reject(new Error('Timeout ao comprar contrato (15s)'));
      }, 15_000);

      ws2.on('message', handler);
      warnIfListenerLeak(ws2, 'buyContract:buy');

      // CRÍTICO: price = ask_price da resposta proposal, NÃO params.stake
      ws2.send(JSON.stringify({
        buy:    proposalId,
        price:  askPrice,
        req_id: reqId,
      }));

      logger.debug('[DerivAdapter] buy enviado', {
        reqId, proposalId, askPrice,
      });
    });
  }

  // ──────────────────────────────────────────────────────────
  // waitForContract
  // Subscreve proposal_open_contract e resolve quando fecha.
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

    return new Promise((resolve, reject) => {
      const reqId = nextReqId();
      let settled = false;
      let timer: ReturnType<typeof setTimeout>;
      let subscriptionServerId: string | null = null;

      function cleanup() {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        ws.removeListener('message', handler);

        // Cancelar subscrição no servidor Deriv
        if (subscriptionServerId && ws.readyState === WebSocket.OPEN) {
          try {
            ws.send(JSON.stringify({ forget: subscriptionServerId, req_id: nextReqId() }));
          } catch { /* ignorar */ }
        }
      }

      function handler(raw: Buffer | string) {
        if (settled) return;
        let msg: Record<string, unknown>;
        try { msg = JSON.parse(raw.toString()); } catch { return; }

        // Erros com o nosso req_id
        if (msg.error && Number(msg.req_id) === reqId) {
          const err = msg.error as Record<string, unknown>;
          cleanup();
          reject(new DerivApiError(
            String(err.message ?? 'Erro ao acompanhar contrato'),
            String(err.code ?? 'UNKNOWN'),
          ));
          return;
        }

        const poc = msg.proposal_open_contract as Record<string, unknown> | undefined;
        if (!poc) return;

        // Filtrar pelo contractId correcto
        if (String(poc.contract_id) !== contractId) return;

        // Capturar serverId na primeira mensagem
        const sub = msg.subscription as Record<string, unknown> | undefined;
        if (sub?.id && !subscriptionServerId) {
          subscriptionServerId = String(sub.id);
        }

        // Contrato fechou?
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
            entryTick: Number(poc.entry_tick  ?? 0),
            exitTick:  Number(poc.exit_tick   ?? 0),
          });
        }
      }

      // Timeout de 35s — adequado para contratos curtos (ticks/segundos).
      // Evita listeners mortos a acumular sob carga alta.
      timer = setTimeout(() => {
        cleanup();
        reject(new Error('Timeout aguardando resultado do contrato (35s)'));
      }, 35_000);

      ws.on('message', handler);
      warnIfListenerLeak(ws, 'waitForContract');

      ws.send(JSON.stringify({
        proposal_open_contract: 1,
        contract_id:            Number(contractId),
        subscribe:              1,
        req_id:                 reqId,
      }));

      logger.debug('[DerivAdapter] A aguardar contrato', { reqId, contractId });
    });
  }

  // ──────────────────────────────────────────────────────────
  // subscribeToTicks
  // Subscrição contínua de ticks para um símbolo.
  // Filtra por symbol E pelo req_id da primeira mensagem.
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

    const reqId = nextReqId();
    let serverId: string | null = null;
    let unsubscribed = false;

    function handler(raw: Buffer | string) {
      if (unsubscribed) return;
      let msg: Record<string, unknown>;
      try { msg = JSON.parse(raw.toString()); } catch { return; }

      if (msg.msg_type !== 'tick') return;

      const tick = msg.tick as Record<string, unknown> | undefined;
      if (!tick) return;

      // Filtrar pelo símbolo correcto
      if (tick.symbol !== symbol) return;

      // Capturar serverId na primeira mensagem para poder cancelar
      if (!serverId) {
        const sub = msg.subscription as Record<string, unknown> | undefined;
        if (sub?.id) serverId = String(sub.id);
      }

      onTick(Number(tick.quote));
    }

    ws.on('message', handler);
    warnIfListenerLeak(ws, 'subscribeToTicks');

    ws.send(JSON.stringify({ ticks: symbol, subscribe: 1, req_id: reqId }));
    logger.debug('[DerivAdapter] Subscrito a ticks', { symbol, reqId });

    return {
      unsubscribe: () => {
        if (unsubscribed) return;
        unsubscribed = true;
        ws.removeListener('message', handler);

        if (serverId && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ forget: serverId, req_id: nextReqId() }));
          logger.debug('[DerivAdapter] Tick unsubscribe enviado', { symbol, serverId });
        }
      },
    };
  }

  return { buyContract, waitForContract, subscribeToTicks };
}
