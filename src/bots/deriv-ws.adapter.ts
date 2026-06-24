// ============================================================
// NEXORA FOREX — Deriv WS Adapter
// ============================================================
// Fluxo correcto da API Deriv para comprar um contrato:
//   1. Enviar "proposal" com os parâmetros do contrato
//   2. Aguardar resposta "proposal" com o id da proposta
//   3. Enviar "buy" com o proposalId + ask_price
//   4. Aguardar resposta "buy" com o contract_id
//   5. Subscrever "proposal_open_contract" para acompanhar
//   6. Resolver quando contract fecha (is_sold / status=sold)
//
// ─── Notas de robustez sob carga (centenas de sessões simultâneas) ───
// Cada chamada a buyContract/waitForContract regista um listener
// 'message' temporário no WebSocket da sessão. Com muitos bots a
// abrir/fechar trades ao mesmo tempo, é crítico garantir que esse
// listener é SEMPRE removido — mesmo em caminhos de erro, timeout,
// ou se o `ws` da sessão for substituído a meio (troca de conta).
// Sem isto, listeners mortos acumulam-se: cada mensagem recebida do
// WS passa por todos eles (custo O(n) por mensagem), e a memória
// cresce de forma não controlada ao longo do tempo.
// ============================================================

import { WebSocket } from 'ws';
import { DerivWsAdapter } from './bot.types.js';
import logger from '@utils/logger.js';

// Preserva o código de erro da Deriv (ex: "InsufficientBalance",
// "InputValidationFailed") para que as estratégias possam reagir de
// forma específica, em vez de fazer parsing frágil da mensagem.
export class DerivApiError extends Error {
  code: string;
  constructor(message: string, code: string) {
    super(message);
    this.name = 'DerivApiError';
    this.code = code;
  }
}

function genReqId(): number {
  return Math.floor(Math.random() * 900_000) + 100_000;
}

// ─── Mapeamento durationUnit → duration_unit da Deriv ────────
const DURATION_UNIT_MAP: Record<string, string> = {
  ticks: 't',
  s:     's',
  m:     'm',
  h:     'h',
  d:     'd',
  t:     't',
};

// Limite de segurança: se uma única ligação WS acumular mais do que
// isto em listeners 'message' activos, há uma fuga de algum tipo
// (cleanup a falhar em algum caminho). Regista um aviso para
// investigação em vez de deixar crescer silenciosamente.
const MAX_SAFE_LISTENERS = 50;

function warnIfListenerLeak(ws: WebSocket, context: string): void {
  const count = ws.listenerCount('message');
  if (count > MAX_SAFE_LISTENERS) {
    logger.warn('[DerivAdapter] Possível fuga de listeners detectada', { context, count });
  }
}

export function createDerivWsAdapter(getDerivWs: () => WebSocket | null): DerivWsAdapter {

  // ─── buyContract ──────────────────────────────────────────
  // Fluxo: proposal → buy → contractId

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
      throw new Error('Deriv WebSocket não está disponível');
    }

    const durationUnit = DURATION_UNIT_MAP[params.durationUnit] ?? params.durationUnit;

    // ── Passo 1: pedir proposal ─────────────────────────────
    const proposalId = await new Promise<string>((resolve, reject) => {
      const reqId = genReqId();
      let settled = false;
      let timer: ReturnType<typeof setTimeout>;

      function cleanup() {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        ws.removeListener('message', handler);
      }

      function handler(raw: Buffer | string) {
        if (settled) return; // protege contra mensagens tardias pós-cleanup
        try {
          const msg = JSON.parse(raw.toString());
          if (msg.req_id !== reqId) return;

          if (msg.error) {
            cleanup();
            reject(new DerivApiError(
              msg.error.message ?? 'Erro na proposta',
              msg.error.code ?? 'UNKNOWN',
            ));
            return;
          }
          if (msg.msg_type === 'proposal' && msg.proposal?.id) {
            cleanup();
            resolve(msg.proposal.id as string);
          } else {
            cleanup();
            reject(new Error('Resposta de proposal inválida'));
          }
        } catch {
          // ignorar mensagens não relacionadas (não JSON, ou de outro req)
        }
      }

      timer = setTimeout(() => {
        cleanup();
        reject(new Error('Timeout ao obter proposta'));
      }, 15_000);

      ws.on('message', handler);
      warnIfListenerLeak(ws, 'buyContract:proposal');

      const proposalPayload = {
        proposal:          1,
        amount:            params.stake,
        basis:             'stake',
        contract_type:     params.contractType,
        currency:          params.currency,
        duration:          params.duration,
        duration_unit:     durationUnit,
        underlying_symbol: params.symbol,
        req_id:            reqId,
      };
      ws.send(JSON.stringify(proposalPayload));
      logger.debug('[DerivAdapter] proposal enviada', {
        reqId, symbol: params.symbol, stake: params.stake,
        contractType: params.contractType, duration: params.duration,
        durationUnit,
      });
    });

    // ── Passo 2: comprar com o proposalId ──────────────────
    // Reler getDerivWs() — se a sessão trocou de conta entre o passo 1
    // e aqui, o ws antigo pode já não ser válido.
    const ws2 = getDerivWs();
    if (!ws2 || ws2.readyState !== WebSocket.OPEN) {
      throw new Error('Deriv WebSocket não está disponível (após proposal)');
    }

    return new Promise((resolve, reject) => {
      const reqId = genReqId();
      let settled = false;
      let timer: ReturnType<typeof setTimeout>;

      function cleanup() {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        ws2!.removeListener('message', handler);
      }

      function handler(raw: Buffer | string) {
        if (settled) return;
        try {
          const msg = JSON.parse(raw.toString());
          if (msg.req_id !== reqId) return;

          if (msg.error) {
            cleanup();
            reject(new DerivApiError(
              msg.error.message ?? 'Erro ao comprar contrato',
              msg.error.code ?? 'UNKNOWN',
            ));
            return;
          }
          if (msg.msg_type === 'buy' && msg.buy?.contract_id) {
            cleanup();
            resolve({ contractId: String(msg.buy.contract_id) });
          } else {
            cleanup();
            reject(new Error('Resposta buy inválida'));
          }
        } catch {
          // ignorar mensagens não relacionadas
        }
      }

      timer = setTimeout(() => {
        cleanup();
        reject(new Error('Timeout ao comprar contrato'));
      }, 15_000);

      ws2.on('message', handler);
      warnIfListenerLeak(ws2, 'buyContract:buy');

      ws2.send(JSON.stringify({ buy: proposalId, price: params.stake, req_id: reqId }));
      logger.debug('[DerivAdapter] buy enviado', { reqId, proposalId });
    });
  }

  // ─── waitForContract ──────────────────────────────────────
  // Subscreve proposal_open_contract e resolve quando fechar.
  // Timeout reduzido para 35s — contratos de duração curta (ticks/
  // segundos, o caso comum dos bots do catálogo) resolvem bem dentro
  // disto; 120s deixava listeners pendentes vivos demasiado tempo
  // sob carga alta, agravando o custo O(n) por mensagem recebida.

  async function waitForContract(contractId: string): Promise<{
    profit: number;
    won: boolean;
    entryTick: number;
    exitTick: number;
  }> {
    const ws = getDerivWs();
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      throw new Error('Deriv WebSocket não está disponível');
    }

    return new Promise((resolve, reject) => {
      const reqId = genReqId();
      let settled = false;
      let timer: ReturnType<typeof setTimeout>;
      let subscriptionId: string | null = null;

      function cleanup() {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        ws.removeListener('message', handler);
        // Cancela a subscrição na Deriv para não continuar a receber
        // updates deste contrato depois de já termos o resultado.
        if (subscriptionId && ws.readyState === WebSocket.OPEN) {
          try { ws.send(JSON.stringify({ forget: subscriptionId })); } catch { /* ignorar */ }
        }
      }

      function handler(raw: Buffer | string) {
        if (settled) return;
        try {
          const msg = JSON.parse(raw.toString());

          if (msg.error && msg.req_id === reqId) {
            cleanup();
            reject(new DerivApiError(msg.error.message ?? 'Erro ao acompanhar contrato', msg.error.code ?? 'UNKNOWN'));
            return;
          }

          const poc = msg.proposal_open_contract;
          if (!poc || String(poc.contract_id) !== contractId) return;

          if (poc.id && !subscriptionId) subscriptionId = poc.id as string;

          if (poc.status === 'sold' || poc.is_sold || poc.is_expired || poc.is_settleable) {
            cleanup();
            const profit = parseFloat(poc.profit ?? '0');
            resolve({
              profit,
              won:       profit > 0,
              entryTick: poc.entry_tick ?? 0,
              exitTick:  poc.exit_tick  ?? 0,
            });
          }
        } catch {
          // ignorar
        }
      }

      timer = setTimeout(() => {
        cleanup();
        reject(new Error('Timeout aguardando resultado do contrato'));
      }, 35_000);

      ws.on('message', handler);
      warnIfListenerLeak(ws, 'waitForContract');

      ws.send(JSON.stringify({
        proposal_open_contract: 1,
        contract_id:            parseInt(contractId),
        subscribe:              1,
        req_id:                 reqId,
      }));
    });
  }

  // ─── subscribeToTicks ─────────────────────────────────────

  function subscribeToTicks(
    symbol: string,
    onTick: (price: number) => void,
  ): { unsubscribe: () => void } {
    const ws = getDerivWs();
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      logger.warn('[DerivAdapter] WebSocket indisponível para ticks');
      return { unsubscribe: () => {} };
    }

    const reqId = genReqId();
    let subId: string | null = null;
    let unsubscribed = false;

    function handler(raw: Buffer | string) {
      if (unsubscribed) return;
      try {
        const msg = JSON.parse(raw.toString());
        if (msg.msg_type === 'tick' && msg.tick?.symbol === symbol) {
          subId = subId ?? msg.subscription?.id ?? null;
          onTick(parseFloat(msg.tick.quote));
        }
      } catch {
        // ignorar
      }
    }

    ws.on('message', handler);
    warnIfListenerLeak(ws, 'subscribeToTicks');
    ws.send(JSON.stringify({ ticks: symbol, subscribe: 1, req_id: reqId }));
    logger.debug('[DerivAdapter] Subscrito a ticks', { symbol });

    return {
      unsubscribe: () => {
        if (unsubscribed) return;
        unsubscribed = true;
        ws.removeListener('message', handler);
        if (subId && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ forget: subId }));
          logger.debug('[DerivAdapter] Tick subscription cancelada', { symbol, subId });
        }
      },
    };
  }

  return { buyContract, waitForContract, subscribeToTicks };
}
