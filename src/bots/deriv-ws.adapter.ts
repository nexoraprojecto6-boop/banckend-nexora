// ============================================================
// NEXORA FOREX — Deriv WS Adapter
// ============================================================
// Recebe o `derivWs` (WebSocket já autenticado via OTP) que
// existe na sessão do cliente em server.ts e expõe os métodos
// que as estratégias precisam: buyContract, waitForContract,
// subscribeToTicks.
//
// Nenhuma nova ligação WebSocket é criada aqui.
// ============================================================

import { WebSocket } from 'ws';
import { DerivWsAdapter } from './bot.types.js';
import logger from '@utils/logger.js';

function genReqId(): number {
  return Math.floor(Math.random() * 900_000) + 100_000;
}

/**
 * Mapeia valores de durationUnit legíveis para os códigos aceites pela Deriv API.
 * A Deriv espera: "t" (ticks), "s" (seconds), "m" (minutes), "h" (hours), "d" (days)
 */
function mapDurationUnit(unit: string): string {
  const map: Record<string, string> = {
    ticks: 't',
    tick: 't',
    t: 't',
    seconds: 's',
    second: 's',
    s: 's',
    minutes: 'm',
    minute: 'm',
    m: 'm',
    hours: 'h',
    hour: 'h',
    h: 'h',
    days: 'd',
    day: 'd',
    d: 'd',
  };
  const mapped = map[unit.toLowerCase()];
  if (!mapped) {
    logger.warn('[DerivAdapter] durationUnit desconhecido, a usar tal-qual', { unit });
    return unit;
  }
  return mapped;
}

/**
 * Cria um adapter a partir do WebSocket autenticado da sessão do cliente.
 * @param getDerivWs - callback que devolve o derivWs actual da sessão
 *                     (pode mudar se o cliente trocar de conta)
 */
export function createDerivWsAdapter(getDerivWs: () => WebSocket | null): DerivWsAdapter {

  // ─── buyContract ──────────────────────────────────────────
  // Fluxo correcto segundo a documentação Deriv:
  //   1. Enviar `proposal` com os parâmetros do contrato
  //   2. Receber o `proposal.id`
  //   3. Enviar `buy: proposalId` com o preço máximo aceite

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

    const durationUnit = mapDurationUnit(params.durationUnit);

    // ── Passo 1: obter proposal ────────────────────────────
    const proposalId = await new Promise<string>((resolve, reject) => {
      const reqId = genReqId();

      const payload = {
        proposal: 1,
        amount: params.stake,
        basis: 'stake',
        contract_type: params.contractType,
        currency: params.currency,
        duration: params.duration,
        duration_unit: durationUnit,
        underlying_symbol: params.symbol,
        req_id: reqId,
      };

      const timer = setTimeout(() => {
        ws.removeListener('message', handler);
        reject(new Error('Timeout ao obter proposal'));
      }, 30_000);

      function handler(raw: Buffer | string) {
        try {
          const msg = JSON.parse(raw.toString());
          if (msg.req_id !== reqId) return;
          clearTimeout(timer);
          ws.removeListener('message', handler);

          if (msg.error) {
            reject(new Error(msg.error.message ?? 'Erro ao obter proposal'));
            return;
          }
          if (msg.msg_type === 'proposal' && msg.proposal?.id) {
            resolve(String(msg.proposal.id));
          } else {
            reject(new Error('Resposta proposal inválida'));
          }
        } catch {
          // ignorar mensagens não relacionadas
        }
      }

      ws.on('message', handler);
      ws.send(JSON.stringify(payload));
      logger.debug('[DerivAdapter] proposal enviado', {
        reqId,
        symbol: params.symbol,
        contractType: params.contractType,
        stake: params.stake,
        duration: params.duration,
        durationUnit,
      });
    });

    // ── Passo 2: comprar com o proposal.id ────────────────
    return new Promise((resolve, reject) => {
      const reqId = genReqId();

      const payload = {
        buy: proposalId,
        price: params.stake,
        req_id: reqId,
      };

      const timer = setTimeout(() => {
        ws.removeListener('message', handler);
        reject(new Error('Timeout ao comprar contrato'));
      }, 30_000);

      function handler(raw: Buffer | string) {
        try {
          const msg = JSON.parse(raw.toString());
          if (msg.req_id !== reqId) return;
          clearTimeout(timer);
          ws.removeListener('message', handler);

          if (msg.error) {
            reject(new Error(msg.error.message ?? 'Erro ao comprar contrato'));
            return;
          }
          if (msg.msg_type === 'buy' && msg.buy?.contract_id) {
            resolve({ contractId: String(msg.buy.contract_id) });
          } else {
            reject(new Error('Resposta buy inválida'));
          }
        } catch {
          // ignorar mensagens não relacionadas
        }
      }

      ws.on('message', handler);
      ws.send(JSON.stringify(payload));
      logger.debug('[DerivAdapter] buy enviado', { reqId, proposalId, stake: params.stake });
    });
  }

  // ─── waitForContract ──────────────────────────────────────

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

      ws.send(JSON.stringify({
        proposal_open_contract: 1,
        contract_id: parseInt(contractId),
        subscribe: 1,
        req_id: reqId,
      }));

      const timer = setTimeout(() => {
        ws.removeListener('message', handler);
        reject(new Error('Timeout aguardando resultado do contrato'));
      }, 120_000);

      function handler(raw: Buffer | string) {
        try {
          const msg = JSON.parse(raw.toString());

          if (msg.error && msg.req_id === reqId) {
            clearTimeout(timer);
            ws.removeListener('message', handler);
            reject(new Error(msg.error.message));
            return;
          }

          const poc = msg.proposal_open_contract;
          if (!poc || String(poc.contract_id) !== contractId) return;

          // Aguardar contrato encerrado
          if (poc.status === 'sold' || poc.is_expired || poc.is_settleable) {
            clearTimeout(timer);
            ws.removeListener('message', handler);
            const profit = parseFloat(poc.profit ?? '0');
            resolve({
              profit,
              won: profit > 0,
              entryTick: poc.entry_tick ?? 0,
              exitTick: poc.exit_tick ?? 0,
            });
          }
        } catch {
          // ignorar
        }
      }

      ws.on('message', handler);
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

    ws.send(JSON.stringify({ ticks: symbol, subscribe: 1, req_id: reqId }));

    function handler(raw: Buffer | string) {
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
    logger.debug('[DerivAdapter] Subscrito a ticks', { symbol });

    return {
      unsubscribe: () => {
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
