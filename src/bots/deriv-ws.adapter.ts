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
// A Deriv usa "t" para ticks, não "ticks".
// O BotConfig usa os valores legíveis: ticks, s, m, h, d.
const DURATION_UNIT_MAP: Record<string, string> = {
  ticks: 't',
  s:     's',
  m:     'm',
  h:     'h',
  d:     'd',
  t:     't', // aceitar já no formato Deriv, por segurança
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

      const timer = setTimeout(() => {
        ws.removeListener('message', handler);
        reject(new Error('Timeout ao obter proposta'));
      }, 15_000);

      function handler(raw: Buffer | string) {
        try {
          const msg = JSON.parse(raw.toString());
          if (msg.req_id !== reqId) return;
          clearTimeout(timer);
          ws.removeListener('message', handler);

          if (msg.error) {
            reject(new DerivApiError(
              msg.error.message ?? 'Erro na proposta',
              msg.error.code ?? 'UNKNOWN',
            ));
            return;
          }
          if (msg.msg_type === 'proposal' && msg.proposal?.id) {
            resolve(msg.proposal.id as string);
          } else {
            reject(new Error('Resposta de proposal inválida'));
          }
        } catch {
          // ignorar mensagens não relacionadas
        }
      }

      ws.on('message', handler);
      ws.send(JSON.stringify(proposalPayload));
      logger.debug('[DerivAdapter] proposal enviada', {
        reqId, symbol: params.symbol, stake: params.stake,
        contractType: params.contractType, duration: params.duration,
        durationUnit,
      });
    });

    // ── Passo 2: comprar com o proposalId ──────────────────
    return new Promise((resolve, reject) => {
      const reqId = genReqId();

      const buyPayload = {
        buy:    proposalId,
        price:  params.stake,
        req_id: reqId,
      };

      const timer = setTimeout(() => {
        ws.removeListener('message', handler);
        reject(new Error('Timeout ao comprar contrato'));
      }, 15_000);

      function handler(raw: Buffer | string) {
        try {
          const msg = JSON.parse(raw.toString());
          if (msg.req_id !== reqId) return;
          clearTimeout(timer);
          ws.removeListener('message', handler);

          if (msg.error) {
            reject(new DerivApiError(
              msg.error.message ?? 'Erro ao comprar contrato',
              msg.error.code ?? 'UNKNOWN',
            ));
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
      ws.send(JSON.stringify(buyPayload));
      logger.debug('[DerivAdapter] buy enviado', { reqId, proposalId });
    });
  }

  // ─── waitForContract ──────────────────────────────────────
  // Subscreve proposal_open_contract e resolve quando fechar.

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
        contract_id:            parseInt(contractId),
        subscribe:              1,
        req_id:                 reqId,
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

          // Contrato fechado: status sold, ou expirado/liquidável
          if (poc.status === 'sold' || poc.is_sold || poc.is_expired || poc.is_settleable) {
            clearTimeout(timer);
            ws.removeListener('message', handler);
            const profit = parseFloat(poc.profit ?? '0');
            resolve({
              profit,
              won:       profit > 0,
              entryTick: poc.entry_tick   ?? 0,
              exitTick:  poc.exit_tick    ?? 0,
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
