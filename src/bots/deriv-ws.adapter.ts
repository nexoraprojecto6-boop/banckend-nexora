// ============================================================
// NEXORA FOREX — Deriv WS Adapter v2 (conta autenticada)
// ============================================================
// Conforme a documentação oficial da Deriv (buy.md, proposal.md,
// proposal-open-contract.md, ticks.md).
//
// Correcções vs v1:
//   ✅ buy directo (buy:1 + parameters): 1 viagem de rede em vez de 2
//      (proposal → buy). Fallback de 2 passos com { directBuy:false }.
//   ✅ Acompanhamento do contrato começa NO MESMO INSTANTE em que o buy
//      responde (antes só começava quando a estratégia chamava
//      waitForContract → contratos de 1 tick podiam fechar antes).
//   ✅ Timeout de waitForContract proporcional à duração (era fixo 35 s:
//      contratos de 1m+ davam "Timeout" falso com o contrato ainda aberto)
//   ✅ Campos da API nova: entry_spot / exit_spot / buy_price /
//      sell_price (a v1 lia entry_tick/exit_tick → sempre 0, o que
//      quebrava os dígitos finais das estratégias Even/Odd e Over/Under)
//   ✅ Contrato só é dado como fechado com is_sold=1 ou status
//      sold|won|lost|cancelled. is_expired/is_settleable NÃO têm lucro
//      final → resultado registado como perda por engano.
//   ✅ Profit = sell_price − buy_price (exacto), fallback profit
//   ✅ Stake arredondado às casas decimais da moeda (martingale gerava
//      0.35000000000000003 → InputValidationFailed)
//   ✅ Streams (ticks / contrato) voltam a subscrever sozinhos após
//      reconexão do WS; erro de símbolo inválido é registado
//   ✅ Watchdog: ticks parados >30 s → nova subscrição
//   ✅ onTick recebe também pip_size (2.º argumento, opcional) para
//      calcular o último dígito correctamente (1.10 ≠ 1.1)
// ============================================================

import { WebSocket } from 'ws';
import { DerivWsAdapter } from './bot.types.js';
import { getMessageRouter } from './message-router.js';
import { nextReqId } from '@utils/req-id.js';
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

function toDerivApiError(err: unknown, fallbackMessage: string): Error {
  if (err instanceof DerivApiError) return err;
  if (err instanceof Error && 'code' in err && typeof (err as { code?: unknown }).code === 'string') {
    return new DerivApiError(err.message || fallbackMessage, (err as { code: string }).code);
  }
  return err instanceof Error ? err : new Error(fallbackMessage);
}

// ─── Utilitários ─────────────────────────────────────────────

const DURATION_UNIT_MAP: Record<string, string> = {
  ticks: 't', t: 't', s: 's', m: 'm', h: 'h', d: 'd',
};

const CRYPTO_8_DECIMALS = new Set(['BTC', 'ETH', 'LTC']);

function roundStake(stake: number, currency: string): number {
  const decimals = CRYPTO_8_DECIMALS.has(currency.toUpperCase()) ? 8 : 2;
  return Number(stake.toFixed(decimals));
}

/** Tempo máximo razoável até o contrato fechar (duração + margem). */
function maxWaitMs(duration: number, unit: string): number {
  const MARGIN = 45_000;
  switch (unit) {
    case 't': return duration * 2_000 + MARGIN;
    case 's': return duration * 1_000 + MARGIN;
    case 'm': return duration * 60_000 + MARGIN;
    case 'h': return duration * 3_600_000 + MARGIN;
    case 'd': return duration * 86_400_000 + MARGIN;
    default:  return 10 * 60_000;
  }
}

const backoffMs = (attempt: number) => Math.min(250 * 2 ** attempt, 5_000);
const num = (v: unknown): number => (v === null || v === undefined || v === '' ? 0 : Number(v));

interface ContractResult {
  profit: number;
  won: boolean;
  entryTick: number;
  exitTick: number;
  entrySpot?: string;
  exitSpot?: string;
  exitDigit?: number;
}

interface TickParams {
  symbol: string;
  contractType: string;
  stake: number;
  duration: number;
  durationUnit: string;
  currency: string;
  barrier?: string;
}

const TICK_STALL_MS = 30_000;
const DEFAULT_WAIT_MS = 10 * 60_000;

// ─── Factory ─────────────────────────────────────────────────

export function createDerivWsAdapter(
  getDerivWs: () => WebSocket | null,
  options: { directBuy?: boolean } = {},
): DerivWsAdapter {
  const directBuy = options.directBuy ?? true;
  const watchers = new Map<string, Promise<ContractResult>>();

  // Espera até 5 s por um WS aberto: durante uma reconexão da sessão
  // (OTP novo) o bot não deve falhar logo no primeiro pedido.
  async function requireOpenWs(waitMs = 5_000): Promise<WebSocket> {
    const deadline = Date.now() + waitMs;
    for (;;) {
      const ws = getDerivWs();
      if (ws && ws.readyState === WebSocket.OPEN) return ws;
      if (Date.now() >= deadline) throw new Error('[DerivAdapter] WebSocket não está disponível');
      await new Promise((r) => setTimeout(r, 100));
    }
  }

  function contractParameters(p: TickParams, unit: string, amount: number): Record<string, unknown> {
    return {
      amount,
      basis: 'stake',
      contract_type: p.contractType,
      currency: p.currency,
      duration: p.duration,
      duration_unit: unit,
      underlying_symbol: p.symbol,
      ...(p.barrier !== undefined ? { barrier: p.barrier } : {}),
    };
  }

  // ──────────────────────────────────────────────────────────
  // buyContract
  // ──────────────────────────────────────────────────────────

  async function buyContract(params: TickParams): Promise<{
    contractId: string;
    payout?: number;
    buyPrice?: number;
    balanceAfter?: number;
  }> {
    const unit = DURATION_UNIT_MAP[params.durationUnit] ?? params.durationUnit;
    const amount = roundStake(params.stake, params.currency);
    const router = getMessageRouter(await requireOpenWs());

    let buyMsg: Record<string, unknown>;

    try {
      if (directBuy) {
        // buy.md: buy:1 + parameters, price = preço máximo aceite.
        buyMsg = await router.request(
          { buy: 1, price: amount, parameters: contractParameters(params, unit, amount) },
          nextReqId(),
          15_000,
        );
      } else {
        const proposalMsg = await router.request(
          { proposal: 1, ...contractParameters(params, unit, amount) },
          nextReqId(),
          15_000,
        );
        const proposal = proposalMsg.proposal as Record<string, unknown> | undefined;
        if (!proposal?.id) throw new Error('Resposta de proposal inválida ou sem id');

        const router2 = getMessageRouter(await requireOpenWs());
        buyMsg = await router2.request(
          { buy: String(proposal.id), price: Number(proposal.ask_price ?? amount) },
          nextReqId(),
          15_000,
        );
      }
    } catch (err) {
      const e = toDerivApiError(err, 'Erro ao comprar contrato');
      if (e.message.startsWith('Timeout')) {
        // NÃO repetir: o contrato pode ter sido comprado.
        throw new Error(`${e.message} — estado da compra desconhecido, verifique o portfólio antes de repetir`);
      }
      throw e;
    }

    const buy = buyMsg.buy as Record<string, unknown> | undefined;
    if (!buy?.contract_id) throw new Error('Resposta buy inválida');

    const contractId = String(buy.contract_id);

    // Começa a acompanhar JÁ (mesmo tick de event-loop).
    startWatch(contractId, maxWaitMs(params.duration, unit));

    logger.debug('[DerivAdapter] buy confirmado', {
      contractId, symbol: params.symbol, contractType: params.contractType, stake: amount,
    });

    return {
      contractId,
      payout: buy.payout === undefined ? undefined : Number(buy.payout),
      buyPrice: buy.buy_price === undefined ? undefined : Number(buy.buy_price),
      balanceAfter: buy.balance_after === undefined ? undefined : Number(buy.balance_after),
    };
  }

  // ──────────────────────────────────────────────────────────
  // Acompanhamento de contrato
  // ──────────────────────────────────────────────────────────

  function startWatch(contractId: string, waitMs: number): Promise<ContractResult> {
    const existing = watchers.get(contractId);
    if (existing) return existing;

    const p = watchContract(contractId, waitMs);
    watchers.set(contractId, p);
    const cleanup = () => { watchers.delete(contractId); };
    p.then(cleanup, cleanup); // trata rejeição → sem unhandledRejection
    return p;
  }

  function watchContract(contractId: string, waitMs: number): Promise<ContractResult> {
    return new Promise<ContractResult>((resolve, reject) => {
      let settled = false;
      let handle: { close: () => void } | null = null;
      let retryTimer: ReturnType<typeof setTimeout> | null = null;
      let deadline: ReturnType<typeof setTimeout> | null = null;

      const finish = (err: Error | null, result?: ContractResult) => {
        if (settled) return;
        settled = true;
        if (deadline) clearTimeout(deadline);
        if (retryTimer) clearTimeout(retryTimer);
        handle?.close();
        handle = null;
        if (err) reject(err); else resolve(result!);
      };

      deadline = setTimeout(
        () => finish(new Error(`Timeout aguardando resultado do contrato ${contractId} (${Math.round(waitMs / 1000)}s)`)),
        waitMs,
      );

      const scheduleAttach = (attempt: number) => {
        if (settled) return;
        retryTimer = setTimeout(() => attach(attempt), backoffMs(attempt));
      };

      const attach = (attempt: number) => {
        if (settled) return;
        const ws = getDerivWs();
        if (!ws || ws.readyState !== WebSocket.OPEN) {
          scheduleAttach(attempt + 1);
          return;
        }
        const router = getMessageRouter(ws);
        handle = router.openStream(
          `poc:${contractId}`,
          { proposal_open_contract: 1, contract_id: Number(contractId) },
          {
            onMessage: (msg) => {
              const poc = msg.proposal_open_contract as Record<string, unknown> | undefined;
              if (!poc || String(poc.contract_id) !== contractId) return;

              const status = typeof poc.status === 'string' ? poc.status : null;
              const closed =
                Number(poc.is_sold) === 1 ||
                status === 'sold' || status === 'won' || status === 'lost' || status === 'cancelled';
              if (!closed) return;

              const buyPrice = num(poc.buy_price);
              const hasSell = poc.sell_price !== null && poc.sell_price !== undefined && poc.sell_price !== '';
              const rawProfit = hasSell ? num(poc.sell_price) - buyPrice : num(poc.profit);
              const profit = Number(rawProfit.toFixed(8));
              const won = status === 'won' ? true : status === 'lost' ? false : profit > 0;

              const entrySpot = poc.entry_spot ?? poc.entry_tick;
              const exitSpot = poc.exit_spot ?? poc.exit_tick;
              const exitSpotStr = exitSpot == null ? undefined : String(exitSpot);
              const lastChar = exitSpotStr?.slice(-1);
              const exitDigit = lastChar !== undefined && /\d/.test(lastChar) ? Number(lastChar) : undefined;

              finish(null, {
                profit,
                won,
                entryTick: num(entrySpot),
                exitTick: num(exitSpot),
                entrySpot: entrySpot == null ? undefined : String(entrySpot),
                exitSpot: exitSpotStr,
                exitDigit,
              });
            },
            onError: (err) => finish(toDerivApiError(err, 'Erro ao acompanhar contrato')),
            onDisconnect: () => {
              handle = null;
              logger.warn('[DerivAdapter] Ligação caiu a meio do contrato — a re-subscrever', { contractId });
              scheduleAttach(0);
            },
          },
        );
      };

      attach(0);
    });
  }

  function waitForContract(contractId: string): Promise<ContractResult> {
    return watchers.get(contractId) ?? startWatch(contractId, DEFAULT_WAIT_MS);
  }

  // ──────────────────────────────────────────────────────────
  // subscribeToTicks
  // ──────────────────────────────────────────────────────────

  function subscribeToTicks(
    symbol: string,
    onTick: (price: number, pipSize?: number) => void,
  ): { unsubscribe: () => void } {
    const key = `ticks:${symbol}`;
    let closed = false;
    let handle: { close: () => void } | null = null;
    let retryTimer: ReturnType<typeof setTimeout> | null = null;
    let lastTickAt = Date.now();
    let failures = 0;
    let routerRef: ReturnType<typeof getMessageRouter> | null = null;

    const scheduleAttach = (attempt: number) => {
      if (closed) return;
      if (retryTimer) clearTimeout(retryTimer);
      retryTimer = setTimeout(() => attach(attempt), backoffMs(attempt));
    };

    const attach = (attempt: number) => {
      if (closed) return;
      const ws = getDerivWs();
      if (!ws || ws.readyState !== WebSocket.OPEN) {
        scheduleAttach(attempt + 1);
        return;
      }
      const router = getMessageRouter(ws);
      routerRef = router;
      lastTickAt = Date.now();

      handle = router.openStream(key, { ticks: symbol }, {
        onMessage: (msg) => {
          const tick = msg.tick as Record<string, unknown> | undefined;
          if (!tick) return;
          const quote = Number(tick.quote);
          if (!Number.isFinite(quote)) return;
          lastTickAt = Date.now();
          failures = 0;
          try {
            onTick(quote, tick.pip_size === undefined ? undefined : Number(tick.pip_size));
          } catch (err) {
            logger.error('[DerivAdapter] onTick lançou erro', { symbol, error: err });
          }
        },
        onError: (err) => {
          handle = null;
          failures++;
          logger.error('[DerivAdapter] Erro na subscrição de ticks', {
            symbol, message: err.message, code: (err as { code?: string }).code, failures,
          });
          if (failures < 5) scheduleAttach(failures);
        },
        onDisconnect: () => {
          handle = null;
          scheduleAttach(0);
        },
      });
    };

    attach(0);

    // Watchdog: stream "mudo" → forçar nova subscrição
    const watchdog = setInterval(() => {
      if (closed || !handle || !routerRef) return;
      if (Date.now() - lastTickAt > TICK_STALL_MS) {
        logger.warn('[DerivAdapter] Ticks parados — a re-subscrever', { symbol });
        lastTickAt = Date.now();
        routerRef.resetStream(key);
      }
    }, 10_000);
    watchdog.unref?.();

    return {
      unsubscribe: () => {
        if (closed) return;
        closed = true;
        clearInterval(watchdog);
        if (retryTimer) clearTimeout(retryTimer);
        handle?.close();
        handle = null;
      },
    };
  }

  // ──────────────────────────────────────────────────────────
  // getProposal — payout sem comprar
  // ──────────────────────────────────────────────────────────

  async function getProposal(params: {
    symbol: string;
    contractType: string;
    duration: number;
    durationUnit: string;
    currency: string;
    barrier?: string;
    stake: number;
  }): Promise<{ payout: number }> {
    const unit = DURATION_UNIT_MAP[params.durationUnit] ?? params.durationUnit;
    const amount = roundStake(params.stake, params.currency);
    const router = getMessageRouter(await requireOpenWs());

    let msg: Record<string, unknown>;
    try {
      msg = await router.request({
        proposal: 1,
        amount,
        basis: 'stake',
        contract_type: params.contractType,
        currency: params.currency,
        duration: params.duration,
        duration_unit: unit,
        underlying_symbol: params.symbol,
        ...(params.barrier !== undefined ? { barrier: params.barrier } : {}),
      }, nextReqId(), 15_000);
    } catch (err) {
      throw toDerivApiError(err, 'Erro ao consultar proposta/payout');
    }

    const proposal = msg.proposal as Record<string, unknown> | undefined;
    if (proposal?.payout === undefined) throw new Error('Resposta de proposal inválida ou sem payout');

    return { payout: Number(proposal.payout) };
  }

  return { buyContract, waitForContract, subscribeToTicks, getProposal };
}
