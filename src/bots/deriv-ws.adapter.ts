// ============================================================
// NEXORA FOREX — Deriv WS Adapter v3 (auto_* da Deriv)
// ============================================================
// A Deriv executa o ciclo de compras (auto_start). Este adaptador:
//   • inicia/retoma o acompanhamento de um run (subscribe:1)
//   • pausa / retoma / pára o run
//   • lê o resultado real de cada contrato (proposal_open_contract)
// Já não existe buy/proposal/ticks aqui.
// ============================================================

import { WebSocket } from 'ws';
import {
  AutoHandlers,
  AutoRunInfo,
  AutoStartParams,
  ContractOutcome,
  DerivWsAdapter,
  ManualBuyParams,
} from './bot.types.js';
import { getMessageRouter } from './message-router.js';
import { nextReqId } from '@utils/req-id.js';
import { roundToCurrency } from '@utils/currency.js';
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

function toDerivApiError(err: unknown, fallback: string): Error {
  if (err instanceof DerivApiError) return err;
  if (err instanceof Error && 'code' in err && typeof (err as { code?: unknown }).code === 'string') {
    return new DerivApiError(err.message || fallback, (err as { code: string }).code);
  }
  return err instanceof Error ? err : new Error(fallback);
}

const num = (v: unknown): number => (v === null || v === undefined || v === '' ? 0 : Number(v));
const FIRST_RESPONSE_MS = 15_000;
const DEFAULT_WAIT_MS = 30 * 60_000;
const backoffMs = (attempt: number) => Math.min(250 * 2 ** attempt, 5_000);
const UNIT: Record<string, string> = { ticks: 't', t: 't', s: 's', m: 'm', h: 'h', d: 'd' };

// ─── Parse do objecto "run" (defensivo) ──────────────────────
// A resposta traz o run dentro de auto_start / auto_get / auto_pause /
// auto_resume / auto_stop. Procuramos o 1.º objecto com run_id.

function toContractId(c: unknown): string | null {
  if (c === null || c === undefined) return null;
  if (typeof c === 'string' || typeof c === 'number') return String(c);
  if (typeof c === 'object') {
    const o = c as Record<string, unknown>;
    const id = o.contract_id ?? o.id;
    return id === undefined || id === null ? null : String(id);
  }
  return null;
}

export function parseRun(msg: Record<string, unknown>): AutoRunInfo | null {
  for (const [key, value] of Object.entries(msg)) {
    if (key === 'echo_req' || key === 'subscription') continue;
    if (!value || typeof value !== 'object' || Array.isArray(value)) continue;
    const r = value as Record<string, unknown>;
    if (typeof r.run_id !== 'string') continue;

    const contracts = Array.isArray(r.contracts) ? r.contracts : [];
    const contractIds = contracts.map(toContractId).filter((x): x is string => x !== null);
    const status = r.status === 'paused' || r.status === 'stopped' ? r.status : 'running';

    return {
      runId: r.run_id,
      status,
      stopReason: r.stop_reason as AutoRunInfo['stopReason'],
      stopReasonCode: typeof r.stop_reason_code === 'string' ? r.stop_reason_code : undefined,
      totalStake: num(r.total_stake),
      totalPayout: num(r.total_payout),
      contractIds,
    };
  }
  return null;
}

// ─── Factory ─────────────────────────────────────────────────

export function createDerivWsAdapter(
  getDerivWs: () => WebSocket | null,
  _options: { directBuy?: boolean } = {}, // mantido por compatibilidade com server.ts
): DerivWsAdapter {
  const watchers = new Map<string, Promise<ContractOutcome>>();

  async function requireOpenWs(waitMs = 5_000): Promise<WebSocket> {
    const deadline = Date.now() + waitMs;
    for (;;) {
      const ws = getDerivWs();
      if (ws && ws.readyState === WebSocket.OPEN) return ws;
      if (Date.now() >= deadline) throw new Error('[DerivAdapter] WebSocket não está disponível');
      await new Promise((r) => setTimeout(r, 100));
    }
  }

  // ──────────────────────────────────────────────────────────
  // auto_start
  // ──────────────────────────────────────────────────────────

  async function autoStart(
    params: AutoStartParams,
    h: AutoHandlers,
  ): Promise<{ run: AutoRunInfo; detach: () => void }> {
    const router = getMessageRouter(await requireOpenWs());

    return new Promise((resolve, reject) => {
      let first = true;
      let handle: { close: () => void } | null = null;

      const timer = setTimeout(() => {
        if (!first) return;
        first = false;
        handle?.close();
        // O run pode ter arrancado: o bot deve reconciliar antes de repetir.
        reject(new Error('Timeout no auto_start — estado do run desconhecido, verifique antes de repetir'));
      }, FIRST_RESPONSE_MS);

      handle = router.openStream(
        `auto:${params.tag}`,
        {
          auto_start: 1,
          contract_template: params.contractTemplate,
          strategy_id: params.strategyId,
          strategy_parameters: params.strategyParameters,
        },
        {
          onMessage: (msg) => {
            const run = parseRun(msg);
            if (!run) {
              logger.debug('[DerivAdapter] mensagem auto_* sem run_id', { keys: Object.keys(msg) });
              return;
            }
            if (first) {
              first = false;
              clearTimeout(timer);
              resolve({ run, detach: () => handle?.close() });
            } else {
              h.onUpdate(run);
            }
          },
          onError: (err) => {
            if (first) {
              first = false;
              clearTimeout(timer);
              reject(toDerivApiError(err, 'Erro no auto_start'));
            } else {
              h.onError(toDerivApiError(err, 'Erro no stream do run'));
            }
          },
          onDisconnect: () => {
            if (first) {
              first = false;
              clearTimeout(timer);
              reject(new Error('Ligação caiu durante o auto_start — estado do run desconhecido'));
            } else {
              h.onDisconnect();
            }
          },
        },
      );
    });
  }

  // ──────────────────────────────────────────────────────────
  // auto_get + subscribe (re-anexar após reconexão)
  // ──────────────────────────────────────────────────────────

  function autoAttach(tag: string, runId: string, h: AutoHandlers): { detach: () => void } {
    const ws = getDerivWs();
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      const t = setTimeout(() => h.onDisconnect(), 0);
      return { detach: () => clearTimeout(t) };
    }
    const router = getMessageRouter(ws);
    const handle = router.openStream(
      `auto:${tag}`,
      { auto_get: 1, run_id: runId },
      {
        onMessage: (msg) => {
          const run = parseRun(msg);
          if (run && run.runId === runId) h.onUpdate(run);
        },
        onError: (err) => h.onError(toDerivApiError(err, 'Erro ao re-subscrever o run')),
        onDisconnect: () => h.onDisconnect(),
      },
    );
    return { detach: () => handle.close() };
  }

  // ──────────────────────────────────────────────────────────
  // auto_pause | auto_resume | auto_stop
  // ──────────────────────────────────────────────────────────

  async function autoControl(
    action: 'pause' | 'resume' | 'stop',
    runId: string,
  ): Promise<AutoRunInfo> {
    const router = getMessageRouter(await requireOpenWs());
    let msg: Record<string, unknown>;
    try {
      msg = await router.request({ [`auto_${action}`]: 1, run_id: runId }, nextReqId(), FIRST_RESPONSE_MS);
    } catch (err) {
      throw toDerivApiError(err, `Erro em auto_${action}`);
    }
    const run = parseRun(msg);
    if (!run) throw new Error(`Resposta auto_${action} inválida`);
    return run;
  }

  // ──────────────────────────────────────────────────────────
  // Resultado real de um contrato (proposal_open_contract)
  // ──────────────────────────────────────────────────────────

  function watchContract(contractId: string, maxWaitMs = DEFAULT_WAIT_MS): Promise<ContractOutcome> {
    const existing = watchers.get(contractId);
    if (existing) return existing;
    const p = doWatch(contractId, maxWaitMs);
    watchers.set(contractId, p);
    const cleanup = () => { watchers.delete(contractId); };
    p.then(cleanup, cleanup);
    return p;
  }

  function doWatch(contractId: string, waitMs: number): Promise<ContractOutcome> {
    return new Promise<ContractOutcome>((resolve, reject) => {
      let settled = false;
      let handle: { close: () => void } | null = null;
      let retryTimer: ReturnType<typeof setTimeout> | null = null;

      const finish = (err: Error | null, result?: ContractOutcome) => {
        if (settled) return;
        settled = true;
        clearTimeout(deadline);
        if (retryTimer) clearTimeout(retryTimer);
        handle?.close();
        handle = null;
        if (err) reject(err); else resolve(result!);
      };

      const deadline = setTimeout(
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
        if (!ws || ws.readyState !== WebSocket.OPEN) { scheduleAttach(attempt + 1); return; }

        handle = getMessageRouter(ws).openStream(
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
              const profit = Number((hasSell ? num(poc.sell_price) - buyPrice : num(poc.profit)).toFixed(8));
              const won = status === 'won' ? true : status === 'lost' ? false : profit > 0;

              const entry = poc.entry_spot ?? poc.entry_tick;
              const exit = poc.exit_spot ?? poc.exit_tick;
              const exitStr = exit == null ? undefined : String(exit);
              const last = exitStr?.slice(-1);

              finish(null, {
                profit, won, buyPrice,
                entryTick: num(entry),
                exitTick: num(exit),
                entrySpot: entry == null ? undefined : String(entry),
                exitSpot: exitStr,
                exitDigit: last !== undefined && /\d/.test(last) ? Number(last) : undefined,
              });
            },
            onError: (err) => finish(toDerivApiError(err, 'Erro ao acompanhar contrato')),
            onDisconnect: () => { handle = null; scheduleAttach(0); },
          },
        );
      };

      attach(0);
    });
  }

  // ──────────────────────────────────────────────────────────
  // buyContract (Operar Manual) — buy directo
  // ──────────────────────────────────────────────────────────

  async function buyContract(p: ManualBuyParams) {
    const amount = roundToCurrency(p.stake, p.currency);
    const router = getMessageRouter(await requireOpenWs());

    let msg: Record<string, unknown>;
    try {
      msg = await router.request(
        {
          buy: 1,
          price: amount,
          parameters: {
            amount,
            basis: 'stake',
            contract_type: p.contractType,
            currency: p.currency,
            duration: p.duration,
            duration_unit: UNIT[p.durationUnit] ?? p.durationUnit,
            underlying_symbol: p.symbol,
            ...(p.barrier !== undefined ? { barrier: p.barrier } : {}),
          },
        },
        nextReqId(),
        15_000,
      );
    } catch (err) {
      const e = toDerivApiError(err, 'Erro ao comprar contrato');
      if (e.message.startsWith('Timeout')) {
        throw new Error(`${e.message} — estado da compra desconhecido, verifique o portfólio antes de repetir`);
      }
      throw e;
    }

    const buy = msg.buy as Record<string, unknown> | undefined;
    if (!buy?.contract_id) throw new Error('Resposta buy inválida');
    return {
      contractId: String(buy.contract_id),
      payout: buy.payout === undefined ? undefined : Number(buy.payout),
      buyPrice: buy.buy_price === undefined ? undefined : Number(buy.buy_price),
      balanceAfter: buy.balance_after === undefined ? undefined : Number(buy.balance_after),
    };
  }

  return { buyContract, autoStart, autoAttach, autoControl, watchContract };
}

