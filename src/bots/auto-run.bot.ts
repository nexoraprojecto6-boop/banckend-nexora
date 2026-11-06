// ============================================================
// NEXORA FOREX — AutoRunBot v1
// ============================================================
// Um "bot" = um run auto_* da Deriv (martingale | dalembert).
// A Deriv decide e compra; nós só: iniciamos, pausamos/retomamos/paramos,
// acompanhamos o run e registamos o resultado real de cada contrato.
// Eventos idênticos aos da versão anterior → frontend não muda.
// ============================================================

import { EventEmitter } from 'events';
import {
  AUTO_CONTRACT_TYPES,
  AutoHandlers,
  AutoRunInfo,
  BARRIER_CONTRACTS,
  BotConfig,
  BotEvent,
  BotEventType,
  BotLogEntry,
  BotState,
  BotStats,
  BotStatus,
  BotStrategyType,
  ContractOutcome,
  DerivWsAdapter,
  LogLevel,
  TradeResult,
} from './bot.types.js';
import { classifyDerivError } from '@utils/deriv-errors.js';
import { formatMoney, roundToCurrency } from '@utils/currency.js';

const DURATION_UNIT: Record<string, string> = { ticks: 't', t: 't', s: 's', m: 'm', h: 'h', d: 'd' };
const MAX_LOGS = 500;

function maxWaitMs(duration: number, unit: string): number {
  const margin = 60_000;
  switch (unit) {
    case 't': return duration * 2_000 + margin;
    case 's': return duration * 1_000 + margin;
    case 'm': return duration * 60_000 + margin;
    case 'h': return duration * 3_600_000 + margin;
    case 'd': return duration * 86_400_000 + margin;
    default: return 30 * 60_000;
  }
}

function initialStats(stake: number): BotStats {
  return {
    totalTrades: 0, wins: 0, losses: 0, totalProfit: 0, totalLoss: 0, netPnL: 0,
    winRate: 0, currentStreak: 0, currentStake: stake, peakProfit: 0, maxDrawdown: 0,
  };
}

export class AutoRunBot extends EventEmitter {
  private state: BotState;
  private runId: string | null = null;
  private detachStream: (() => void) | null = null;
  private seen = new Set<string>();
  private reattachTimer: ReturnType<typeof setTimeout> | null = null;
  private reattachAttempt = 0;
  private finalized = false;

  constructor(
    id: string,
    name: string,
    strategy: BotStrategyType,
    config: BotConfig,
    private derivWs: DerivWsAdapter,
  ) {
    super();
    this.state = {
      id, name, strategy, status: 'idle', config,
      stats: initialStats(config.initialStake), logs: [],
    };
  }

  // ─── Estado ───────────────────────────────────────────────

  getState(): BotState {
    return {
      ...this.state,
      config: { ...this.state.config },
      stats: { ...this.state.stats },
      logs: [...this.state.logs],
    };
  }

  private money(n: number | string): string {
    return formatMoney(n, this.state.config.currency);
  }

  // ─── Validação e pedido auto_start ────────────────────────

  private buildRequest(): { template: Record<string, unknown>; params: Record<string, unknown> } {
    const c = this.state.config;
    const p = c.strategyParams ?? {};
    const cur = c.currency;
    const r = (n: number) => String(roundToCurrency(n, cur));

    if (!(AUTO_CONTRACT_TYPES as readonly string[]).includes(c.contractType)) {
      throw new Error(`Tipo de contrato não suportado pelo auto: ${c.contractType}`);
    }
    if (!(c.initialStake > 0)) throw new Error('Stake inicial inválido');
    if (!(c.maxLoss > 0) || !(c.maxProfit > 0)) {
      throw new Error('Stop loss e take profit são obrigatórios (> 0)');
    }
    const needsBarrier = (BARRIER_CONTRACTS as readonly string[]).includes(c.contractType);
    if (needsBarrier && (p.barrier === undefined || p.barrier === '')) {
      throw new Error(`${c.contractType} exige barrier`);
    }
    if (p.maxStake !== undefined && p.maxStake < c.initialStake) {
      throw new Error('maxStake tem de ser >= stake inicial');
    }

    const template: Record<string, unknown> = {
      amount: c.initialStake,
      basis: 'stake',
      contract_type: c.contractType,
      currency: cur,
      duration: c.duration,
      duration_unit: DURATION_UNIT[c.durationUnit] ?? c.durationUnit,
      underlying_symbol: c.symbol,
      ...(p.barrier !== undefined && p.barrier !== '' ? { barrier: String(p.barrier) } : {}),
    };

    // additionalProperties:false na Deriv → só enviar chaves documentadas
    const params: Record<string, unknown> = {
      initial_stake: r(c.initialStake),
      take_profit: r(c.maxProfit),
      stop_loss: r(c.maxLoss),
      ...(p.maxStake !== undefined ? { max_stake: r(p.maxStake) } : {}),
      ...(c.maxTrades && c.maxTrades > 0 ? { max_contracts: Math.floor(c.maxTrades) } : {}),
    };

    if (this.state.strategy === 'martingale') {
      const m = p.multiplier ?? 2;
      if (!(m > 1 && m <= 10)) throw new Error('multiplier tem de estar em (1, 10]');
      params.multiplier = m;
    } else {
      if (!(p.unit !== undefined && p.unit > 0)) throw new Error('D\'Alembert exige unit > 0');
      params.unit = r(p.unit);
    }

    return { template, params };
  }

  // ─── Ciclo de vida ────────────────────────────────────────

  async start(): Promise<void> {
    if (this.state.status === 'running' || this.state.status === 'paused') return;

    const { template, params } = this.buildRequest(); // lança erro claro se inválido

    this.finalized = false;
    this.seen.clear();
    this.reattachAttempt = 0;
    this.state.stats = initialStats(this.state.config.initialStake);
    this.state.lastError = undefined;
    this.state.stoppedAt = undefined;
    this.state.status = 'running';

    try {
      const { run, detach } = await this.derivWs.autoStart(
        {
          tag: this.state.id,
          strategyId: this.state.strategy,
          contractTemplate: template,
          strategyParameters: params,
        },
        this.handlers(),
      );
      this.runId = run.runId;
      this.state.runId = run.runId;
      this.detachStream = detach;
      this.state.startedAt = new Date();
      this.log('info', `Run iniciado na Deriv — ${this.state.strategy} (${run.runId})`);
      this.emit('bot:started', this.ev('bot:started', { runId: run.runId }));
      this.onRun(run);
    } catch (err) {
      this.fail(err);
      throw err;
    }
  }

  async stop(reason = 'manual'): Promise<void> {
    if (this.finalized) return;
    if (this.state.status === 'idle') return;
    this.finalized = true;
    this.clearReattach();

    if (this.runId) {
      try {
        await this.derivWs.autoControl('stop', this.runId);
      } catch (err) {
        this.log('error', `Falha no auto_stop: ${err instanceof Error ? err.message : String(err)}`);
      }
    }
    this.detach();
    this.state.status = 'stopped';
    this.state.stoppedAt = new Date();
    this.log('info', `Bot parado — motivo: ${reason}`);
    this.emit('bot:stopped', this.ev('bot:stopped', { reason }));
  }

  /** Nunca rejeita: erros ficam no log (as rotas podem chamar sem await). */
  async pause(): Promise<void> {
    if (this.state.status !== 'running' || !this.runId) return;
    try {
      await this.derivWs.autoControl('pause', this.runId);
      this.state.status = 'paused';
      this.log('info', 'Bot pausado');
      this.emit('bot:paused', this.ev('bot:paused', {}));
    } catch (err) {
      this.log('error', `Falha ao pausar: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  async resume(): Promise<void> {
    if (this.state.status !== 'paused' || !this.runId) return;
    try {
      await this.derivWs.autoControl('resume', this.runId);
      this.state.status = 'running';
      this.log('info', 'Bot retomado');
      this.emit('bot:resumed', this.ev('bot:resumed', {}));
    } catch (err) {
      this.log('error', `Falha ao retomar: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  // ─── Acompanhamento do run ────────────────────────────────

  private handlers(): AutoHandlers {
    return {
      onUpdate: (run) => {
        this.reattachAttempt = 0;
        this.onRun(run);
      },
      onError: (err) => {
        this.log('warn', `Erro no stream do run: ${err.message}`);
        this.scheduleReattach();
      },
      onDisconnect: () => {
        this.log('warn', 'Ligação caiu — a re-subscrever o run (continua a correr na Deriv)');
        this.scheduleReattach();
      },
    };
  }

  private scheduleReattach(): void {
    if (this.finalized || !this.runId) return;
    if (this.state.status !== 'running' && this.state.status !== 'paused') return;
    if (this.reattachTimer) return;

    const delay = Math.min(500 * 2 ** this.reattachAttempt, 10_000);
    this.reattachAttempt++;
    this.reattachTimer = setTimeout(() => {
      this.reattachTimer = null;
      if (this.finalized || !this.runId) return;
      this.detach();
      const { detach } = this.derivWs.autoAttach(this.state.id, this.runId, this.handlers());
      this.detachStream = detach;
    }, delay);
  }

  private clearReattach(): void {
    if (this.reattachTimer) clearTimeout(this.reattachTimer);
    this.reattachTimer = null;
  }

  private detach(): void {
    try { this.detachStream?.(); } catch { /* ligação já fechada */ }
    this.detachStream = null;
  }

  private onRun(run: AutoRunInfo): void {
    for (const id of run.contractIds) {
      if (this.seen.has(id)) continue;
      this.seen.add(id);
      this.emit('bot:trade_opened', this.ev('bot:trade_opened', { contractId: id }));
      this.track(id);
    }
    if (run.status === 'stopped') this.onRemoteStopped(run);
  }

  private track(contractId: string): void {
    const { duration, durationUnit } = this.state.config;
    const unit = DURATION_UNIT[durationUnit] ?? durationUnit;
    this.derivWs
      .watchContract(contractId, maxWaitMs(duration, unit))
      .then((out) => this.recordTrade(contractId, out))
      .catch((err) => {
        this.log('warn', `Sem resultado do contrato ${contractId}: ${err instanceof Error ? err.message : String(err)}`);
      });
  }

  private recordTrade(contractId: string, out: ContractOutcome): void {
    const s = this.state.stats;
    const result: TradeResult = {
      contractId, profit: out.profit, stake: out.buyPrice, won: out.won,
      entryTick: out.entryTick, exitTick: out.exitTick, exitDigit: out.exitDigit,
      entryTime: new Date(), exitTime: new Date(),
    };

    s.totalTrades += 1;
    s.currentStake = out.buyPrice;
    if (out.won) {
      s.wins += 1;
      s.totalProfit += out.profit;
      s.currentStreak = s.currentStreak > 0 ? s.currentStreak + 1 : 1;
    } else {
      s.losses += 1;
      s.totalLoss += Math.abs(out.profit);
      s.currentStreak = s.currentStreak < 0 ? s.currentStreak - 1 : -1;
    }
    s.netPnL = s.totalProfit - s.totalLoss;
    s.winRate = (s.wins / s.totalTrades) * 100;
    s.peakProfit = Math.max(s.peakProfit, s.netPnL);
    s.maxDrawdown = Math.max(s.maxDrawdown, s.peakProfit - s.netPnL);

    this.log('trade', out.won
      ? `✅ Ganhou ${this.money(out.profit.toFixed(2))} (stake ${this.money(out.buyPrice.toFixed(2))})`
      : `❌ Perdeu ${this.money(Math.abs(out.profit).toFixed(2))} (stake ${this.money(out.buyPrice.toFixed(2))})`);

    this.emit('bot:trade_closed', this.ev('bot:trade_closed', {
      contractId: result.contractId,
      profit: result.profit,
      won: result.won,
      stake: result.stake,
      entryTick: result.entryTick,
      exitTick: result.exitTick,
      exitDigit: result.exitDigit,
      contractType: this.state.config.contractType,
    }));
    this.emit('bot:stats_updated', this.ev('bot:stats_updated', { stats: { ...s } }));
  }

  // ─── A Deriv parou o run ──────────────────────────────────

  private onRemoteStopped(run: AutoRunInfo): void {
    if (this.finalized) return;
    this.finalized = true;
    this.clearReattach();
    this.detach();
    this.state.stoppedAt = new Date();
    const code = run.stopReasonCode ?? '';

    if (run.stopReason === 'condition_triggered') {
      const reason = /profit/i.test(code) ? 'max_profit_reached'
        : /loss/i.test(code) ? 'max_loss_reached'
        : /contract|trade/i.test(code) ? 'max_trades_reached'
        : 'condition_triggered';
      this.state.status = 'stopped';
      this.log('info', `Run terminado pela Deriv — ${reason}`, { code });
      this.emit('bot:goal_reached', this.ev('bot:goal_reached', { reason, code }));
      this.emit('bot:stopped', this.ev('bot:stopped', { reason }));
      return;
    }

    if (run.stopReason === 'error') {
      if (/balance/i.test(code)) {
        this.emit('bot:insufficient_balance', this.ev('bot:insufficient_balance', { message: code }));
      }
      this.state.status = 'error';
      this.state.lastError = `Run parado pela Deriv com erro (${code || 'sem código'})`;
      this.log('error', this.state.lastError, { code });
      this.emit('bot:error', this.ev('bot:error', { error: this.state.lastError }));
      return;
    }

    this.state.status = 'stopped';
    this.log('info', 'Run parado');
    this.emit('bot:stopped', this.ev('bot:stopped', { reason: run.stopReason ?? 'stopped' }));
  }

  private fail(err: unknown): void {
    const c = classifyDerivError(err);
    this.finalized = true;
    this.clearReattach();
    this.detach();
    if (c.kind === 'insufficient_balance') {
      this.emit('bot:insufficient_balance', this.ev('bot:insufficient_balance', { message: c.message }));
    }
    this.state.status = 'error';
    this.state.lastError = c.userMessage || c.message;
    this.log('error', `Falha ao iniciar: ${this.state.lastError}`);
    this.emit('bot:error', this.ev('bot:error', { error: this.state.lastError }));
  }

  // ─── Helpers ──────────────────────────────────────────────

  private log(level: LogLevel, message: string, data?: Record<string, unknown>): void {
    const entry: BotLogEntry = { timestamp: new Date(), level, message, data };
    if (this.state.logs.length >= MAX_LOGS) this.state.logs.shift();
    this.state.logs.push(entry);
    this.emit('bot:log', this.ev('bot:log', { entry }));
  }

  private ev(type: BotEventType, payload: Record<string, unknown>): BotEvent {
    return { type, botId: this.state.id, payload };
  }

  get status(): BotStatus { return this.state.status; }
}
