// ============================================================
// NEXORA FOREX — Base Strategy
// ============================================================

import { EventEmitter } from 'events';
import {
  BotConfig,
  BotEvent,
  BotEventType,
  BotLogEntry,
  BotState,
  BotStats,
  BotStatus,
  BotStrategyType,
  LogLevel,
  TradeResult,
  DerivWsAdapter,
} from '../bot.types.js';
import { DerivApiError } from '../deriv-ws.adapter.js';

export function createInitialStats(initialStake: number): BotStats {
  return {
    totalTrades: 0,
    wins: 0,
    losses: 0,
    totalProfit: 0,
    totalLoss: 0,
    netPnL: 0,
    winRate: 0,
    currentStreak: 0,
    currentStake: initialStake,
    peakProfit: 0,
    maxDrawdown: 0,
  };
}

export abstract class BaseStrategy extends EventEmitter {
  protected state: BotState;
  protected active = false;
  protected derivWs: DerivWsAdapter;
  private loopTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    id: string,
    name: string,
    strategy: BotStrategyType,
    config: BotConfig,
    derivWs: DerivWsAdapter,
  ) {
    super();
    this.derivWs = derivWs;
    this.state = {
      id,
      name,
      strategy,
      status: 'idle',
      config,
      stats: createInitialStats(config.initialStake),
      logs: [],
    };
  }

  // ─── API pública ──────────────────────────────────────────

  async start(): Promise<void> {
    if (this.active) return;
    this.active = true;
    this.setStatus('running');
    this.state.startedAt = new Date();
    this.log('info', `Bot "${this.state.name}" iniciado — estratégia: ${this.state.strategy}`);
    this.emit('bot:started', this.buildEvent('bot:started', {}));
    await this.onStart();
    this.scheduleNextTick();
  }

  async stop(reason = 'manual'): Promise<void> {
    if (!this.active && this.state.status === 'stopped') return;
    this.active = false;
    if (this.loopTimer) clearTimeout(this.loopTimer);
    this.setStatus('stopped');
    this.state.stoppedAt = new Date();
    this.log('info', `Bot parado — motivo: ${reason}`);
    this.emit('bot:stopped', this.buildEvent('bot:stopped', { reason }));
    await this.onStop();
  }

  pause(): void {
    if (this.state.status !== 'running') return;
    this.active = false;
    if (this.loopTimer) clearTimeout(this.loopTimer);
    this.setStatus('paused');
    this.log('info', 'Bot pausado');
    this.emit('bot:paused', this.buildEvent('bot:paused', {}));
  }

  async resume(): Promise<void> {
    if (this.state.status !== 'paused') return;
    this.active = true;
    this.setStatus('running');
    this.log('info', 'Bot retomado');
    this.emit('bot:resumed', this.buildEvent('bot:resumed', {}));
    this.scheduleNextTick();
  }

  getState(): BotState {
    return { ...this.state, logs: [...this.state.logs] };
  }

  // ─── Hooks abstratos ──────────────────────────────────────

  protected abstract onStart(): Promise<void>;
  protected abstract onStop(): Promise<void>;
  protected abstract tick(): Promise<void>;
  protected getTickDelay(): number { return 1000; }

  // ─── Loop interno ─────────────────────────────────────────

  private scheduleNextTick(): void {
    if (!this.active) return;
    this.loopTimer = setTimeout(async () => {
      try {
        await this.tick();
        this.checkStopConditions();
      } catch (err) {
        if (err instanceof DerivApiError && err.code === 'InsufficientBalance') {
          this.log('error', 'Saldo insuficiente para abrir o contrato — bot parado.');
          this.emit('bot:insufficient_balance', this.buildEvent('bot:insufficient_balance', {
            message: err.message,
          }));
          await this.stop('insufficient_balance');
          return;
        }
        const msg = err instanceof Error ? err.message : String(err);
        this.log('error', `Erro no tick: ${msg}`);
        this.setError(msg);
      }
      if (this.active) this.scheduleNextTick();
    }, this.getTickDelay());
  }

  // ─── Verificação de condições de parada ───────────────────
  // Emite bot:goal_reached com o motivo exacto ANTES de parar,
  // para o frontend poder mostrar uma mensagem clara: meta atingida
  // (take profit), stop de perda (max loss), ou limite de trades.

  protected checkStopConditions(): void {
    const { maxTrades, maxLoss, maxProfit } = this.state.config;
    const { totalTrades, totalLoss, netPnL } = this.state.stats;

    if (maxTrades && maxTrades > 0 && totalTrades >= maxTrades) {
      this.emit('bot:goal_reached', this.buildEvent('bot:goal_reached', {
        reason: 'max_trades_reached', totalTrades, maxTrades,
      }));
      this.stop('max_trades_reached');
      return;
    }
    if (maxLoss && totalLoss >= maxLoss) {
      this.emit('bot:goal_reached', this.buildEvent('bot:goal_reached', {
        reason: 'max_loss_reached', totalLoss, maxLoss,
      }));
      this.stop('max_loss_reached');
      return;
    }
    if (maxProfit && netPnL >= maxProfit) {
      this.emit('bot:goal_reached', this.buildEvent('bot:goal_reached', {
        reason: 'max_profit_reached', netPnL, maxProfit,
      }));
      this.stop('max_profit_reached');
      return;
    }
  }

  // ─── Registo de resultado de trade ───────────────────────

  protected recordTradeResult(result: TradeResult): void {
    const s = this.state.stats;
    s.totalTrades += 1;

    if (result.won) {
      s.wins += 1;
      s.totalProfit += result.profit;
      s.currentStreak = s.currentStreak > 0 ? s.currentStreak + 1 : 1;
    } else {
      s.losses += 1;
      s.totalLoss += Math.abs(result.profit);
      s.currentStreak = s.currentStreak < 0 ? s.currentStreak - 1 : -1;
    }

    s.netPnL = s.totalProfit - s.totalLoss;
    s.winRate = s.totalTrades > 0 ? (s.wins / s.totalTrades) * 100 : 0;
    s.peakProfit = Math.max(s.peakProfit, s.netPnL);
    s.maxDrawdown = Math.max(s.maxDrawdown, s.peakProfit - s.netPnL);

    // contractType incluído para a UI mostrar a coluna "Tipo" (ex: CALL,
    // PUT, DIGITOVER), e entryTick/exitTick para a coluna "Tick Final".
    this.emit('bot:trade_closed', this.buildEvent('bot:trade_closed', {
      contractId:   result.contractId,
      profit:       result.profit,
      won:          result.won,
      stake:        result.stake,
      entryTick:    result.entryTick,
      exitTick:     result.exitTick,
      contractType: this.state.config.contractType,
    }));
    this.emit('bot:stats_updated', this.buildEvent('bot:stats_updated', { stats: { ...s } }));
  }

  // ─── Helpers ──────────────────────────────────────────────

  protected setStatus(status: BotStatus): void {
    this.state.status = status;
  }

  protected setError(message: string): void {
    this.state.status = 'error';
    this.state.lastError = message;
    this.active = false;
    this.emit('bot:error', this.buildEvent('bot:error', { error: message }));
  }

  protected log(level: LogLevel, message: string, data?: Record<string, unknown>): void {
    const entry: BotLogEntry = { timestamp: new Date(), level, message, data };
    if (this.state.logs.length >= 500) this.state.logs.shift();
    this.state.logs.push(entry);
    this.emit('bot:log', this.buildEvent('bot:log', { entry }));
  }

  private buildEvent(type: BotEventType, payload: Record<string, unknown>): BotEvent {
    return { type, botId: this.state.id, payload };
  }
}
