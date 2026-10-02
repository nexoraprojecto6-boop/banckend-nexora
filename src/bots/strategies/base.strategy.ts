// ============================================================
// NEXORA FOREX — Base Strategy v2
// ============================================================
// Correcções vs v1:
//   ✅ Loop sem "tempo morto": o atraso do próximo ciclo desconta o
//      tempo que o tick() já demorou (antes: +1 s extra DEPOIS de cada
//      contrato terminar)
//   ✅ Geração do loop: stop()+start() rápido já não cria 2 loops
//   ✅ start(): se onStart() falhar, o bot não fica "running" fantasma
//   ✅ setError(): limpa timer e chama onStop() (subscrições de ticks
//      ficavam vivas e podiam continuar a operar um bot em erro)
//   ✅ checkStopConditions: não dispara 2× goal_reached/stop; stop()
//      sem await já não gera unhandled rejection
//   ✅ maxLoss compara a PERDA LÍQUIDA (era a soma bruta das perdas:
//      um bot com +50 de lucro líquido parava por "max loss")
//   ✅ Um erro transitório (timeout/rate limit) já não mata o bot:
//      só após 3 erros seguidos. Erros de auth/saldo param logo.
//   ✅ getState() devolve cópias de stats/config
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

const FATAL_AUTH_CODES = new Set([
  'InvalidToken', 'AuthorizationRequired', 'Unauthorized', 'PermissionDenied',
]);
const MAX_CONSECUTIVE_ERRORS = 3;

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
  protected consecutiveErrors = 0;
  private loopTimer: ReturnType<typeof setTimeout> | null = null;
  private generation = 0;

  // Estratégias orientadas a tick marcam skipTickLoop = true e chamam
  // checkStopConditions() elas próprias após cada resultado.
  protected skipTickLoop = false;

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
    this.consecutiveErrors = 0;
    this.generation++;
    this.setStatus('running');
    this.state.startedAt = new Date();
    this.log('info', `Bot "${this.state.name}" iniciado — estratégia: ${this.state.strategy}`);
    this.emit('bot:started', this.buildEvent('bot:started', {}));

    try {
      await this.onStart();
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.log('error', `Falha ao iniciar: ${msg}`);
      this.setError(msg);
      throw err;
    }

    if (!this.skipTickLoop) this.scheduleNextTick();
  }

  async stop(reason = 'manual'): Promise<void> {
    if (!this.active && this.state.status === 'stopped') return;
    this.active = false;
    this.generation++;
    this.clearLoopTimer();
    this.setStatus('stopped');
    this.state.stoppedAt = new Date();
    this.log('info', `Bot parado — motivo: ${reason}`);
    this.emit('bot:stopped', this.buildEvent('bot:stopped', { reason }));
    try {
      await this.onStop();
    } catch (err) {
      this.log('error', `Erro no onStop: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  pause(): void {
    if (this.state.status !== 'running') return;
    this.active = false;
    this.generation++;
    this.clearLoopTimer();
    this.setStatus('paused');
    this.log('info', 'Bot pausado');
    this.emit('bot:paused', this.buildEvent('bot:paused', {}));
  }

  async resume(): Promise<void> {
    if (this.state.status !== 'paused') return;
    this.active = true;
    this.generation++;
    this.setStatus('running');
    this.log('info', 'Bot retomado');
    this.emit('bot:resumed', this.buildEvent('bot:resumed', {}));
    if (!this.skipTickLoop) this.scheduleNextTick();
  }

  getState(): BotState {
    return {
      ...this.state,
      config: { ...this.state.config },
      stats: { ...this.state.stats },
      logs: [...this.state.logs],
    };
  }

  // ─── Hooks abstratos ──────────────────────────────────────

  protected abstract onStart(): Promise<void>;
  protected abstract onStop(): Promise<void>;
  protected abstract tick(): Promise<void>;
  protected getTickDelay(): number { return 1000; }

  // ─── Loop interno ─────────────────────────────────────────

  private clearLoopTimer(): void {
    if (this.loopTimer) {
      clearTimeout(this.loopTimer);
      this.loopTimer = null;
    }
  }

  private scheduleNextTick(elapsedMs = 0): void {
    if (!this.active) return;
    const gen = this.generation;
    // desconta o tempo que o tick() anterior já gastou
    const delay = Math.max(0, this.getTickDelay() - elapsedMs);

    this.loopTimer = setTimeout(async () => {
      if (!this.active || gen !== this.generation) return;
      const t0 = Date.now();
      try {
        await this.tick();
        this.checkStopConditions();
      } catch (err) {
        await this.handleUnexpectedError(err);
      }
      if (this.active && gen === this.generation) this.scheduleNextTick(Date.now() - t0);
    }, delay);
  }

  // ─── Tratamento de erros partilhado ───────────────────────

  protected async handleUnexpectedError(err: unknown): Promise<void> {
    if (err instanceof DerivApiError && err.code === 'InsufficientBalance') {
      this.log('error', 'Saldo insuficiente para abrir o contrato — bot parado.');
      this.emit('bot:insufficient_balance', this.buildEvent('bot:insufficient_balance', {
        message: err.message,
      }));
      await this.stop('insufficient_balance');
      return;
    }

    const msg = err instanceof Error ? err.message : String(err);

    if (err instanceof DerivApiError && FATAL_AUTH_CODES.has(err.code)) {
      this.log('error', `Erro de autenticação (${err.code}): ${msg} — bot parado.`);
      this.setError(`${err.code}: ${msg}`);
      return;
    }

    this.consecutiveErrors++;
    this.log('error', `Erro na operação (${this.consecutiveErrors}/${MAX_CONSECUTIVE_ERRORS}): ${msg}`);
    if (this.consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
      this.setError(msg);
    }
  }

  // ─── Condições de paragem ─────────────────────────────────

  protected checkStopConditions(): void {
    if (!this.active) return; // já parado/pausado: evita eventos duplicados

    const { maxTrades, maxLoss, maxProfit } = this.state.config;
    const { totalTrades, totalLoss, netPnL } = this.state.stats;
    const netLoss = -netPnL;

    let reason: string | null = null;
    let extra: Record<string, unknown> = {};

    if (maxTrades && maxTrades > 0 && totalTrades >= maxTrades) {
      reason = 'max_trades_reached';
      extra = { totalTrades, maxTrades };
    } else if (maxLoss && maxLoss > 0 && netLoss >= maxLoss) {
      reason = 'max_loss_reached';
      extra = { totalLoss, netLoss, maxLoss };
    } else if (maxProfit && maxProfit > 0 && netPnL >= maxProfit) {
      reason = 'max_profit_reached';
      extra = { netPnL, maxProfit };
    }

    if (!reason) return;

    this.emit('bot:goal_reached', this.buildEvent('bot:goal_reached', { reason, ...extra }));
    this.stop(reason).catch((err) => {
      this.log('error', `Falha ao parar (${reason}): ${err instanceof Error ? err.message : String(err)}`);
    });
  }

  // ─── Registo de resultado de trade ───────────────────────

  protected recordTradeResult(result: TradeResult): void {
    const s = this.state.stats;
    s.totalTrades += 1;
    this.consecutiveErrors = 0;

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

    this.emit('bot:trade_closed', this.buildEvent('bot:trade_closed', {
      contractId:   result.contractId,
      profit:       result.profit,
      won:          result.won,
      stake:        result.stake,
      entryTick:    result.entryTick,
      exitTick:     result.exitTick,
      exitDigit:    result.exitDigit,
      contractType: this.state.config.contractType,
    }));
    this.emit('bot:stats_updated', this.buildEvent('bot:stats_updated', { stats: { ...s } }));
  }

  // ─── Helpers ──────────────────────────────────────────────

  protected setStatus(status: BotStatus): void {
    this.state.status = status;
  }

  protected setError(message: string): void {
    this.active = false;
    this.generation++;
    this.clearLoopTimer();
    this.state.status = 'error';
    this.state.lastError = message;
    this.emit('bot:error', this.buildEvent('bot:error', { error: message }));
    // libertar subscrições de ticks/contratos
    this.onStop().catch((err) => {
      this.log('error', `Erro no onStop (após erro): ${err instanceof Error ? err.message : String(err)}`);
    });
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
