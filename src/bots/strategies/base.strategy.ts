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
import { classifyDerivError } from '@utils/deriv-errors.js';
import { formatMoney, roundToCurrency } from '@utils/currency.js';
const MAX_CONSECUTIVE_ERRORS = 3;
const SYMBOL_WATCH_MS = 60_000;

/** Resultado da verificação/troca de mercado (ver server.ts → resolveSymbol). */
export interface SymbolResolution {
  symbol: string | null;   // símbolo a usar; null = nenhum disponível agora
  changed: boolean;        // true = é diferente do atual
  reason?: string;
}
export type SymbolResolver = (
  strategy: BotStrategyType,
  config: BotConfig,
  current: string,
) => Promise<SymbolResolution>;

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
  private cooldownUntil = 0;
  private symbolResolver: SymbolResolver | null = null;
  private symbolTimer: ReturnType<typeof setInterval> | null = null;
  private switchingSymbol = false;
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
    this.startSymbolWatch();
  }

  async stop(reason = 'manual'): Promise<void> {
    if (!this.active && this.state.status === 'stopped') return;
    this.active = false;
    this.generation++;
    this.clearLoopTimer();
    this.stopSymbolWatch();
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
    this.stopSymbolWatch();
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
    this.startSymbolWatch();
  }

  // ─── Moeda da conta (nunca "USD" fixo) ────────────────────

  /** "1.50 EUR" / "0.00010000 BTC" */
  protected money(n: number | string): string {
    return formatMoney(n, this.state.config.currency);
  }

  /** Arredonda às casas decimais da moeda da conta. */
  protected round(n: number): number {
    return roundToCurrency(n, this.state.config.currency);
  }

  /** true enquanto o bot espera (mercado fechado, rate limit…). Estratégias por tick usam-no. */
  protected get coolingDown(): boolean {
    return Date.now() < this.cooldownUntil;
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

  private scheduleNextTick(elapsedMs = 0, minDelayMs = 0): void {
    if (!this.active) return;
    const gen = this.generation;
    // desconta o tempo que o tick() anterior já gastou
    const cool = Math.max(0, this.cooldownUntil - Date.now());
    const delay = Math.max(minDelayMs, cool, this.getTickDelay() - elapsedMs);

    this.loopTimer = setTimeout(async () => {
      if (!this.active || gen !== this.generation) return;
      const t0 = Date.now();
      let backoff = 0;
      try {
        await this.tick();
        this.checkStopConditions();
      } catch (err) {
        await this.handleUnexpectedError(err);
        // erros seguidos → espera crescente (1 s, 2 s, ... máx 5 s)
        backoff = this.consecutiveErrors > 0 ? Math.min(1000 * this.consecutiveErrors, 5000) : 0;
      }
      if (this.active && gen === this.generation) this.scheduleNextTick(Date.now() - t0, backoff);
    }, delay);
  }

  // ─── Tratamento de erros partilhado ───────────────────────

  protected async handleUnexpectedError(err: unknown): Promise<void> {
    const c = classifyDerivError(err);

    if (c.kind === 'insufficient_balance') {
      this.log('error', `${c.userMessage} Bot parado.`);
      this.emit('bot:insufficient_balance', this.buildEvent('bot:insufficient_balance', {
        message: c.message,
      }));
      await this.stop('insufficient_balance');
      return;
    }

    // Mercado fechado/suspenso/indisponível: tenta mudar para outro mercado
    // aberto (ver SymbolResolver) antes de esperar ou parar.
    if (c.kind === 'market_closed' || c.kind === 'symbol_unavailable') {
      if (await this.ensureSymbolAvailable(c.kind)) return; // mudou → tenta já no novo mercado
    }

    // Erros em que repetir não adianta (auth, stake fora dos limites,
    // contrato/mercado/moeda indisponível para o país): para já, com
    // mensagem clara, em vez de 3 tentativas inúteis.
    if (c.fatal) {
      this.log('error', `${c.userMessage} Bot parado.`, { code: c.code });
      this.setError(c.userMessage);
      return;
    }

    // Mercado fechado / rate limit: espera sem contar como falha.
    if (c.cooldownMs > 0) {
      this.cooldownUntil = Date.now() + c.cooldownMs;
      this.log('warn', c.userMessage, { code: c.code, waitMs: c.cooldownMs });
      return;
    }

    this.consecutiveErrors++;
    this.log('error', `Erro na operação (${this.consecutiveErrors}/${MAX_CONSECUTIVE_ERRORS}): ${c.message}`);
    if (this.consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
      this.setError(c.message);
    }
  }

  // ─── Mudança automática de mercado ────────────────────────

  setSymbolResolver(resolver: SymbolResolver | null): void {
    this.symbolResolver = resolver;
    if (resolver && this.active) this.startSymbolWatch();
    if (!resolver) this.stopSymbolWatch();
  }

  private startSymbolWatch(): void {
    if (!this.symbolResolver || this.symbolTimer) return;
    this.symbolTimer = setInterval(() => {
      if (this.active) void this.ensureSymbolAvailable('verificação periódica');
    }, SYMBOL_WATCH_MS);
    this.symbolTimer.unref?.();
  }

  private stopSymbolWatch(): void {
    if (this.symbolTimer) {
      clearInterval(this.symbolTimer);
      this.symbolTimer = null;
    }
  }

  /**
   * Verifica se o mercado atual está operável; se não, muda para outro.
   * Devolve true se mudou de mercado.
   */
  protected async ensureSymbolAvailable(trigger: string): Promise<boolean> {
    if (!this.symbolResolver || this.switchingSymbol || !this.active) return false;
    this.switchingSymbol = true;
    try {
      const res = await this.symbolResolver(this.state.strategy, this.state.config, this.state.config.symbol);
      if (res.symbol === null) {
        this.log('warn', `Sem mercado alternativo aberto (${res.reason ?? trigger}) — a aguardar.`);
        return false;
      }
      if (res.changed && res.symbol !== this.state.config.symbol) {
        const from = this.state.config.symbol;
        this.state.config = { ...this.state.config, symbol: res.symbol };
        this.log('warn', `Mercado ${from} indisponível (${res.reason ?? trigger}) — a mudar para ${res.symbol}.`, {
          from, to: res.symbol,
        });
        await this.onSymbolChanged(from, res.symbol);
        return true;
      }
      return false;
    } catch (err) {
      this.log('warn', `Não foi possível verificar o mercado: ${err instanceof Error ? err.message : String(err)}`);
      return false;
    } finally {
      this.switchingSymbol = false;
    }
  }

  /** Estratégias com subscrição própria de ticks sobrepõem isto para re-subscrever no novo mercado. */
  protected async onSymbolChanged(_from: string, _to: string): Promise<void> { /* por omissão: nada */ }

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
    this.stopSymbolWatch();
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
