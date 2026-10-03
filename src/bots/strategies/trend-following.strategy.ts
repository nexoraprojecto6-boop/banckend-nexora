// ============================================================
// NEXORA FOREX — Trend Following Strategy v2
// ============================================================
// Subscreve ticks ao vivo e analisa uma janela deslizante.
// CALL se a tendência é ascendente, PUT se descendente.
//
// Correcções vs v1:
//   ✅ Orientada a TICK (skipTickLoop = true). Antes, a decisão só era
//      tomada a cada 3 s por um timer, por isso a entrada podia chegar
//      até 3 s depois do tick que confirmou a tendência.
//   ✅ Erros tratados aqui (handleUnexpectedError) e stop loss / take
//      profit verificados logo após cada resultado
//   ✅ Sem log "Aguardando ticks" a cada ciclo (spam)
//   ✅ Params parciais mantêm os defaults
// ============================================================

import { BotConfig, TrendFollowingParams, TradeDirection, TradeResult, DerivWsAdapter } from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';
import { withDefaults } from './digit-utils.js';

export class TrendFollowingStrategy extends BaseStrategy {
  private params: TrendFollowingParams;
  private pending = false;
  private tickBuffer: number[] = [];
  private bufferReadyLogged = false;
  private tickSub: { unsubscribe: () => void } | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    super(id, name, 'trend_following', config, derivWs);
    this.params = withDefaults<TrendFollowingParams>({
      ticksWindow: 10,
      trendThreshold: 0.002,
    }, config.strategyParams);
    this.skipTickLoop = true;
  }

  protected async onStart(): Promise<void> {
    const { symbol } = this.state.config;
    const { ticksWindow } = this.params;
    this.tickBuffer = [];
    this.bufferReadyLogged = false;
    this.pending = false;

    this.tickSub = this.derivWs.subscribeToTicks(symbol, (price: number) => this.onTick(price));
    this.log('info', `Trend Following — símbolo: ${symbol}, janela: ${ticksWindow} ticks`);
  }

  protected async onStop(): Promise<void> {
    this.tickSub?.unsubscribe();
    this.tickSub = null;
    this.tickBuffer = [];
    this.pending = false;
  }

  protected async tick(): Promise<void> { /* no-op: ação em onTick() */ }

  // Mudança de mercado: janela nova e re-subscrição
  protected async onSymbolChanged(): Promise<void> {
    this.tickBuffer = [];
    this.bufferReadyLogged = false;
    this.tickSub?.unsubscribe();
    this.tickSub = this.derivWs.subscribeToTicks(this.state.config.symbol, (price: number) => this.onTick(price));
  }

  // ─── Lógica orientada a tick ──────────────────────────────

  private onTick(price: number): void {
    const { ticksWindow, trendThreshold } = this.params;

    this.tickBuffer.push(price);
    if (this.tickBuffer.length > ticksWindow) this.tickBuffer.shift();

    if (!this.active || this.pending || this.coolingDown) return;
    if (this.tickBuffer.length < ticksWindow) return;

    if (!this.bufferReadyLogged) {
      this.bufferReadyLogged = true;
      this.log('info', `Janela completa (${ticksWindow} ticks) — a analisar tendência a cada tick.`);
    }

    const direction = this.detectTrend(trendThreshold);
    if (direction) void this.placeTrade(direction);
  }

  private async placeTrade(direction: TradeDirection): Promise<void> {
    this.pending = true;
    const { symbol, duration, durationUnit, currency } = this.state.config;
    const stake = this.state.stats.currentStake;

    this.log('trade', `Tendência detectada: ${direction} — stake: ${this.money(stake.toFixed(2))}`, {
      first: this.tickBuffer[0],
      last: this.tickBuffer[this.tickBuffer.length - 1],
    });

    try {
      const entryTime = new Date();
      const { contractId } = await this.derivWs.buyContract({
        symbol, contractType: direction, stake, duration, durationUnit, currency,
      });

      this.emit('bot:trade_opened', {
        type: 'bot:trade_opened',
        botId: this.state.id,
        payload: { contractId, stake, direction },
      });

      const result = await this.derivWs.waitForContract(contractId);
      const tradeResult: TradeResult = {
        contractId, profit: result.profit, stake, won: result.won,
        entryTick: result.entryTick, exitTick: result.exitTick,
        entryTime, exitTime: new Date(),
      };

      this.recordTradeResult(tradeResult);
      this.log('trade', result.won
        ? `✅ Ganhou ${this.money(result.profit.toFixed(2))}`
        : `❌ Perdeu ${this.money(Math.abs(result.profit).toFixed(2))}`);

      this.checkStopConditions();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.log('error', `[Trend] Falha ao operar: ${message}`, { stake });
      await this.handleUnexpectedError(err);
    } finally {
      this.pending = false;
    }
  }

  private detectTrend(threshold: number): TradeDirection | null {
    const first = this.tickBuffer[0];
    const last = this.tickBuffer[this.tickBuffer.length - 1];
    if (!first) return null;
    const change = (last - first) / first;
    if (change > threshold) return 'CALL';
    if (change < -threshold) return 'PUT';
    return null;
  }
}
