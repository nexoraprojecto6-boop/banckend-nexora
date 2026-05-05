// ============================================================
// NEXORA FOREX — Trend Following Strategy
// ============================================================
// Subscreve ticks ao vivo. Analisa janela deslizante.
// CALL se tendência ascendente, PUT se descendente.
// ============================================================
import { BotConfig, TrendFollowingParams, TradeDirection, TradeResult, DerivWsAdapter } from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';

export class TrendFollowingStrategy extends BaseStrategy {
  private params: TrendFollowingParams;
  private pending = false;
  private tickBuffer: number[] = [];
  private tickSub: { unsubscribe: () => void } | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    super(id, name, 'trend_following', config, derivWs);
    this.params = (config.strategyParams ?? {
      ticksWindow: 10,
      trendThreshold: 0.002,
    }) as unknown as TrendFollowingParams;
  }

  protected async onStart(): Promise<void> {
    const { symbol } = this.state.config;
    const { ticksWindow } = this.params;
    this.tickBuffer = [];
    this.tickSub = this.derivWs.subscribeToTicks(symbol, (price) => {
      this.tickBuffer.push(price);
      if (this.tickBuffer.length > ticksWindow) this.tickBuffer.shift();
    });
    this.log('info', `Trend Following — símbolo: ${symbol}, janela: ${ticksWindow} ticks`);
  }

  protected async onStop(): Promise<void> {
    this.tickSub?.unsubscribe();
    this.tickSub = null;
    this.tickBuffer = [];
    this.pending = false;
  }

  protected getTickDelay(): number { return 3000; }

  protected async tick(): Promise<void> {
    if (this.pending) return;
    const { ticksWindow, trendThreshold } = this.params;
    const { symbol, duration, durationUnit, currency } = this.state.config;

    if (this.tickBuffer.length < ticksWindow) {
      this.log('info', `Aguardando ticks: ${this.tickBuffer.length}/${ticksWindow}`);
      return;
    }

    const direction = this.detectTrend(trendThreshold);
    if (!direction) {
      this.log('info', 'Sem tendência clara — aguardando');
      return;
    }

    const stake = this.state.stats.currentStake;
    this.log('trade', `Tendência detectada: ${direction} — stake: $${stake.toFixed(2)}`, {
      first: this.tickBuffer[0],
      last: this.tickBuffer[this.tickBuffer.length - 1],
    });

    this.pending = true;
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
      result.won
        ? this.log('trade', `✅ Ganhou $${result.profit.toFixed(2)}`)
        : this.log('trade', `❌ Perdeu $${Math.abs(result.profit).toFixed(2)}`);
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
