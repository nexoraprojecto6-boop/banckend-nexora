// ============================================================
// NEXORA FOREX — Scalping Strategy
// ============================================================
// Abre contratos rápidos consecutivos.
// Para automaticamente após N perdas consecutivas.
// ============================================================
import { BotConfig, ScalpingParams, TradeDirection, TradeResult, DerivWsAdapter } from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';

export class ScalpingStrategy extends BaseStrategy {
  private params: ScalpingParams;
  private consecutiveLosses = 0;
  private pending = false;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    super(id, name, 'scalping', config, derivWs);
    this.params = (config.strategyParams ?? {
      maxConsecutiveLosses: 3,
    }) as unknown as ScalpingParams;
  }

  protected async onStart(): Promise<void> {
    this.consecutiveLosses = 0;
    this.pending = false;
    this.log('info', `Scalping — stake: $${this.state.config.initialStake}, símbolo: ${this.state.config.symbol}`);
  }

  protected async onStop(): Promise<void> {
    this.pending = false;
  }

  protected getTickDelay(): number { return 2000; }

  protected async tick(): Promise<void> {
    if (this.pending) return;
    const { symbol, duration, durationUnit, currency } = this.state.config;
    const { maxConsecutiveLosses } = this.params;

    if (maxConsecutiveLosses > 0 && this.consecutiveLosses >= maxConsecutiveLosses) {
      this.log('warn', `${this.consecutiveLosses} perdas consecutivas — bot pausado automaticamente`);
      this.pause();
      return;
    }

    const stake = this.state.stats.currentStake;
    const direction = this.chooseDirection();
    this.log('trade', `Abrindo ${direction} — stake: $${stake.toFixed(2)}`, { symbol, direction, stake });

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

      if (result.won) {
        this.consecutiveLosses = 0;
        this.log('trade', `✅ Ganhou $${result.profit.toFixed(2)}`);
      } else {
        this.consecutiveLosses += 1;
        this.log('trade', `❌ Perdeu $${Math.abs(result.profit).toFixed(2)} — perdas consecutivas: ${this.consecutiveLosses}`);
      }
    } catch (err) {
      this.consecutiveLosses += 1;
      throw err;
    } finally {
      this.pending = false;
    }
  }

  // Substituir por sinal real (RSI, análise de ticks, etc.)
  private chooseDirection(): TradeDirection {
    return Math.random() > 0.5 ? 'CALL' : 'PUT';
  }
}
