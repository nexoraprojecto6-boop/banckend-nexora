// ============================================================
// NEXORA FOREX — Martingale & Anti-Martingale Strategy
// ============================================================
// Martingale:      multiplica stake após PERDA, reseta no win
// Anti-Martingale: multiplica stake após WIN, reseta na perda
// ============================================================

import { BotConfig, MartingaleParams, TradeDirection, TradeResult } from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';
import { DerivWsAdapter } from '../bot.types.js';

export class MartingaleStrategy extends BaseStrategy {
  private params: MartingaleParams;
  private pending = false;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    const params = (config.strategyParams ?? {
      multiplier: 2,
      maxStake: 100,
      resetOnWin: false,
    }) as MartingaleParams;
    super(id, name, params.resetOnWin ? 'anti_martingale' : 'martingale', config, derivWs);
    this.params = params;
  }

  protected async onStart(): Promise<void> {
    this.state.stats.currentStake = this.state.config.initialStake;
    const mode = this.params.resetOnWin ? 'Anti-Martingale' : 'Martingale';
    this.log('info', `${mode} — stake inicial: $${this.state.config.initialStake}, multiplicador: ${this.params.multiplier}x`);
  }

  protected async onStop(): Promise<void> {
    this.pending = false;
  }

  protected getTickDelay(): number { return 1500; }

  protected async tick(): Promise<void> {
    if (this.pending) return;

    const { symbol, duration, durationUnit, currency } = this.state.config;
    const { multiplier, maxStake, resetOnWin } = this.params;

    const stake = Math.min(this.state.stats.currentStake, maxStake);
    const direction = this.chooseDirection();
    const mode = resetOnWin ? 'Anti-Martingale' : 'Martingale';

    this.log('trade', `[${mode}] Abrindo ${direction} — stake: $${stake.toFixed(2)}`, {
      stake, currentStreak: this.state.stats.currentStreak,
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
      this.adjustStake(result.won, stake, multiplier, maxStake, resetOnWin);

      const nextStake = this.state.stats.currentStake.toFixed(2);
      if (result.won) {
        this.log('trade', `✅ Ganhou $${result.profit.toFixed(2)} — próximo stake: $${nextStake}`);
      } else {
        this.log('trade', `❌ Perdeu $${Math.abs(result.profit).toFixed(2)} — próximo stake: $${nextStake}`);
      }
    } finally {
      this.pending = false;
    }
  }

  private adjustStake(won: boolean, current: number, multiplier: number, max: number, resetOnWin: boolean): void {
    const shouldMultiply = resetOnWin ? won : !won;
    this.state.stats.currentStake = shouldMultiply
      ? Math.min(current * multiplier, max)
      : this.state.config.initialStake;
  }

  private chooseDirection(): TradeDirection {
    return Math.random() > 0.5 ? 'CALL' : 'PUT';
  }
}
