// ============================================================
// NEXORA FOREX — Martingale & Anti-Martingale Strategy v2
// ============================================================
// Martingale:      multiplica stake após PERDA, reseta no win
// Anti-Martingale: multiplica stake após WIN, reseta na perda
//
// Correcções vs v1:
//   ✅ Params parciais mantêm os defaults (antes `?? {...}` só valia com
//      strategyParams ausente: multiplier/maxStake em falta → NaN)
//   ✅ Stake arredondado a 2 casas (0.35 × 2.2 gerava dízimas)
//   ✅ Intervalo entre trades 1500 ms → 300 ms. Como o loop da
//      BaseStrategy desconta o tempo do contrato, o próximo trade
//      começa logo que o anterior fecha.
//
// ⚠️ A direção (CALL/PUT) continua a ser ALEATÓRIA: não há sinal. O
//    martingale não cria vantagem; apenas altera o formato do risco.
//    Recomendado apenas em conta demo.
// ============================================================

import { BotConfig, MartingaleParams, TradeDirection, TradeResult, DerivWsAdapter } from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';
import { withDefaults } from './digit-utils.js';

export class MartingaleStrategy extends BaseStrategy {
  private params: MartingaleParams;
  private pending = false;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    const params = withDefaults<MartingaleParams>({
      multiplier: 2,
      maxStake: 100,
      resetOnWin: false,
    }, config.strategyParams);
    super(id, name, params.resetOnWin ? 'anti_martingale' : 'martingale', config, derivWs);
    this.params = params;
  }

  protected async onStart(): Promise<void> {
    this.state.stats.currentStake = this.state.config.initialStake;
    this.pending = false;
    const mode = this.params.resetOnWin ? 'Anti-Martingale' : 'Martingale';
    this.log('info', `${mode} — stake inicial: ${this.money(this.state.config.initialStake)}, multiplicador: ${this.params.multiplier}x`);
  }

  protected async onStop(): Promise<void> {
    this.pending = false;
  }

  protected getTickDelay(): number { return 300; }

  protected async tick(): Promise<void> {
    if (this.pending) return;
    const { symbol, duration, durationUnit, currency } = this.state.config;
    const { multiplier, maxStake, resetOnWin } = this.params;
    const stake = this.round(Math.min(this.state.stats.currentStake, maxStake));
    const direction = this.chooseDirection();
    const mode = resetOnWin ? 'Anti-Martingale' : 'Martingale';

    this.log('trade', `[${mode}] Abrindo ${direction} — stake: ${this.money(stake.toFixed(2))}`, {
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

      this.tradeContractType = direction;
      this.recordTradeResult(tradeResult);
      this.adjustStake(result.won, stake, multiplier, maxStake, resetOnWin);

      const nextStake = this.state.stats.currentStake.toFixed(2);
      this.log('trade', result.won
        ? `✅ Ganhou ${this.money(result.profit.toFixed(2))} — próximo stake: ${this.money(nextStake)}`
        : `❌ Perdeu ${this.money(Math.abs(result.profit).toFixed(2))} — próximo stake: ${this.money(nextStake)}`);
    } finally {
      this.pending = false;
    }
  }

  private adjustStake(won: boolean, current: number, multiplier: number, max: number, resetOnWin: boolean): void {
    const shouldMultiply = resetOnWin ? won : !won;
    this.state.stats.currentStake = shouldMultiply
      ? this.round(Math.min(current * multiplier, max))
      : this.state.config.initialStake;
  }

  private chooseDirection(): TradeDirection {
    return Math.random() > 0.5 ? 'CALL' : 'PUT';
  }
}
