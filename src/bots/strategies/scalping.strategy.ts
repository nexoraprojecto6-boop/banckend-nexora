// ============================================================
// NEXORA FOREX — Scalping Strategy v2
// ============================================================
// Abre contratos rápidos consecutivos. Pausa após N perdas seguidas.
//
// Correcções vs v1:
//   ✅ Ao pausar por perdas consecutivas, o contador é zerado. Antes ficava
//      ≥ max, por isso ao RETOMAR o bot voltava a pausar de imediato
//      (nunca conseguia operar depois da 1ª pausa).
//   ✅ Params parciais mantêm os defaults
//   ✅ Intervalo entre trades 2000 ms → 300 ms (o loop desconta o tempo
//      do contrato: o próximo trade começa logo que o anterior fecha)
//
// ⚠️ A direção continua ALEATÓRIA (não há sinal). Só em conta demo.
// ============================================================

import { BotConfig, ScalpingParams, TradeDirection, TradeResult, DerivWsAdapter } from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';
import { withDefaults } from './digit-utils.js';

export class ScalpingStrategy extends BaseStrategy {
  private params: ScalpingParams;
  private consecutiveLosses = 0;
  private pending = false;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    super(id, name, 'scalping', config, derivWs);
    this.params = withDefaults<ScalpingParams>({ maxConsecutiveLosses: 3 }, config.strategyParams);
  }

  protected async onStart(): Promise<void> {
    this.consecutiveLosses = 0;
    this.pending = false;
    this.log('info', `Scalping — stake: ${this.money(this.state.config.initialStake)}, símbolo: ${this.state.config.symbol}`);
  }

  protected async onStop(): Promise<void> {
    this.pending = false;
  }

  protected getTickDelay(): number { return 300; }

  protected async tick(): Promise<void> {
    if (this.pending) return;
    const { symbol, duration, durationUnit, currency } = this.state.config;
    const { maxConsecutiveLosses } = this.params;

    if (maxConsecutiveLosses > 0 && this.consecutiveLosses >= maxConsecutiveLosses) {
      this.log('warn', `${this.consecutiveLosses} perdas consecutivas — bot pausado automaticamente`);
      this.consecutiveLosses = 0; // senão pausaria de novo logo após retomar
      this.pause();
      return;
    }

    const stake = this.state.stats.currentStake;
    const direction = this.chooseDirection();
    this.log('trade', `Abrindo ${direction} — stake: ${this.money(stake.toFixed(2))}`, { symbol, direction, stake });

    this.pending = true;
    // Erros de execução (rede, saldo…) NÃO mexem no contador de perdas:
    // propagam para o handleUnexpectedError da BaseStrategy.
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
      if (result.won) {
        this.consecutiveLosses = 0;
        this.log('trade', `✅ Ganhou ${this.money(result.profit.toFixed(2))}`);
      } else {
        this.consecutiveLosses += 1;
        this.log('trade', `❌ Perdeu ${this.money(Math.abs(result.profit).toFixed(2))} — perdas consecutivas: ${this.consecutiveLosses}`);
      }
    } finally {
      this.pending = false;
    }
  }

  private chooseDirection(): TradeDirection {
    return Math.random() > 0.5 ? 'CALL' : 'PUT';
  }
}
