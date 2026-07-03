// ============================================================
// NEXORA FOREX — Digit Over Martingale Strategy
// ============================================================
// Contrato: DIGITOVER (aposta que o último dígito do tick será
// MAIOR que `predictDigit`). Duração fixa de 1 tick.
//
// Diferença estrutural em relação à MartingaleStrategy "clássica":
// esta estratégia é orientada a EVENTOS DE TICK, não ao timer fixo
// da BaseStrategy. Cada tick que chega via subscribeToTicks:
//   1) actualiza o histórico de dígitos
//   2) verifica a condição de entrada (dígitos de referência "baixos")
//   3) se bater a condição e não houver trade pendente, compra
//
// O método tick() (chamado pelo loop da BaseStrategy) fica como
// no-op — ele existe só para satisfazer o contrato abstrato; toda a
// acção real acontece no callback de onTick.
//
// AVISO (documentar para o utilizador final no frontend): dígitos em
// índices sintéticos são próximos de uniformes e independentes entre
// ticks — esta análise de "dígitos de referência" não constitui uma
// vantagem estatística comprovada. É uma estratégia de alto risco por
// usar martingale. Recomendado apenas em conta demo.
// ============================================================

import {
  BotConfig,
  DigitOverMartingaleParams,
  TradeResult,
  DerivWsAdapter,
} from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';

export class ScalperStrategy extends BaseStrategy {
  private params: DigitOverMartingaleParams;
  private pending = false;
  private digitHistory: number[] = [];
  private lossCount = 0;
  private tickSub: { unsubscribe: () => void } | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    const params = (config.strategyParams ?? {
      predictDigit: 3,
      winStake: config.initialStake,
      martingaleStep: 2,
      stp1: 2,
      stp2: 2,
      digitThreshold: 4, // "baixo" = dígito < digitThreshold (predictDigit + 1)
    }) as unknown as DigitOverMartingaleParams;

    super(id, name, 'digit_over_martingale', config, derivWs);
    this.params = params;
  }

  protected async onStart(): Promise<void> {
    this.state.stats.currentStake = this.state.config.initialStake;
    this.digitHistory = [];
    this.lossCount = 0;

    this.log(
      'info',
      `DigitOver Martingale — symbol: ${this.state.config.symbol}, predict: ${this.params.predictDigit}, ` +
      `stake inicial: $${this.state.config.initialStake}, martingaleStep: ${this.params.martingaleStep}`,
    );

    this.tickSub = this.derivWs.subscribeToTicks(this.state.config.symbol, (price) => {
      this.onTick(price);
    });
  }

  protected async onStop(): Promise<void> {
    this.pending = false;
    this.tickSub?.unsubscribe();
    this.tickSub = null;
  }

  // O loop fixo da BaseStrategy não é usado para decisão — a acção
  // acontece em onTick(), disparada pela subscrição de ticks.
  protected async tick(): Promise<void> {
    // no-op de propósito
  }

  protected getTickDelay(): number {
    return 2000; // só controla a frequência do loop "morto" acima
  }

  // ─── Lógica orientada a tick ──────────────────────────────

  private onTick(price: number): void {
    const digit = this.getLastDigit(price);
    this.digitHistory.push(digit);
    if (this.digitHistory.length > 50) this.digitHistory.shift();

    if (!this.active || this.pending) return;

    const { stp1, stp2, digitThreshold } = this.params;
    const need = Math.max(stp1, stp2);
    if (this.digitHistory.length < need) return;

    // Equivalente ao "GET FROM_END STP1 / STP2" do DBot
    const t1 = this.digitHistory[this.digitHistory.length - stp1];
    const t2 = this.digitHistory[this.digitHistory.length - stp2];

    if (t1 < digitThreshold && t2 < digitThreshold) {
      void this.placeTrade();
    }
  }

  private getLastDigit(price: number): number {
    const str = price.toString().replace('.', '');
    return parseInt(str[str.length - 1], 10);
  }

  private async placeTrade(): Promise<void> {
    this.pending = true;
    const { symbol, duration, durationUnit, currency, contractType } = this.state.config;
    const { predictDigit } = this.params;
    const stake = this.state.stats.currentStake;

    this.log('trade', `[DigitOver] Abrindo ${contractType} > ${predictDigit} — stake: $${stake.toFixed(2)}`, {
      stake, lossCount: this.lossCount,
    });

    try {
      const entryTime = new Date();
      const { contractId } = await this.derivWs.buyContract({
        symbol,
        contractType,        // 'DIGITOVER'
        stake,
        duration,
        durationUnit,
        currency,
        barrier: String(predictDigit),
      });

      this.emit('bot:trade_opened', {
        type: 'bot:trade_opened',
        botId: this.state.id,
        payload: { contractId, stake, barrier: predictDigit },
      });

      const result = await this.derivWs.waitForContract(contractId);
      const tradeResult: TradeResult = {
        contractId,
        profit: result.profit,
        stake,
        won: result.won,
        entryTick: result.entryTick,
        exitTick: result.exitTick,
        entryTime,
        exitTime: new Date(),
      };

      this.recordTradeResult(tradeResult);
      this.adjustStake(result.won);

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

  // ─── Gestão de stake (martingale em N níveis, depois reseta) ─

  private adjustStake(won: boolean): void {
    const { martingaleStep, winStake } = this.params;
    const initial = this.state.config.initialStake;

    if (won) {
      this.lossCount = 0;
      this.state.stats.currentStake = winStake;
      return;
    }

    this.lossCount += 1;

    if (this.lossCount < martingaleStep) {
      // progressão geométrica simples: initial * 2^lossCount
      this.state.stats.currentStake = parseFloat((initial * Math.pow(2, this.lossCount)).toFixed(2));
    } else {
      // atingiu o número máximo de níveis -> reseta
      this.lossCount = 0;
      this.state.stats.currentStake = initial;
    }
  }
}
