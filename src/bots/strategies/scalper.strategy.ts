// ============================================================
// NEXORA FOREX — Scalper Strategy v3 (Digit Over, 1 tick + Gale)
// ============================================================
// Contrato: DIGITOVER, duração fixa de 1 tick.
// Orientada a eventos de tick (skipTickLoop = true).
// entryFilter=false → entra em todos os ticks (mais rápido).
// entryFilter=true  → só entra quando o último dígito <= predictDigit.
//
// STOP LOSS / TAKE PROFIT: config.maxLoss / config.maxProfit (BaseStrategy).
// AVISO: sem vantagem estatística; Gale aumenta o risco. Só em conta demo.
// ============================================================

import { BotConfig, ScalperParams, TradeResult, DerivWsAdapter } from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';
import { decimalsFromPip, exitDigitOf, lastDigit, withDefaults } from './digit-utils.js';

export class ScalperStrategy extends BaseStrategy {
  private params: ScalperParams;
  private pending = false;
  private lossCount = 0;
  private lossAccum = 0;
  private martingaleSteps = 0;
  private paused = false;
  private pipDecimals?: number;
  private lastPrice?: number;
  private tickSub: { unsubscribe: () => void } | null = null;
  private pauseTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    const params = withDefaults<ScalperParams>({
      predictDigit: 2,
      gale: 2,
      lossesToStartGale: 0,
      maxMartingaleSteps: 10,
      pauseLossLimit: 20,
      pauseSeconds: 10,
      decimalPlaces: 2,
      entryFilter: true,
    }, config.strategyParams);

    super(id, name, 'scalper', config, derivWs);
    this.params = params;
    this.skipTickLoop = true;
  }

  protected async onStart(): Promise<void> {
    this.state.stats.currentStake = this.state.config.initialStake;
    this.lossCount = 0;
    this.lossAccum = 0;
    this.martingaleSteps = 0;
    this.paused = false;
    this.pending = false;
    this.lastPrice = undefined;

    this.log(
      'info',
      `Scalper — symbol: ${this.state.config.symbol}, predict: over ${this.params.predictDigit}, ` +
      `stake inicial: ${this.money(this.state.config.initialStake)}, gale: ${this.params.gale}x`,
    );

    this.tickSub = this.derivWs.subscribeToTicks(
      this.state.config.symbol,
      (price: number, pipSize?: number) => this.onTick(price, pipSize),
    );
  }

  protected async onStop(): Promise<void> {
    this.pending = false;
    this.tickSub?.unsubscribe();
    this.tickSub = null;
    if (this.pauseTimer) {
      clearTimeout(this.pauseTimer);
      this.pauseTimer = null;
    }
  }

  protected async tick(): Promise<void> { /* no-op: ação em onTick() */ }

  protected async onSymbolChanged(): Promise<void> {
    this.pipDecimals = undefined;
    this.lastPrice = undefined;
    this.tickSub?.unsubscribe();
    this.tickSub = this.derivWs.subscribeToTicks(
      this.state.config.symbol,
      (price: number, pipSize?: number) => this.onTick(price, pipSize),
    );
  }

  // ─── Lógica orientada a tick ──────────────────────────────

  private get decimals(): number {
    return this.pipDecimals ?? this.params.decimalPlaces ?? 2;
  }

  private onTick(price: number, pipSize?: number): void {
    if (pipSize !== undefined && this.pipDecimals === undefined) {
      this.pipDecimals = decimalsFromPip(pipSize, this.params.decimalPlaces ?? 2);
    }
    this.lastPrice = price;
    this.tryTrade();
  }

  private tryTrade(): void {
    if (!this.active || this.pending || this.paused || this.coolingDown) return;
    if (this.lastPrice === undefined) return;

    if (
      this.params.entryFilter === false ||
      lastDigit(this.lastPrice, this.decimals) <= this.params.predictDigit
    ) {
      void this.placeTrade();
    }
  }

  private async placeTrade(): Promise<void> {
    this.pending = true;
    const { symbol, currency } = this.state.config;
    const { predictDigit } = this.params;
    const stake = this.state.stats.currentStake;

    this.log('trade', `[Scalper] Abrindo DIGITOVER > ${predictDigit} (1 tick) — stake: ${this.money(stake.toFixed(2))}`, {
      stake, lossCount: this.lossCount,
    });

    try {
      const entryTime = new Date();
      const { contractId } = await this.derivWs.buyContract({
        symbol,
        contractType: 'DIGITOVER',
        stake,
        duration: 1,
        durationUnit: 't',
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
        exitDigit: exitDigitOf(result, this.decimals),
        entryTime,
        exitTime: new Date(),
      };

      this.tradeContractType = 'DIGITOVER';
      this.recordTradeResult(tradeResult);
      this.adjustStake(result.won, Math.abs(result.profit));

      const nextStake = this.state.stats.currentStake.toFixed(2);
      this.log('trade', result.won
        ? `✅ Ganhou ${this.money(result.profit.toFixed(2))} — próximo stake: ${this.money(nextStake)}`
        : `❌ Perdeu ${this.money(Math.abs(result.profit).toFixed(2))} — próximo stake: ${this.money(nextStake)}`);

      this.checkStopConditions();
      if (!this.active) return;
      this.checkPauseCondition();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.log('error', `[Scalper] Falha ao operar: ${message}`, { stake });
      await this.handleUnexpectedError(err);
    } finally {
      this.pending = false;
      // reentrada imediata com o último tick, sem esperar outro
      if (this.active && !this.paused) setImmediate(() => this.tryTrade());
    }
  }

  // ─── Gale ─────────────────────────────────────────────────

  private adjustStake(won: boolean, lossValue: number): void {
    const { gale, lossesToStartGale, maxMartingaleSteps } = this.params;
    const initial = this.state.config.initialStake;

    if (won) {
      this.lossCount = 0;
      this.lossAccum = 0;
      this.martingaleSteps = 0;
      this.state.stats.currentStake = initial;
      return;
    }

    this.lossCount += 1;
    this.lossAccum += lossValue;

    if (this.lossCount > lossesToStartGale && this.martingaleSteps < maxMartingaleSteps) {
      this.martingaleSteps += 1;
      this.state.stats.currentStake = this.round(this.state.stats.currentStake * gale);
    } else {
      // atingiu o teto de níveis → reseta (corte de perdas)
      this.lossCount = 0;
      this.lossAccum = 0;
      this.martingaleSteps = 0;
      this.state.stats.currentStake = initial;
    }
  }

  // ─── Pausa automática temporária ──────────────────────────

  private checkPauseCondition(): void {
    const { pauseLossLimit, pauseSeconds } = this.params;
    if (pauseLossLimit <= 0 || this.lossAccum < pauseLossLimit) return;

    this.paused = true;
    this.lossAccum = 0;
    this.log('info', `Pausa automática — perda acumulada atingiu ${this.money(pauseLossLimit)}. Retomando em ${pauseSeconds}s.`);

    if (this.pauseTimer) clearTimeout(this.pauseTimer);
    this.pauseTimer = setTimeout(() => {
      this.paused = false;
      this.pauseTimer = null;
      this.log('info', 'Pausa automática terminada — retomando operações.');
    }, pauseSeconds * 1000);
  }
}
