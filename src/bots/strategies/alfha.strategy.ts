// ============================================================
// NEXORA FOREX — Alfha Strategy v2 (Digit Over/Under + Gale)
// ============================================================
// Opera nos dois lados, conforme o último dígito observado:
//   - dígito <= digitOver   -> compra DIGITOVER (digitOver)
//   - dígito >= digitUnder  -> compra DIGITUNDER (digitUnder)
//   - qualquer outro dígito -> não entra
//
// Orientada a EVENTOS DE TICK (skipTickLoop = true).
//
// Correcções vs v1:
//   ✅ Último dígito com o pip_size REAL do tick (decimalPlaces=2 fixo
//      lia o dígito errado em R_10 (3 casas), R_50 (4 casas), etc.)
//   ✅ exitDigit agora é enviado ao frontend (faltava), vindo de
//      exit_spot (string original da Deriv)
//   ✅ Params parciais já não perdem os defaults de forma silenciosa
//   ✅ Stake sem dízimas (0.35 × 2.2 …)
//
// STOP LOSS / TAKE PROFIT: config.maxLoss / config.maxProfit (BaseStrategy).
//
// AVISO: dígitos em índices sintéticos são independentes entre ticks;
// esta estratégia não tem vantagem estatística e o Gale aumenta o risco.
// Recomendado apenas em conta demo.
// ============================================================

import { AlfhaParams, BotConfig, TradeResult, DerivWsAdapter } from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';
import { decimalsFromPip, exitDigitOf, lastDigit, withDefaults } from './digit-utils.js';

type Side = 'DIGITOVER' | 'DIGITUNDER';
type AlfhaParamsResolved = AlfhaParams & { decimalPlaces?: number };

export class AlfhaStrategy extends BaseStrategy {
  private params: AlfhaParamsResolved;
  private pending = false;
  private lossCount = 0;
  private lossAccum = 0;
  private martingaleSteps = 0;
  private paused = false;
  private pipDecimals?: number;
  private tickSub: { unsubscribe: () => void } | null = null;
  private pauseTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    const params = withDefaults<AlfhaParamsResolved>({
      digitOver: 2,
      digitUnder: 7,
      gale: 2,
      lossesToStartGale: 0,
      maxMartingaleSteps: 10,
      pauseLossLimit: 20,
      pauseSeconds: 10,
      decimalPlaces: 2,
    }, config.strategyParams);

    super(id, name, 'alfha', config, derivWs);
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

    this.log(
      'info',
      `Alfha — symbol: ${this.state.config.symbol}, over: ${this.params.digitOver}, under: ${this.params.digitUnder}, ` +
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

  // Mudança de mercado: re-subscrever ticks e reaprender as casas decimais
  protected async onSymbolChanged(): Promise<void> {
    this.pipDecimals = undefined;
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
    if (!this.active || this.pending || this.paused || this.coolingDown) return;

    const digit = lastDigit(price, this.decimals);
    const { digitOver, digitUnder } = this.params;

    if (digit <= digitOver) {
      void this.placeTrade('DIGITOVER', digitOver);
    } else if (digit >= digitUnder) {
      void this.placeTrade('DIGITUNDER', digitUnder);
    }
  }

  private async placeTrade(side: Side, barrier: number): Promise<void> {
    this.pending = true;
    const { symbol, duration, durationUnit, currency } = this.state.config;
    const stake = this.state.stats.currentStake;

    this.log('trade', `[Alfha] Abrindo ${side} ${barrier} — stake: ${this.money(stake.toFixed(2))}`, {
      stake, lossCount: this.lossCount,
    });

    try {
      const entryTime = new Date();
      const { contractId } = await this.derivWs.buyContract({
        symbol, contractType: side, stake, duration, durationUnit, currency,
        barrier: String(barrier),
      });

      this.emit('bot:trade_opened', {
        type: 'bot:trade_opened',
        botId: this.state.id,
        payload: { contractId, stake, side, barrier },
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
      this.log('error', `[Alfha] Falha ao operar: ${message}`, { stake });
      await this.handleUnexpectedError(err);
    } finally {
      this.pending = false;
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
