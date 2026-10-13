// ============================================================
// NEXORA FOREX — Even/Odd Smart Strategy (DIGITEVEN / DIGITODD, 1 tick)
// ============================================================
// - Contrato: DIGITEVEN ou DIGITODD, 1 tick, sem barrier.
// - Orientada a eventos de tick (skipTickLoop = true): entra no tick
//   seguinte ao sinal, e reentra de imediato após cada resultado.
// - Sinal: sequência (streak) de paridade nos últimos dígitos.
//     mode 'follow'  → aposta na mesma paridade da sequência
//     mode 'reverse' → aposta na paridade oposta
//     streakLength = 0 → entra em todos os ticks (usa a última paridade)
// - Martingale inteligente (recuperação):
//     stake = (perda acumulada + lucro alvo) / retorno líquido real
//     • retorno líquido vem do payout real da Deriv (getProposal, 1 vez
//       por mercado — não atrasa as entradas); fallback = payoutRatio
//     • vitória parcial reduz a dívida em vez de resetar tudo
//     • tetos: maxMartingaleSteps, maxStakeMultiplier e o que resta do
//       config.maxLoss → atingido = corta a perda e volta à stake inicial
//     • pausa curta após N perdas seguidas
// - STOP LOSS / TAKE PROFIT: config.maxLoss / config.maxProfit (BaseStrategy).
//
// AVISO: dígitos par/ímpar em índices sintéticos são independentes entre
// ticks; nenhum filtro cria vantagem estatística. O martingale só muda o
// formato do risco. Usar em conta demo.
// ============================================================

import { BotConfig, EvenOddSmartParams, TradeResult, DerivWsAdapter } from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';
import { decimalsFromPip, exitDigitOf, lastDigit, withDefaults } from './digit-utils.js';

type Parity = 'EVEN' | 'ODD';

export class EvenOddSmartStrategy extends BaseStrategy {
  private params: EvenOddSmartParams;
  private pending = false;
  private paused = false;
  private lossStreak = 0;
  private lossAccum = 0;
  private martingaleSteps = 0;
  private pipDecimals?: number;
  private lastParity?: Parity;
  private streak = 0;
  private netRatio: number;                 // lucro líquido por 1 de stake
  private tickSub: { unsubscribe: () => void } | null = null;
  private pauseTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    const params = withDefaults<EvenOddSmartParams>({
      mode: 'follow',
      streakLength: 3,
      payoutRatio: 0.95,
      recoveryProfitFactor: 0.5,
      maxMartingaleSteps: 6,
      maxStakeMultiplier: 40,
      pauseAfterLosses: 4,
      pauseSeconds: 8,
      decimalPlaces: 2,
    }, config.strategyParams);

    super(id, name, 'even_odd_smart', config, derivWs);
    this.params = params;
    this.netRatio = params.payoutRatio;
    this.skipTickLoop = true;
  }

  // ─── Ciclo de vida ────────────────────────────────────────

  protected async onStart(): Promise<void> {
    this.resetRecovery();
    this.paused = false;
    this.pending = false;
    this.lastParity = undefined;
    this.streak = 0;

    this.log(
      'info',
      `EvenOddSmart — symbol: ${this.state.config.symbol}, modo: ${this.params.mode}, ` +
      `streak: ${this.params.streakLength}, stake inicial: ${this.money(this.state.config.initialStake)}`,
    );

    this.subscribe();
    void this.refreshPayout(); // não bloqueia: entradas começam já
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
    this.lastParity = undefined;
    this.streak = 0;
    this.tickSub?.unsubscribe();
    this.subscribe();
    void this.refreshPayout();
  }

  private subscribe(): void {
    this.tickSub = this.derivWs.subscribeToTicks(
      this.state.config.symbol,
      (price: number, pipSize?: number) => this.onTick(price, pipSize),
    );
  }

  /** Lê o payout real (1 pedido, fora do caminho crítico) → retorno líquido por 1 de stake. */
  private async refreshPayout(): Promise<void> {
    const { symbol, currency, initialStake } = this.state.config;
    try {
      const { payout } = await this.derivWs.getProposal({
        symbol,
        contractType: 'DIGITEVEN',
        duration: 1,
        durationUnit: 't',
        currency,
        stake: initialStake,
      });
      const ratio = payout / initialStake - 1;
      if (Number.isFinite(ratio) && ratio > 0.3 && ratio < 1.5) {
        this.netRatio = ratio;
        this.log('info', `Payout real: retorno líquido ${(ratio * 100).toFixed(1)}% por trade.`);
      }
    } catch {
      /* mantém o payoutRatio configurado */
    }
  }

  // ─── Lógica orientada a tick ──────────────────────────────

  private get decimals(): number {
    return this.pipDecimals ?? this.params.decimalPlaces ?? 2;
  }

  private onTick(price: number, pipSize?: number): void {
    if (pipSize !== undefined && this.pipDecimals === undefined) {
      this.pipDecimals = decimalsFromPip(pipSize, this.params.decimalPlaces ?? 2);
    }

    // a sequência é atualizada em todos os ticks, mesmo durante trade/pausa
    const parity: Parity = lastDigit(price, this.decimals) % 2 === 0 ? 'EVEN' : 'ODD';
    this.streak = parity === this.lastParity ? this.streak + 1 : 1;
    this.lastParity = parity;

    this.tryTrade();
  }

  private pickContract(): 'DIGITEVEN' | 'DIGITODD' | null {
    if (!this.lastParity) return null;
    const { streakLength, mode } = this.params;
    if (streakLength > 0 && this.streak < streakLength) return null;

    const last = this.lastParity;
    const target: Parity = mode === 'follow' ? last : last === 'EVEN' ? 'ODD' : 'EVEN';
    return target === 'EVEN' ? 'DIGITEVEN' : 'DIGITODD';
  }

  private tryTrade(): void {
    if (!this.active || this.pending || this.paused || this.coolingDown) return;
    const contractType = this.pickContract();
    if (!contractType) return;
    void this.placeTrade(contractType);
  }

  private async placeTrade(contractType: 'DIGITEVEN' | 'DIGITODD'): Promise<void> {
    this.pending = true;
    const { symbol, currency } = this.state.config;
    const stake = this.state.stats.currentStake;

    this.log(
      'trade',
      `[EvenOdd] Abrindo ${contractType} (1 tick) — stake: ${this.money(stake.toFixed(2))} · nível ${this.martingaleSteps}`,
      { stake, martingaleSteps: this.martingaleSteps, lossAccum: this.lossAccum },
    );

    try {
      const entryTime = new Date();
      const { contractId } = await this.derivWs.buyContract({
        symbol,
        contractType,
        stake,
        duration: 1,
        durationUnit: 't',
        currency,
      });

      this.emit('bot:trade_opened', {
        type: 'bot:trade_opened',
        botId: this.state.id,
        payload: { contractId, stake, contractType },
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

      this.tradeContractType = contractType;
      this.recordTradeResult(tradeResult);
      this.adjustStake(result.won, result.profit);

      const nextStake = this.state.stats.currentStake.toFixed(2);
      this.log('trade', result.won
        ? `✅ Ganhou ${this.money(result.profit.toFixed(2))} — próximo stake: ${this.money(nextStake)}`
        : `❌ Perdeu ${this.money(Math.abs(result.profit).toFixed(2))} — próximo stake: ${this.money(nextStake)}`);

      this.checkStopConditions();
      if (!this.active) return;
      this.checkPauseCondition();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.log('error', `[EvenOdd] Falha ao operar: ${message}`, { stake });
      await this.handleUnexpectedError(err);
    } finally {
      this.pending = false;
      if (this.active && !this.paused) setImmediate(() => this.tryTrade());
    }
  }

  // ─── Martingale inteligente (recuperação) ─────────────────

  private resetRecovery(): void {
    this.lossStreak = 0;
    this.lossAccum = 0;
    this.martingaleSteps = 0;
    this.state.stats.currentStake = this.state.config.initialStake;
  }

  /** Teto da stake: múltiplo da inicial e, se houver maxLoss, o que ainda resta dele. */
  private stakeCap(): number {
    const initial = this.state.config.initialStake;
    let cap = initial * this.params.maxStakeMultiplier;

    const { maxLoss } = this.state.config;
    if (maxLoss && maxLoss > 0) {
      const remaining = maxLoss + this.state.stats.netPnL; // netPnL < 0 em perda
      if (remaining > 0) cap = Math.min(cap, remaining);
    }
    return Math.max(cap, initial);
  }

  private adjustStake(won: boolean, profit: number): void {
    const { recoveryProfitFactor, maxMartingaleSteps } = this.params;
    const initial = this.state.config.initialStake;

    if (won) {
      this.lossStreak = 0;
      // vitória reduz a dívida; só reseta quando está totalmente recuperada
      this.lossAccum = Math.max(0, this.lossAccum - profit);
      if (this.lossAccum <= 0) {
        this.resetRecovery();
        return;
      }
    } else {
      this.lossStreak += 1;
      this.lossAccum += Math.abs(profit);
      this.martingaleSteps += 1;
    }

    if (this.martingaleSteps > maxMartingaleSteps) {
      this.log('info', `Teto de martingale (${maxMartingaleSteps}) atingido — corte de ${this.money(this.lossAccum.toFixed(2))}.`);
      this.resetRecovery();
      return;
    }

    // stake que recupera a dívida + lucro alvo com um único acerto
    const target = this.lossAccum + initial * recoveryProfitFactor;
    const next = Math.max(initial, this.round(target / this.netRatio));

    const cap = this.stakeCap();
    if (next > cap) {
      this.log('info', `Stake calculada (${this.money(next.toFixed(2))}) acima do teto (${this.money(cap.toFixed(2))}) — corte de perda.`);
      this.resetRecovery();
      return;
    }

    this.state.stats.currentStake = next;
  }

  // ─── Pausa automática após sequência de perdas ────────────

  private checkPauseCondition(): void {
    const { pauseAfterLosses, pauseSeconds } = this.params;
    if (pauseAfterLosses <= 0 || this.lossStreak < pauseAfterLosses) return;

    this.paused = true;
    this.lossStreak = 0;
    this.log('info', `Pausa automática — ${pauseAfterLosses} perdas seguidas. Retomando em ${pauseSeconds}s.`);

    if (this.pauseTimer) clearTimeout(this.pauseTimer);
    this.pauseTimer = setTimeout(() => {
      this.paused = false;
      this.pauseTimer = null;
      this.log('info', 'Pausa automática terminada — retomando operações.');
      this.tryTrade();
    }, pauseSeconds * 1000);
  }
}
