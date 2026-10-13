// ============================================================
// NEXORA FOREX — Even/Odd Smart Strategy (DIGITEVEN / DIGITODD, 1 tick)
// ============================================================
// - Contrato: DIGITEVEN ou DIGITODD, duração fixa de 1 tick (sem barrier).
// - Orientada a eventos de tick (skipTickLoop = true) → entra de imediato.
// - Sinal: sequência (streak) de paridade nos últimos dígitos.
//     mode 'follow'  → aposta na mesma paridade da sequência
//     mode 'reverse' → aposta na paridade oposta
//     streakLength = 0 → entra em todos os ticks (usa a última paridade)
// - Martingale inteligente (recuperação):
//     próxima stake = (perda acumulada + lucro alvo) / payoutRatio
//     • vitória parcial reduz a dívida em vez de resetar tudo
//     • tetos: maxMartingaleSteps, maxStakeMultiplier, maxStakePctBalance
//     • atingiu teto → corta a perda e volta à stake inicial
//     • pausa curta após sequência de perdas (cooldown)
// - STOP LOSS / TAKE PROFIT: config.maxLoss / config.maxProfit (BaseStrategy).
//
// AVISO: dígitos par/ímpar nos índices sintéticos são aleatórios; nenhum
// filtro cria vantagem estatística. O martingale só altera a distribuição
// dos resultados (muitos ganhos pequenos, raras perdas grandes). Teste em demo.
// ============================================================

import { BotConfig, TradeResult, DerivWsAdapter } from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';
import { decimalsFromPip, exitDigitOf, lastDigit, withDefaults } from './digit-utils.js';

export interface EvenOddParams {
  /** 'follow' = segue a sequência; 'reverse' = aposta contra */
  mode: 'follow' | 'reverse';
  /** nº de ticks seguidos com a mesma paridade para disparar (0 = todo tick) */
  streakLength: number;
  /** retorno líquido por 1 de stake (Deriv even/odd ≈ 0.95) */
  payoutRatio: number;
  /** lucro alvo extra ao recuperar, como fração da stake inicial */
  recoveryProfitFactor: number;
  /** máximo de níveis de martingale */
  maxMartingaleSteps: number;
  /** teto da stake = stake inicial × este valor */
  maxStakeMultiplier: number;
  /** teto da stake em % do saldo (0 = desligado) */
  maxStakePctBalance: number;
  /** perdas seguidas que disparam a pausa (0 = desligado) */
  pauseAfterLosses: number;
  pauseSeconds: number;
  decimalPlaces: number;
}

type Parity = 'EVEN' | 'ODD';

export class EvenOddSmartStrategy extends BaseStrategy {
  private params: EvenOddParams;
  private pending = false;
  private paused = false;
  private lossStreak = 0;
  private lossAccum = 0;
  private martingaleSteps = 0;
  private pipDecimals?: number;
  private lastParity?: Parity;
  private streak = 0;
  private tickSub: { unsubscribe: () => void } | null = null;
  private pauseTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    const params = withDefaults<EvenOddParams>({
      mode: 'follow',
      streakLength: 3,
      payoutRatio: 0.95,
      recoveryProfitFactor: 0.5,
      maxMartingaleSteps: 6,
      maxStakeMultiplier: 40,
      maxStakePctBalance: 5,
      pauseAfterLosses: 4,
      pauseSeconds: 8,
      decimalPlaces: 2,
    }, config.strategyParams);

    // ATENÇÃO: adicionar 'even-odd-smart' à união de tipos de estratégia em bot.types.ts
    super(id, name, 'even-odd-smart' as any, config, derivWs);
    this.params = params;
    this.skipTickLoop = true;
  }

  // ─── Ciclo de vida ────────────────────────────────────────

  protected async onStart(): Promise<void> {
    this.state.stats.currentStake = this.state.config.initialStake;
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
  }

  private subscribe(): void {
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

    // Atualiza a sequência de paridade (mesmo durante trade/pausa, para o sinal ficar fresco)
    const parity: Parity = lastDigit(price, this.decimals) % 2 === 0 ? 'EVEN' : 'ODD';
    this.streak = parity === this.lastParity ? this.streak + 1 : 1;
    this.lastParity = parity;

    this.tryTrade();
  }

  private pickContract(): 'DIGITEVEN' | 'DIGITODD' | null {
    if (!this.lastParity) return null;
    const { streakLength, mode } = this.params;

    if (streakLength > 0 && this.streak < streakLength) return null;

    const follow = this.lastParity;
    const target: Parity = mode === 'follow' ? follow : follow === 'EVEN' ? 'ODD' : 'EVEN';
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
        // DIGITEVEN/DIGITODD não usam barrier
      } as any);

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
      // reentrada imediata com o estado de tick mais recente
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

  private stakeCap(): number {
    const initial = this.state.config.initialStake;
    const { maxStakeMultiplier, maxStakePctBalance } = this.params;
    let cap = initial * maxStakeMultiplier;

    const balance = (this.state.stats as any).balance ?? (this.state as any).balance;
    if (maxStakePctBalance > 0 && typeof balance === 'number' && balance > 0) {
      cap = Math.min(cap, (balance * maxStakePctBalance) / 100);
    }
    return Math.max(cap, initial);
  }

  private adjustStake(won: boolean, profit: number): void {
    const { payoutRatio, recoveryProfitFactor, maxMartingaleSteps } = this.params;
    const initial = this.state.config.initialStake;

    if (won) {
      this.lossStreak = 0;
      // vitória reduz a dívida; só reseta quando ela é totalmente recuperada
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

    // Teto de níveis atingido → corta a perda e recomeça
    if (this.martingaleSteps > maxMartingaleSteps) {
      this.log('info', `Teto de martingale (${maxMartingaleSteps}) atingido — corte de perda de ${this.money(this.lossAccum.toFixed(2))}.`);
      this.resetRecovery();
      return;
    }

    // stake necessária para recuperar a dívida + lucro alvo num único acerto
    const target = this.lossAccum + initial * recoveryProfitFactor;
    let next = this.round(target / payoutRatio);
    next = Math.max(initial, next);

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
