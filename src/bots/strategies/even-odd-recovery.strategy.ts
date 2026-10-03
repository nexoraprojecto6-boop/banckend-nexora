// ============================================================
// NEXORA FOREX — Even/Odd Recovery Strategy v2
// ============================================================
// Contrato: DIGITEVEN ou DIGITODD (targetParity), 1 tick.
// Orientada a EVENTOS DE TICK (skipTickLoop = true).
// Gatilho: sequência de `streakTrigger` ticks seguidos com a paridade
// OPOSTA à escolhida (filtro de entrada; NÃO prevê o próximo dígito).
//
// Correcções vs v1:
//   ✅ A sequência de ticks é contada em TODOS os ticks. Antes, os ticks
//      que chegavam durante um trade em curso (pending) eram ignorados,
//      por isso a "sequência seguida" saía errada.
//   ✅ Payout ratio em CACHE (pré-carregado no arranque, renovado em
//      segundo plano e atualizado a cada vitória com profit/stake).
//      Antes, depois de CADA perda havia um pedido `proposal` à Deriv
//      (100–300 ms) antes de poder voltar a operar → 1–2 ticks perdidos
//      no momento em que o tempo de entrada mais importa.
//   ✅ Último dígito com o pip_size REAL do tick; exitDigit de exit_spot
//   ✅ Params parciais mantêm os defaults
//
// AVISO: ticks são independentes; o gale NÃO muda o valor esperado.
// maxMartingaleSteps e maxStakeCap são proteções obrigatórias.
// Recomendado apenas em conta demo.
// ============================================================

import { BotConfig, EvenOddRecoveryParams, TradeResult, DerivWsAdapter } from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';
import { decimalsFromPip, exitDigitOf, lastDigit, withDefaults } from './digit-utils.js';

const RATIO_REFRESH_MS = 120_000;   // renova em segundo plano
const RATIO_MAX_AGE_MS = 600_000;   // acima disto, volta a pedir antes de usar

export class EvenOddRecoveryStrategy extends BaseStrategy {
  private params: EvenOddRecoveryParams;
  private pending = false;
  private lossAccum = 0;
  private martingaleSteps = 0;
  private paused = false;
  private oppositeStreak = 0;
  private pipDecimals?: number;
  private payoutRatio: { value: number; at: number } | null = null;
  private refreshing = false;
  private tickSub: { unsubscribe: () => void } | null = null;
  private pauseTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    const params = withDefaults<EvenOddRecoveryParams>({
      targetParity: 'even',
      streakTrigger: 4,
      recoveryMarginPercent: 10,
      protectiveStakePercent: 25,
      maxMartingaleSteps: 6,
      maxStakeCap: 50,
      minStake: 0.35,
      pauseLossLimit: 20,
      pauseSeconds: 10,
      decimalPlaces: 2,
    }, config.strategyParams);

    super(id, name, 'even_odd_recovery', config, derivWs);
    this.params = params;
    this.skipTickLoop = true;
  }

  protected async onStart(): Promise<void> {
    this.state.stats.currentStake = this.state.config.initialStake;
    this.lossAccum = 0;
    this.martingaleSteps = 0;
    this.oppositeStreak = 0;
    this.paused = false;
    this.pending = false;

    this.log(
      'info',
      `Even/Odd Recovery — symbol: ${this.state.config.symbol}, alvo: ${this.getContractType()}, ` +
      `gatilho: ${this.params.streakTrigger} ticks seguidos na paridade oposta, ` +
      `stake inicial: ${this.money(this.state.config.initialStake)}, margem: +${this.params.recoveryMarginPercent}%, ` +
      `stake protetor: ${this.params.protectiveStakePercent}%, teto de stake: ${this.money(this.params.maxStakeCap)}`,
    );

    // pré-carrega o payout (não bloqueia o arranque)
    void this.refreshPayoutRatio();

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
    this.oppositeStreak = 0;
    this.payoutRatio = null;
    void this.refreshPayoutRatio();
    this.tickSub?.unsubscribe();
    this.tickSub = this.derivWs.subscribeToTicks(
      this.state.config.symbol,
      (price: number, pipSize?: number) => this.onTick(price, pipSize),
    );
  }

  // ─── Lógica orientada a tick ──────────────────────────────

  private getContractType(): 'DIGITEVEN' | 'DIGITODD' {
    return this.params.targetParity === 'odd' ? 'DIGITODD' : 'DIGITEVEN';
  }

  private get decimals(): number {
    return this.pipDecimals ?? this.params.decimalPlaces ?? 2;
  }

  private onTick(price: number, pipSize?: number): void {
    if (pipSize !== undefined && this.pipDecimals === undefined) {
      this.pipDecimals = decimalsFromPip(pipSize, this.params.decimalPlaces ?? 2);
    }
    if (!this.active) return;

    // A sequência conta TODOS os ticks (mesmo durante trade/pausa).
    const digit = lastDigit(price, this.decimals);
    const targetIsEven = this.params.targetParity !== 'odd';
    const matchesTarget = (digit % 2 === 0) === targetIsEven;

    if (matchesTarget) {
      this.oppositeStreak = 0;
      return;
    }
    this.oppositeStreak += 1;

    if (this.pending || this.paused || this.coolingDown) return;

    if (this.oppositeStreak >= this.params.streakTrigger) {
      this.oppositeStreak = 0;
      void this.placeTrade();
    }
  }

  private async placeTrade(): Promise<void> {
    this.pending = true;
    const { symbol, duration, durationUnit, currency } = this.state.config;
    const contractType = this.getContractType();
    const stake = this.state.stats.currentStake;

    this.log('trade', `[Even/Odd Recovery] Abrindo ${contractType} — stake: ${this.money(stake.toFixed(2))}`, {
      stake, lossAccum: this.lossAccum, martingaleSteps: this.martingaleSteps,
    });

    try {
      const entryTime = new Date();
      const { contractId } = await this.derivWs.buyContract({
        symbol, contractType, stake, duration, durationUnit, currency,
      });

      this.emit('bot:trade_opened', {
        type: 'bot:trade_opened',
        botId: this.state.id,
        payload: { contractId, stake, contractType },
      });

      const result = await this.derivWs.waitForContract(contractId);

      // vitória → ratio exato (já inclui margem da Deriv), sem pedido de rede
      if (result.won && stake > 0) {
        const ratio = result.profit / stake;
        if (Number.isFinite(ratio) && ratio > 0) this.payoutRatio = { value: ratio, at: Date.now() };
      }

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
      await this.adjustStake(result.won, Math.abs(result.profit));

      const nextStake = this.state.stats.currentStake.toFixed(2);
      this.log('trade', result.won
        ? `✅ Ganhou ${this.money(result.profit.toFixed(2))} — ciclo resetado, próximo stake: ${this.money(nextStake)}`
        : `❌ Perdeu ${this.money(Math.abs(result.profit).toFixed(2))} — próximo stake (recuperação): ${this.money(nextStake)}`);

      this.checkStopConditions();
      if (!this.active) return;
      this.checkPauseCondition();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.log('error', `[Even/Odd Recovery] Falha ao operar: ${message}`, { stake });
      await this.handleUnexpectedError(err);
    } finally {
      this.pending = false;
    }
  }

  // ─── Gale de recuperação ──────────────────────────────────

  private async adjustStake(won: boolean, lossValue: number): Promise<void> {
    const { maxMartingaleSteps, maxStakeCap, protectiveStakePercent, minStake } = this.params;
    const initial = this.state.config.initialStake;

    if (won) {
      this.lossAccum = 0;
      this.martingaleSteps = 0;
      this.state.stats.currentStake = initial;
      return;
    }

    this.lossAccum += lossValue;
    this.martingaleSteps += 1;

    if (this.martingaleSteps > maxMartingaleSteps) {
      this.log('info', `${this.martingaleSteps} perdas seguidas — teto atingido, cortando o ciclo e resetando o stake por segurança.`);
      this.lossAccum = 0;
      this.martingaleSteps = 0;
      this.state.stats.currentStake = initial;
      return;
    }

    if (this.martingaleSteps === 1) {
      try {
        const stake = await this.computeRecoveryStake();
        this.state.stats.currentStake = stake;
        if (stake >= maxStakeCap) {
          this.log('warn',
            `Stake de recuperação (${this.money(stake.toFixed(2))}) atingiu o teto (${this.money(maxStakeCap)}). ` +
            `A recuperação total pode ficar incompleta — intencional para proteger a conta.`);
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        this.log('error', `Falha ao calcular stake de recuperação (${message}) — resetando ao stake inicial por segurança.`);
        this.lossAccum = 0;
        this.martingaleSteps = 0;
        this.state.stats.currentStake = initial;
      }
      return;
    }

    const protectiveStake = Math.max(initial * (protectiveStakePercent / 100), minStake ?? 0);
    this.state.stats.currentStake = this.round(protectiveStake);
    this.log('info',
      `Perda seguida (nível ${this.martingaleSteps}) — protegendo a banca, stake reduzido: ` +
      `${this.money(this.state.stats.currentStake.toFixed(2))} (${protectiveStakePercent}% do stake inicial).`);
  }

  private async computeRecoveryStake(): Promise<number> {
    const { recoveryMarginPercent, maxStakeCap, minStake } = this.params;
    const initial = this.state.config.initialStake;

    const ratio = await this.getPayoutRatio();
    if (!Number.isFinite(ratio) || ratio <= 0) {
      throw new Error(`payout ratio inválido (${ratio})`);
    }

    const target = this.lossAccum + initial * (recoveryMarginPercent / 100);
    let stake = target / ratio;
    if (minStake) stake = Math.max(stake, minStake);
    stake = Math.min(stake, maxStakeCap);
    return this.round(stake);
  }

  // ─── Payout ratio (cache) ─────────────────────────────────

  /** ratio = lucro por unidade de stake (0.95 = ganha 95%). Cache → sem espera de rede. */
  private async getPayoutRatio(): Promise<number> {
    const c = this.payoutRatio;
    const now = Date.now();
    if (c && now - c.at < RATIO_MAX_AGE_MS) {
      if (now - c.at > RATIO_REFRESH_MS) void this.refreshPayoutRatio(); // renova sem bloquear
      return c.value;
    }
    return this.fetchPayoutRatio();
  }

  private async refreshPayoutRatio(): Promise<void> {
    if (this.refreshing) return;
    this.refreshing = true;
    try {
      await this.fetchPayoutRatio();
    } catch (err) {
      this.log('warn', `Não foi possível atualizar o payout em segundo plano: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      this.refreshing = false;
    }
  }

  private async fetchPayoutRatio(): Promise<number> {
    const { symbol, duration, durationUnit, currency, initialStake } = this.state.config;
    const stake = initialStake > 0 ? initialStake : 1;

    const proposal = await this.derivWs.getProposal({
      symbol,
      contractType: this.getContractType(),
      duration,
      durationUnit,
      currency,
      stake,
    });

    if (!proposal || !Number.isFinite(proposal.payout) || proposal.payout <= 0) {
      throw new Error('resposta de proposta inválida ou payout ausente');
    }
    const value = proposal.payout / stake - 1;
    this.payoutRatio = { value, at: Date.now() };
    return value;
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
