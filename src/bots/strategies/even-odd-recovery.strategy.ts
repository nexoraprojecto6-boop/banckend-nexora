// ============================================================
// NEXORA FOREX — Even/Odd Recovery Strategy (Digit Even/Odd + Gale de Recuperação)
// ============================================================
// Contrato: DIGITEVEN ou DIGITODD (configurável via `targetParity`).
// Duração fixa de 1 tick. Ganha se o último dígito tiver a paridade
// escolhida (par ou ímpar); perde se tiver a paridade oposta.
//
// ORIENTADA A EVENTOS DE TICK (não ao timer fixo da BaseStrategy),
// no mesmo formato do Over1RecoveryStrategy. Marca `skipTickLoop = true`
// — ver base.strategy.ts.
//
// GATILHO DE ENTRADA — DIFERENÇA EM RELAÇÃO AO OVER1:
// No Over/Under existe uma "zona de dígito" (0 ou 1, por exemplo) que
// funciona como filtro natural de entrada. No Even/Odd isso não existe
// — a paridade de um dígito não tem "zona de risco". Para manter uma
// lógica de entrada equivalente, o bot conta uma SEQUÊNCIA de ticks
// seguidos com a paridade OPOSTA à escolhida (streakTrigger) e só
// entra depois de ver essa sequência — a ideia clássica de "esperar a
// reversão".
//
// AVISO IMPORTANTE (leia antes de usar em conta real):
// - Cada tick em índices sintéticos é estatisticamente INDEPENDENTE
//   do anterior. Uma sequência de 5 dígitos ímpares seguidos NÃO
//   aumenta a probabilidade do próximo ser par — isso é a "falácia do
//   apostador". O streakTrigger é apenas um filtro de entrada, não
//   uma previsão real, e não muda o valor esperado do jogo.
// - Even/Odd paga próximo de 1:1 (payout ~95-97% do stake, antes da
//   margem da casa) — por isso o gale de recuperação aqui tende a
//   crescer mais devagar do que no Over 1, mas ainda assim precisa
//   das mesmas proteções.
// - Gale de recuperação NÃO muda o valor esperado do jogo. Ele só
//   muda o formato do risco: muitas vitórias pequenas e,
//   ocasionalmente, uma sequência de perdas que exige um stake maior.
//   maxMartingaleSteps e maxStakeCap são proteções obrigatórias, não
//   sugestões — sem elas o stake calculado pode ultrapassar qualquer
//   saldo de conta depois de poucas perdas seguidas.
// - Recomendado apenas em conta demo.
//
// STOP LOSS e TAKE PROFIT do bot inteiro (não do ciclo de gale)
// continuam em config.maxLoss / config.maxProfit, tratados pela
// BaseStrategy.
//
// NOTA (reset do oppositeStreak): o contador de ticks seguidos com a
// paridade oposta é resetado sempre que o bot sai de uma pausa
// automática ou termina um trade (placeTrade). Sem isto, um streak
// parcial acumulado antes de uma pausa/trade "sobrevivia" à
// interrupção e podia disparar uma entrada no 1º tick seguinte, sem
// esperar a sequência completa de reversão — inconsistente com a
// lógica documentada acima.
// ============================================================

import {
  BotConfig,
  EvenOddRecoveryParams,
  TradeResult,
  DerivWsAdapter,
} from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';

export class EvenOddRecoveryStrategy extends BaseStrategy {
  private params: EvenOddRecoveryParams;
  private pending = false;
  private lossAccum = 0;          // perda acumulada desde o último reset/pausa
  private martingaleSteps = 0;
  private paused = false;
  private oppositeStreak = 0;     // ticks seguidos com a paridade oposta à escolhida
  private tickSub: { unsubscribe: () => void } | null = null;
  private pauseTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    const params = (config.strategyParams ?? {
      targetParity: 'even',       // 'even' -> DIGITEVEN | 'odd' -> DIGITODD
      streakTrigger: 4,           // entra após N ticks seguidos com a paridade oposta
      recoveryMarginPercent: 10,
      protectiveStakePercent: 25,
      maxMartingaleSteps: 6,
      maxStakeCap: 50,
      minStake: 0.35,
      pauseLossLimit: 20,
      pauseSeconds: 10,
      decimalPlaces: 2,
    }) as unknown as EvenOddRecoveryParams;

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

    const contractType = this.getContractType();
    this.log(
      'info',
      `Even/Odd Recovery — symbol: ${this.state.config.symbol}, alvo: ${contractType}, ` +
      `gatilho: ${this.params.streakTrigger} ticks seguidos na paridade oposta, ` +
      `stake inicial: $${this.state.config.initialStake}, margem de recuperação: +${this.params.recoveryMarginPercent}%, ` +
      `stake protetor: ${this.params.protectiveStakePercent}% após perdas seguidas, teto de stake: $${this.params.maxStakeCap}`,
    );

    this.tickSub = this.derivWs.subscribeToTicks(this.state.config.symbol, (price) => {
      this.onTick(price);
    });
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

  // O loop fixo da BaseStrategy não decide nada aqui — a ação
  // acontece em onTick(), disparada pela subscrição de ticks.
  protected async tick(): Promise<void> {
    // no-op de propósito
  }

  protected getTickDelay(): number {
    return 2000;
  }

  // ─── Lógica orientada a tick ──────────────────────────────

  private getContractType(): 'DIGITEVEN' | 'DIGITODD' {
    return this.params.targetParity === 'odd' ? 'DIGITODD' : 'DIGITEVEN';
  }

  private isEven(digit: number): boolean {
    return digit % 2 === 0;
  }

  private onTick(price: number): void {
    if (!this.active || this.pending || this.paused) return;

    const digit = this.getLastDigit(price);
    const targetIsEven = this.params.targetParity !== 'odd';
    const digitMatchesTarget = this.isEven(digit) === targetIsEven;

    if (digitMatchesTarget) {
      // Este tick teria ganho — não conta para a sequência de reversão.
      this.oppositeStreak = 0;
      return;
    }

    this.oppositeStreak += 1;

    if (this.oppositeStreak >= this.params.streakTrigger) {
      this.oppositeStreak = 0; // reseta a contagem ao disparar a entrada
      void this.placeTrade();
    }
  }

  private getLastDigit(price: number): number {
    const decimalPlaces = this.params.decimalPlaces ?? 2;
    const str = price.toFixed(decimalPlaces).replace('.', '');
    return parseInt(str[str.length - 1], 10);
  }

  private async placeTrade(): Promise<void> {
    this.pending = true;
    const { symbol, duration, durationUnit, currency } = this.state.config;
    const contractType = this.getContractType();
    const stake = this.state.stats.currentStake;

    this.log('trade', `[Even/Odd Recovery] Abrindo ${contractType} — stake: $${stake.toFixed(2)}`, {
      stake, lossAccum: this.lossAccum, martingaleSteps: this.martingaleSteps,
    });

    try {
      const entryTime = new Date();
      const { contractId } = await this.derivWs.buyContract({
        symbol,
        contractType,        // 'DIGITEVEN' ou 'DIGITODD' — sem barreira
        stake,
        duration,
        durationUnit,
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
        exitDigit: this.getLastDigit(result.exitTick),
        entryTime,
        exitTime: new Date(),
      };

      this.recordTradeResult(tradeResult);
      await this.adjustStake(result.won, Math.abs(result.profit));

      const nextStake = this.state.stats.currentStake.toFixed(2);
      if (result.won) {
        this.log('trade', `✅ Ganhou $${result.profit.toFixed(2)} — ciclo resetado, próximo stake: $${nextStake}`);
      } else {
        this.log('trade', `❌ Perdeu $${Math.abs(result.profit).toFixed(2)} — próximo stake (recuperação): $${nextStake}`);
      }

      // Mesmo cuidado do Over1 Recovery: verifica stop loss / take
      // profit logo após o resultado, sem esperar pelo loop de 2s.
      this.checkStopConditions();
      if (!this.active) return;

      this.checkPauseCondition();
    } catch (err) {
      // Mesmo cuidado do Over1 Recovery: qualquer erro (ex: saldo
      // insuficiente) é delegado a handleUnexpectedError, para o bot
      // parar sozinho em vez de continuar tentando operar.
      const message = err instanceof Error ? err.message : String(err);
      this.log('error', `[Even/Odd Recovery] Falha ao operar: ${message}`, { stake });
      await this.handleUnexpectedError(err);
    } finally {
      this.pending = false;
      // Reset do streak ao terminar um trade — evita aproveitar um
      // streak parcial acumulado antes/depois deste ciclo (ver nota
      // no cabeçalho do ficheiro).
      this.oppositeStreak = 0;
    }
  }

  // ─── Gestão de stake (gale de recuperação baseado em payout real) ─

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

    // Camada extra de segurança: mesmo em modo protetor (stake pequeno),
    // uma sequência longa demais de perdas seguidas corta o ciclo e
    // reseta — independente do pauseLossLimit, que olha para o valor
    // acumulado, não para a contagem de perdas seguidas.
    if (this.martingaleSteps > maxMartingaleSteps) {
      this.log('info', `${this.martingaleSteps} perdas seguidas — teto atingido, cortando o ciclo e resetando o stake por segurança.`);
      this.lossAccum = 0;
      this.martingaleSteps = 0;
      this.state.stats.currentStake = initial;
      return;
    }

    if (this.martingaleSteps === 1) {
      // 1ª perda do ciclo: tenta recuperar 100% do valor perdido + margem.
      try {
        const stake = await this.computeRecoveryStake();
        this.state.stats.currentStake = stake;

        if (stake >= maxStakeCap) {
          this.log(
            'warn',
            `Stake de recuperação calculado ($${stake.toFixed(2)}) atingiu o teto ($${maxStakeCap}). ` +
            `A recuperação total pode ficar incompleta — isto é intencional para proteger a conta.`,
          );
        }
      } catch (err) {
        // Se não conseguimos consultar o payout com segurança, NÃO
        // adivinhamos um stake maior. Resetamos ao stake inicial —
        // mais seguro do que arriscar um valor errado.
        const message = err instanceof Error ? err.message : String(err);
        this.log('error', `Falha ao calcular stake de recuperação (${message}) — resetando ao stake inicial por segurança.`);
        this.lossAccum = 0;
        this.martingaleSteps = 0;
        this.state.stats.currentStake = initial;
      }
      return;
    }

    // 2ª perda SEGUIDA em diante: em vez de continuar escalando (o que
    // ficaria cada vez mais agressivo), derruba o nível — entra com um
    // stake pequeno e protegido até ganhar (reseta o ciclo) ou até a
    // pausa automática entrar.
    const protectiveStake = Math.max(
      initial * (protectiveStakePercent / 100),
      minStake ?? 0,
    );
    this.state.stats.currentStake = parseFloat(protectiveStake.toFixed(2));
    this.log(
      'info',
      `Perda seguida (nível ${this.martingaleSteps}) — protegendo a banca, entrando com stake reduzido: ` +
      `$${this.state.stats.currentStake.toFixed(2)} (${protectiveStakePercent}% do stake inicial).`,
    );
  }

  // Calcula o stake necessário para, se a próxima operação ganhar,
  // cobrir toda a perda acumulada no ciclo + a margem extra configurada
  // (recoveryMarginPercent, aplicada sobre o stake inicial).
  private async computeRecoveryStake(): Promise<number> {
    const { recoveryMarginPercent, maxStakeCap, minStake } = this.params;
    const initial = this.state.config.initialStake;

    // Usamos o stake atual como referência para pedir o payout, já que a
    // razão payout/stake dos contratos Even/Odd é praticamente constante
    // (linear) para o mesmo símbolo, dentro da mesma faixa de stake.
    const referenceStake = this.state.stats.currentStake > 0 ? this.state.stats.currentStake : initial;
    const payoutRatio = await this.getPayoutRatio(referenceStake);

    if (!Number.isFinite(payoutRatio) || payoutRatio <= 0) {
      throw new Error(`payout ratio inválido retornado pela Deriv (${payoutRatio})`);
    }

    const target = this.lossAccum + initial * (recoveryMarginPercent / 100);
    let stake = target / payoutRatio;

    if (minStake) stake = Math.max(stake, minStake);
    stake = Math.min(stake, maxStakeCap); // teto absoluto, nunca ultrapassado

    return parseFloat(stake.toFixed(2));
  }

  // payoutRatio = lucro por unidade de stake (ex: 0.90 = ganha 90% do stake)
  private async getPayoutRatio(stake: number): Promise<number> {
    const { symbol, duration, durationUnit, currency } = this.state.config;
    const contractType = this.getContractType();

    const proposal = await this.derivWs.getProposal({
      symbol,
      contractType,
      duration,
      durationUnit,
      currency,
      stake,
    });

    if (!proposal || !Number.isFinite(proposal.payout) || proposal.payout <= 0) {
      throw new Error('resposta de proposta inválida ou payout ausente');
    }

    return proposal.payout / stake - 1;
  }

  // ─── Pausa automática temporária ──────────────────────────

  private checkPauseCondition(): void {
    const { pauseLossLimit, pauseSeconds } = this.params;
    if (pauseLossLimit <= 0) return;
    if (this.lossAccum < pauseLossLimit) return;

    this.paused = true;
    this.lossAccum = 0;
    this.log('info', `Pausa automática — perda acumulada atingiu $${pauseLossLimit}. Retomando em ${pauseSeconds}s.`);

    this.pauseTimer = setTimeout(() => {
      this.paused = false;
      this.pauseTimer = null;
      // Reset do streak ao sair da pausa — ver nota no cabeçalho do
      // ficheiro. Garante que a sequência de reversão é sempre
      // contada do zero após qualquer interrupção.
      this.oppositeStreak = 0;
      this.log('info', 'Pausa automática terminada — retomando operações.');
    }, pauseSeconds * 1000);
  }
}
