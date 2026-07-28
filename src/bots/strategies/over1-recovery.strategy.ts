// ============================================================
// NEXORA FOREX — Over1 Recovery Strategy (Digit Over 1 + Gale de Recuperação)
// ============================================================
// Contrato: DIGITOVER com barreira 1 (perde se o dígito final for
// 0 ou 1; ganha se for 2–9). Duração fixa de 1 tick.
//
// ORIENTADA A EVENTOS DE TICK (não ao timer fixo da BaseStrategy),
// no mesmo formato do ScalperStrategy.
//
// DIFERENÇA-CHAVE em relação ao Scalper: em vez de multiplicar o
// stake por um fator fixo (gale clássico), este bot CONSULTA o
// payout real da Deriv antes de cada compra (via getProposal) e
// calcula o stake exato necessário para, se ganhar, recuperar toda
// a perda acumulada no ciclo + uma margem extra (recoveryMarginPercent).
//
// AVISO IMPORTANTE (leia antes de usar em conta real):
// - Dígitos em índices sintéticos são próximos de uniformes e
//   independentes entre ticks. Over 1 acerta com alta frequência
//   (~80-90%), mas o payout é proporcionalmente baixo — a Deriv
//   ajusta o preço para que o valor esperado fique perto de zero
//   (ou negativo). Não existe "acertividade" garantida.
// - Gale de recuperação NÃO muda o valor esperado do jogo. Ele só
//   muda o formato do risco: muitas vitórias pequenas e,
//   ocasionalmente, uma sequência de perdas que exige um stake
//   exponencialmente maior. maxMartingaleSteps e maxStakeCap são
//   proteções obrigatórias, não sugestões — sem elas o stake
//   calculado pode ultrapassar qualquer saldo de conta depois de
//   poucas perdas seguidas.
// - Recomendado apenas em conta demo.
//
// STOP LOSS e TAKE PROFIT do bot inteiro (não do ciclo de gale)
// continuam em config.maxLoss / config.maxProfit, tratados pela
// BaseStrategy.
// ============================================================

import {
  BotConfig,
  OverOneRecoveryParams,
  TradeResult,
  DerivWsAdapter,
} from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';

export class Over1RecoveryStrategy extends BaseStrategy {
  private params: OverOneRecoveryParams;
  private pending = false;
  private lossAccum = 0;          // perda acumulada desde o último reset/pausa
  private martingaleSteps = 0;
  private paused = false;
  private tickSub: { unsubscribe: () => void } | null = null;
  private pauseTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    const params = (config.strategyParams ?? {
      digitBarrier: 1,
      recoveryMarginPercent: 10,
      maxMartingaleSteps: 6,
      maxStakeCap: 50,
      minStake: 0.35,
      pauseLossLimit: 20,
      pauseSeconds: 10,
      decimalPlaces: 2,
    }) as unknown as OverOneRecoveryParams;

    super(id, name, 'over1_recovery', config, derivWs);
    this.params = params;
  }

  protected async onStart(): Promise<void> {
    this.state.stats.currentStake = this.state.config.initialStake;
    this.lossAccum = 0;
    this.martingaleSteps = 0;
    this.paused = false;

    this.log(
      'info',
      `Over1 Recovery — symbol: ${this.state.config.symbol}, barreira: over ${this.params.digitBarrier}, ` +
      `stake inicial: $${this.state.config.initialStake}, margem de recuperação: +${this.params.recoveryMarginPercent}%, ` +
      `teto de stake: $${this.params.maxStakeCap}`,
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

  private onTick(price: number): void {
    if (!this.active || this.pending || this.paused) return;

    const digit = this.getLastDigit(price);
    const threshold = this.params.digitBarrier + 1;

    // Mesmo gatilho do Scalper: dispara quando o último dígito
    // observado está na zona que faria o Over perder (0..digitBarrier).
    if (digit < threshold) {
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
    const { symbol, duration, durationUnit, currency, contractType } = this.state.config;
    const { digitBarrier } = this.params;
    const stake = this.state.stats.currentStake;

    this.log('trade', `[Over1 Recovery] Abrindo ${contractType} > ${digitBarrier} — stake: $${stake.toFixed(2)}`, {
      stake, lossAccum: this.lossAccum, martingaleSteps: this.martingaleSteps,
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
        barrier: String(digitBarrier),
      });

      this.emit('bot:trade_opened', {
        type: 'bot:trade_opened',
        botId: this.state.id,
        payload: { contractId, stake, barrier: digitBarrier },
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

      this.checkPauseCondition();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.log('error', `[Over1 Recovery] Falha ao operar: ${message}`, { stake });
    } finally {
      this.pending = false;
    }
  }

  // ─── Gestão de stake (gale de recuperação baseado em payout real) ─

  private async adjustStake(won: boolean, lossValue: number): Promise<void> {
    const { maxMartingaleSteps, maxStakeCap } = this.params;
    const initial = this.state.config.initialStake;

    if (won) {
      this.lossAccum = 0;
      this.martingaleSteps = 0;
      this.state.stats.currentStake = initial;
      return;
    }

    this.lossAccum += lossValue;

    if (this.martingaleSteps >= maxMartingaleSteps) {
      // Teto de níveis atingido -> corta as perdas, não tenta mais recuperar.
      this.log('info', `Teto de ${maxMartingaleSteps} níveis de gale atingido — cortando o ciclo e resetando o stake.`);
      this.lossAccum = 0;
      this.martingaleSteps = 0;
      this.state.stats.currentStake = initial;
      return;
    }

    this.martingaleSteps += 1;

    try {
      const stake = await this.computeRecoveryStake();
      this.state.stats.currentStake = stake;

      if (stake >= maxStakeCap) {
        this.log(
          'warn',
          `Stake de recuperação calculado ($${stake.toFixed(2)}) atingiu o teto ($${maxStakeCap}). ` +
          `A recuperação total pode ficar incompleta neste nível — isto é intencional para proteger a conta.`,
        );
      }
    } catch (err) {
      // Se não conseguimos consultar o payout com segurança, NÃO adivinhamos
      // um stake maior. Resetamos ao stake inicial e desistimos deste ciclo
      // de recuperação — mais seguro do que arriscar um valor errado.
      const message = err instanceof Error ? err.message : String(err);
      this.log('error', `Falha ao calcular stake de recuperação (${message}) — resetando ao stake inicial por segurança.`);
      this.lossAccum = 0;
      this.martingaleSteps = 0;
      this.state.stats.currentStake = initial;
    }
  }

  // Calcula o stake necessário para, se a próxima operação ganhar,
  // cobrir toda a perda acumulada no ciclo + a margem extra configurada
  // (recoveryMarginPercent, aplicada sobre o stake inicial).
  private async computeRecoveryStake(): Promise<number> {
    const { recoveryMarginPercent, maxStakeCap, minStake } = this.params;
    const initial = this.state.config.initialStake;

    // Usamos o stake atual como referência para pedir o payout, já que a
    // razão payout/stake dos contratos de dígitos é praticamente constante
    // (linear) para uma mesma barreira e símbolo, dentro da mesma faixa de stake.
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

  // payoutRatio = lucro por unidade de stake (ex: 0.11 = ganha 11% do stake)
  private async getPayoutRatio(stake: number): Promise<number> {
    const { symbol, duration, durationUnit, currency, contractType } = this.state.config;
    const { digitBarrier } = this.params;

    const proposal = await this.derivWs.getProposal({
      symbol,
      contractType,
      duration,
      durationUnit,
      currency,
      barrier: String(digitBarrier),
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
      this.log('info', 'Pausa automática terminada — retomando operações.');
    }, pauseSeconds * 1000);
  }
}
