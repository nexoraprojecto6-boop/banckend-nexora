// ============================================================
// NEXORA FOREX — Over1 Recovery Strategy (Digit Over 1 + Gale de Recuperação)
// ============================================================
// Contrato: DIGITOVER com barreira 1 (perde se o dígito final for
// 0 ou 1; ganha se for 2–9). Duração fixa de 1 tick.
//
// ORIENTADA A EVENTOS DE TICK (não ao timer fixo da BaseStrategy),
// no mesmo formato do ScalperStrategy. Marca `skipTickLoop = true`
// — ver base.strategy.ts.
//
// DIFERENÇA-CHAVE em relação ao Scalper: em vez de multiplicar o
// stake por um fator fixo (gale clássico) a cada perda, este bot:
//
//   • Na 1ª perda do ciclo — CONSULTA o payout real da Deriv (via
//     getProposal) e calcula o stake exato necessário para, se
//     ganhar, recuperar 100% do valor perdido + uma margem extra
//     (recoveryMarginPercent).
//   • Na 2ª perda SEGUIDA em diante — em vez de continuar escalando
//     o stake de recuperação (o que ficaria cada vez mais agressivo,
//     já que o payout do Over 1 é baixo), o bot DERRUBA o nível e
//     entra com um stake pequeno e protegido (protectiveStakePercent,
//     % do stake inicial). Fica nesse modo protetor até ganhar (o
//     ciclo reseta) ou até a pausa automática (pauseLossLimit) entrar.
//     Isto evita que uma sequência ruim faça o stake explodir.
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

const RATIO_REFRESH_MS = 120_000;   // renova em segundo plano
const RATIO_MAX_AGE_MS = 600_000;   // acima disto, volta a pedir antes de usar

export class Over1RecoveryStrategy extends BaseStrategy {
  private params: OverOneRecoveryParams;
  private pending = false;
  private payoutCache: { value: number; at: number } | null = null;
  private refreshing = false;
  private lossAccum = 0;          // perda acumulada desde o último reset/pausa
  private martingaleSteps = 0;
  private paused = false;
  private tickSub: { unsubscribe: () => void } | null = null;
  private pauseTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    const params = (config.strategyParams ?? {
      digitBarrier: 1,
      recoveryMarginPercent: 10,
      protectiveStakePercent: 25,
      maxMartingaleSteps: 6,
      maxStakeCap: 50,
      minStake: 0.35,
      pauseLossLimit: 20,
      pauseSeconds: 10,
      decimalPlaces: 2,
    }) as unknown as OverOneRecoveryParams;

    super(id, name, 'over1_recovery', config, derivWs);
    this.params = params;
    this.skipTickLoop = true;
  }

  protected async onStart(): Promise<void> {
    this.state.stats.currentStake = this.state.config.initialStake;
    this.lossAccum = 0;
    this.martingaleSteps = 0;
    this.paused = false;

    this.log(
      'info',
      `Over1 Recovery (TURBO STRIKE) — symbol: ${this.state.config.symbol}, barreira: over ${this.params.digitBarrier}, ` +
      `stake inicial: $${this.state.config.initialStake}, margem de recuperação: +${this.params.recoveryMarginPercent}%, ` +
      `stake protetor: ${this.params.protectiveStakePercent}% após perdas seguidas, teto de stake: $${this.params.maxStakeCap}`,
    );

    // Pré-aquece o payout: a 1ª perda já não espera pela rede (perdia 1–2 ticks de entrada).
    void this.refreshPayout();

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

      // BUG CORRIGIDO: ver explicação em base.strategy.ts / alfha —
      // sem isto, o stop loss / take profit só era verificado até 2s
      // depois, através do loop "vazio" da BaseStrategy.
      this.checkStopConditions();
      if (!this.active) return;

      this.checkPauseCondition();
    } catch (err) {
      // BUG CORRIGIDO: antes, este catch só registava a falha no log
      // e não delegava para handleUnexpectedError — ou seja, se a
      // Deriv devolvesse "saldo insuficiente", o bot continuava a
      // tentar operar indefinidamente em vez de parar sozinho.
      const message = err instanceof Error ? err.message : String(err);
      this.log('error', `[Over1 Recovery] Falha ao operar: ${message}`, { stake });
      await this.handleUnexpectedError(err);
    } finally {
      this.pending = false;
    }
  }

  // ─── Gestão de stake (gale de recuperação baseado em payout real) ─

  private async adjustStake(won: boolean, lossValue: number): Promise<void> {
    const { maxMartingaleSteps, maxStakeCap, protectiveStakePercent, minStake } = this.params;
    const initial = this.state.config.initialStake;

    if (won) {
      // CORRIGIDO: antes zerava a dívida ao primeiro ganho (mesmo com stake
      // protetor pequeno) e "esquecia" a perda. Agora abate o lucro à dívida
      // e só reseta o ciclo quando a dívida estiver recuperada.
      this.lossAccum = Math.max(0, this.lossAccum - lossValue);
      if (this.lossAccum < 0.01) {
        this.lossAccum = 0;
        this.martingaleSteps = 0;
        this.state.stats.currentStake = initial;
        return;
      }
      try {
        this.state.stats.currentStake = await this.computeRecoveryStake();
        this.log('info', `Ganhou, mas ainda faltam $${this.lossAccum.toFixed(2)} para recuperar — próximo stake: $${this.state.stats.currentStake.toFixed(2)}.`);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        this.log('error', `Falha ao calcular stake de recuperação (${message}) — resetando ao stake inicial por segurança.`);
        this.lossAccum = 0;
        this.martingaleSteps = 0;
        this.state.stats.currentStake = initial;
      }
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

  // payoutRatio = lucro por unidade de stake (ex: 0.11 = ganha 11% do stake).
  // Cache: a razão payout/stake é praticamente constante → sem espera de rede.
  private async getPayoutRatio(_stake: number): Promise<number> {
    const c = this.payoutCache;
    const now = Date.now();
    if (c && now - c.at < RATIO_MAX_AGE_MS) {
      if (now - c.at > RATIO_REFRESH_MS) void this.refreshPayout();
      return c.value;
    }
    return this.fetchPayout();
  }

  private async refreshPayout(): Promise<void> {
    if (this.refreshing) return;
    this.refreshing = true;
    try {
      await this.fetchPayout();
    } catch (err) {
      this.log('warn', `Não foi possível atualizar o payout em segundo plano: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      this.refreshing = false;
    }
  }

  private async fetchPayout(): Promise<number> {
    const { symbol, duration, durationUnit, currency, contractType, initialStake } = this.state.config;
    const stake = initialStake > 0 ? initialStake : 1;
    const proposal = await this.derivWs.getProposal({
      symbol,
      contractType,
      duration,
      durationUnit,
      currency,
      barrier: String(this.params.digitBarrier),
      stake,
    });
    if (!proposal || !Number.isFinite(proposal.payout) || proposal.payout <= 0) {
      throw new Error('resposta de proposta inválida ou payout ausente');
    }
    const value = proposal.payout / stake - 1;
    this.payoutCache = { value, at: Date.now() };
    return value;
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
