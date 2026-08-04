// ============================================================
// NEXORA FOREX — Scalper Strategy (Digit Over + Gale)
// ============================================================
// Contrato: DIGITOVER (aposta que o último dígito do tick será
// MAIOR que `predictDigit`). Duração fixa de 1 tick.
//
// Orientada a EVENTOS DE TICK (não ao timer fixo da BaseStrategy):
// cada tick que chega via subscribeToTicks é avaliado; se o último
// dígito observado for "baixo" (< predictDigit + 1), compra.
// Por isso marca `skipTickLoop = true` — ver base.strategy.ts.
//
// Parâmetros expostos ao utilizador (cartão de configuração):
//   predictDigit        barreira do DIGITOVER (padrão: 2 -> gatilho cobre 0,1,2)
//   gale                multiplicador do stake por perda
//   lossesToStartGale   nº de perdas seguidas antes do Gale começar a multiplicar
//   maxMartingaleSteps  nº de níveis de Gale antes de resetar o stake
//   pauseLossLimit      perda acumulada (no ciclo) que pausa o bot temporariamente
//   pauseSeconds        duração da pausa automática, em segundos
//   decimalPlaces       nº de casas decimais do símbolo, usado para extrair
//                        o último dígito corretamente (padrão: 2)
//
// STOP LOSS e TAKE PROFIT NÃO ficam aqui — já são cobertos por
// config.maxLoss / config.maxProfit, tratados pela BaseStrategy.
//
// AVISO: dígitos em índices sintéticos são próximos de uniformes e
// independentes entre ticks — esta estratégia não tem vantagem
// estatística comprovada, e o uso de Gale (martingale) aumenta o
// risco a cada perda. Recomendado apenas em conta demo.
// ============================================================

import {
  BotConfig,
  ScalperParams,
  TradeResult,
  DerivWsAdapter,
} from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';

export class ScalperStrategy extends BaseStrategy {
  private params: ScalperParams;
  private pending = false;
  private lossCount = 0;
  private lossAccum = 0;          // perda acumulada desde o último reset/pausa
  private martingaleSteps = 0;
  private paused = false;
  private tickSub: { unsubscribe: () => void } | null = null;
  private pauseTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    const params = (config.strategyParams ?? {
      predictDigit: 2,
      gale: 2,
      lossesToStartGale: 0,
      maxMartingaleSteps: 10,
      pauseLossLimit: 20,
      pauseSeconds: 10,
      decimalPlaces: 2,
    }) as unknown as ScalperParams;

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

    this.log(
      'info',
      `Scalper — symbol: ${this.state.config.symbol}, predict: over ${this.params.predictDigit}, ` +
      `stake inicial: $${this.state.config.initialStake}, gale: ${this.params.gale}x`,
    );

    this.tickSub = this.derivWs.subscribeToTicks(this.state.config.symbol, (price) => {
      this.onTick(price);
    });
  }

  protected async onStop(): Promise<void> {
    this.pending = false;
    this.tickSub?.unsubscribe();
    this.tickSub = null;

    // evita que a pausa automática "acorde" o bot depois de já parado
    if (this.pauseTimer) {
      clearTimeout(this.pauseTimer);
      this.pauseTimer = null;
    }
  }

  // O loop fixo da BaseStrategy não decide nada aqui — a acção
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
    const threshold = this.params.predictDigit + 1;

    if (digit < threshold) {
      void this.placeTrade();
    }
  }

  private getLastDigit(price: number): number {
    // Usa casas decimais fixas (configuráveis) em vez de toString(),
    // pois toString() corta zeros à direita (ex: 1.50 -> "1.5")
    // e faria o último dígito ser lido errado.
    const decimalPlaces = this.params.decimalPlaces ?? 2;
    const str = price.toFixed(decimalPlaces).replace('.', '');
    return parseInt(str[str.length - 1], 10);
  }

  private async placeTrade(): Promise<void> {
    this.pending = true;
    const { symbol, duration, durationUnit, currency, contractType } = this.state.config;
    const { predictDigit } = this.params;
    const stake = this.state.stats.currentStake;

    this.log('trade', `[Scalper] Abrindo ${contractType} > ${predictDigit} — stake: $${stake.toFixed(2)}`, {
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
        // Calculado aqui (não no frontend) porque só a estratégia sabe
        // o decimalPlaces correcto do símbolo — evita duplicar essa
        // lógica do lado do cliente com um valor por omissão errado.
        exitDigit: this.getLastDigit(result.exitTick),
        entryTime,
        exitTime: new Date(),
      };

      this.recordTradeResult(tradeResult);
      this.adjustStake(result.won, Math.abs(result.profit));

      const nextStake = this.state.stats.currentStake.toFixed(2);
      if (result.won) {
        this.log('trade', `✅ Ganhou $${result.profit.toFixed(2)} — próximo stake: $${nextStake}`);
      } else {
        this.log('trade', `❌ Perdeu $${Math.abs(result.profit).toFixed(2)} — próximo stake: $${nextStake}`);
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
      this.log('error', `[Scalper] Falha ao operar: ${message}`, { stake });
      await this.handleUnexpectedError(err);
    } finally {
      this.pending = false;
    }
  }

  // ─── Gestão de stake (Gale a partir de N perdas, teto de níveis) ─

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

    const shouldGale = this.lossCount > lossesToStartGale && this.martingaleSteps < maxMartingaleSteps;

    if (shouldGale) {
      this.martingaleSteps += 1;
      this.state.stats.currentStake = parseFloat((this.state.stats.currentStake * gale).toFixed(2));
    } else {
      // atingiu o tecto de níveis -> reseta (corte de perdas, não recuperação)
      this.lossCount = 0;
      this.lossAccum = 0;
      this.martingaleSteps = 0;
      this.state.stats.currentStake = initial;
    }
  }

  // ─── Pausa automática temporária ──────────────────────────
  // Diferente do Stop Loss (config.maxLoss, tratado pela BaseStrategy
  // e que PARA o bot definitivamente): isto pausa e retoma sozinho.

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
