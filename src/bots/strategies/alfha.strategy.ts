// ============================================================
// NEXORA FOREX — Alfha Strategy (Digit Over/Under + Gale)
// ============================================================
// Opera nos dois lados, conforme o último dígito observado:
//   - dígito <= digitOver   -> compra DIGITOVER (digitOver)
//   - dígito >= digitUnder  -> compra DIGITUNDER (digitUnder)
//   - qualquer outro dígito -> não entra, espera o próximo tick
//
// Orientada a EVENTOS DE TICK (não ao timer fixo da BaseStrategy).
//
// Parâmetros expostos ao utilizador (cartão de configuração) —
// apenas o que é realmente necessário ajustar:
//   digitOver           barreira para entrar em DIGITOVER
//   digitUnder          barreira para entrar em DIGITUNDER
//   gale                multiplicador do stake por perda
//   lossesToStartGale   nº de perdas seguidas antes do Gale começar a multiplicar
//   maxMartingaleSteps  nº de níveis de Gale antes de resetar o stake
//   pauseLossLimit      perda acumulada (no ciclo) que pausa o bot temporariamente
//   pauseSeconds        duração da pausa automática, em segundos
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
  AlfhaParams,
  BotConfig,
  TradeResult,
  DerivWsAdapter,
} from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';

type Side = 'DIGITOVER' | 'DIGITUNDER';

export class AlfhaStrategy extends BaseStrategy {
  private params: AlfhaParams;
  private pending = false;
  private lossCount = 0;
  private lossAccum = 0;
  private martingaleSteps = 0;
  private paused = false;
  private tickSub: { unsubscribe: () => void } | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    const params = (config.strategyParams ?? {
      digitOver: 2,
      digitUnder: 7,
      gale: 2,
      lossesToStartGale: 0,
      maxMartingaleSteps: 10,
      pauseLossLimit: 20,
      pauseSeconds: 10,
    }) as unknown as AlfhaParams;

    super(id, name, 'alfha', config, derivWs);
    this.params = params;
  }

  protected async onStart(): Promise<void> {
    this.state.stats.currentStake = this.state.config.initialStake;
    this.lossCount = 0;
    this.lossAccum = 0;
    this.martingaleSteps = 0;
    this.paused = false;

    this.log(
      'info',
      `Alfha — symbol: ${this.state.config.symbol}, over: ${this.params.digitOver}, under: ${this.params.digitUnder}, ` +
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
  }

  protected async tick(): Promise<void> {
    // no-op de propósito — a acção acontece em onTick()
  }

  protected getTickDelay(): number {
    return 2000;
  }

  // ─── Lógica orientada a tick ──────────────────────────────

  private onTick(price: number): void {
    if (!this.active || this.pending || this.paused) return;

    const digit = this.getLastDigit(price);
    const { digitOver, digitUnder } = this.params;

    if (digit <= digitOver) {
      void this.placeTrade('DIGITOVER', digitOver);
    } else if (digit >= digitUnder) {
      void this.placeTrade('DIGITUNDER', digitUnder);
    }
  }

  private getLastDigit(price: number): number {
    const str = price.toString().replace('.', '');
    return parseInt(str[str.length - 1], 10);
  }

  private async placeTrade(side: Side, barrier: number): Promise<void> {
    this.pending = true;
    const { symbol, duration, durationUnit, currency } = this.state.config;
    const stake = this.state.stats.currentStake;

    this.log('trade', `[Alfha] Abrindo ${side} ${barrier} — stake: $${stake.toFixed(2)}`, {
      stake, lossCount: this.lossCount,
    });

    try {
      const entryTime = new Date();
      const { contractId } = await this.derivWs.buyContract({
        symbol,
        contractType: side,
        stake,
        duration,
        durationUnit,
        currency,
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

      this.checkPauseCondition();
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
      this.lossCount = 0;
      this.lossAccum = 0;
      this.martingaleSteps = 0;
      this.state.stats.currentStake = initial;
    }
  }

  // ─── Pausa automática temporária ──────────────────────────

  private checkPauseCondition(): void {
    const { pauseLossLimit, pauseSeconds } = this.params;
    if (pauseLossLimit <= 0) return;
    if (this.lossAccum < pauseLossLimit) return;

    this.paused = true;
    this.lossAccum = 0;
    this.log('info', `Pausa automática — perda acumulada atingiu $${pauseLossLimit}. Retomando em ${pauseSeconds}s.`);

    setTimeout(() => {
      this.paused = false;
      this.log('info', 'Pausa automática terminada — retomando operações.');
    }, pauseSeconds * 1000);
  }
}
