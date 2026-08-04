// ============================================================
// NEXORA FOREX — Alfha Strategy (Digit Over/Under + Gale)
// ============================================================
// Opera nos dois lados, conforme o último dígito observado:
//   - dígito <= digitOver   -> compra DIGITOVER (digitOver)
//   - dígito >= digitUnder  -> compra DIGITUNDER (digitUnder)
//   - qualquer outro dígito -> não entra, espera o próximo tick
//
// Orientada a EVENTOS DE TICK (não ao timer fixo da BaseStrategy) —
// por isso marca `skipTickLoop = true`: o loop periódico da
// BaseStrategy não tem nada para fazer aqui e só desperdiçava um
// timer. Em compensação, esta estratégia chama
// `this.checkStopConditions()` ela própria, logo após cada trade.
//
// Parâmetros expostos ao utilizador (cartão de configuração):
//   digitOver           barreira para entrar em DIGITOVER
//   digitUnder          barreira para entrar em DIGITUNDER
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
  AlfhaParams,
  BotConfig,
  TradeResult,
  DerivWsAdapter,
} from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';

type Side = 'DIGITOVER' | 'DIGITUNDER';

// `decimalPlaces` acrescentado localmente — ver correção de
// getLastDigit() mais abaixo. Se/quando `AlfhaParams` (bot.types.ts)
// passar a declarar este campo nativamente, este alias deixa de ser
// necessário.
type AlfhaParamsResolved = AlfhaParams & { decimalPlaces?: number };

export class AlfhaStrategy extends BaseStrategy {
  private params: AlfhaParamsResolved;
  private pending = false;
  private lossCount = 0;
  private lossAccum = 0;
  private martingaleSteps = 0;
  private paused = false;
  private tickSub: { unsubscribe: () => void } | null = null;
  private pauseTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    // Nota: o `decimalPlaces: 2` no fallback abaixo cobre tanto o
    // caso de strategyParams ausente como o caso (mais comum) de
    // strategyParams existir mas não incluir decimalPlaces — o
    // spread do config.strategyParams por cima garante que qualquer
    // valor explícito do utilizador continua a prevalecer.
    const params = ({
      digitOver: 2,
      digitUnder: 7,
      gale: 2,
      lossesToStartGale: 0,
      maxMartingaleSteps: 10,
      pauseLossLimit: 20,
      pauseSeconds: 10,
      decimalPlaces: 2,
      ...(config.strategyParams ?? {}),
    }) as unknown as AlfhaParamsResolved;

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

    // BUG CORRIGIDO: esta estratégia agendava a pausa automática com
    // um `setTimeout` solto, sem guardar a referência. Se o bot fosse
    // parado a meio de uma pausa, o timer continuava vivo e, minutos
    // depois, reaparecia a "desmarcar" this.paused — o que podia
    // fazer um bot reiniciado assumir um estado de pausa errado.
    // ScalperStrategy e Over1RecoveryStrategy já guardavam este
    // timer; faltava aqui.
    if (this.pauseTimer) {
      clearTimeout(this.pauseTimer);
      this.pauseTimer = null;
    }
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

  // BUG CRÍTICO CORRIGIDO: a versão anterior usava
  // `price.toString().replace('.', '')`, que corta zeros à direita
  // (ex.: 1233.20 -> "1233.2" -> lê-se "2" como último dígito em vez
  // do "0" correto). Isto fazia a estratégia comprar DIGITOVER/UNDER
  // com base no dígito ERRADO em qualquer preço terminado em zero —
  // exactamente o tipo de "entrada ruim" reportado. ScalperStrategy e
  // Over1RecoveryStrategy já usavam a forma correta com
  // `toFixed(decimalPlaces)`; esta estratégia estava desalinhada.
  private getLastDigit(price: number): number {
    const decimalPlaces = this.params.decimalPlaces ?? 2;
    const str = price.toFixed(decimalPlaces).replace('.', '');
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

      // BUG CORRIGIDO: como esta estratégia é orientada a tick (não
      // ao loop fixo da BaseStrategy), o stop loss / take profit só
      // era verificado até 2s depois (getTickDelay), através do loop
      // "vazio" da BaseStrategy — o bot podia abrir mais um ou dois
      // contratos depois de já ter atingido o limite configurado.
      // Verificamos agora imediatamente, aqui.
      this.checkStopConditions();
      if (!this.active) return;

      this.checkPauseCondition();
    } catch (err) {
      // BUG CRÍTICO CORRIGIDO: esta estratégia não tinha NENHUM catch
      // — qualquer erro (rede, saldo insuficiente, etc.) tornava-se
      // uma unhandled promise rejection, já que placeTrade() é
      // chamado como "fire and forget" (`void this.placeTrade(...)`)
      // a partir de onTick(). Sem o catch, o erro nunca chegava a
      // handleUnexpectedError — em particular, saldo insuficiente
      // NUNCA parava este bot automaticamente.
      const message = err instanceof Error ? err.message : String(err);
      this.log('error', `[Alfha] Falha ao operar: ${message}`, { stake });
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

    this.pauseTimer = setTimeout(() => {
      this.paused = false;
      this.pauseTimer = null;
      this.log('info', 'Pausa automática terminada — retomando operações.');
    }, pauseSeconds * 1000);
  }
}
