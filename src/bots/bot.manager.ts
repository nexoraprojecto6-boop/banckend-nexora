// ============================================================
// NEXORA FOREX — Bot Manager v2
// ============================================================
// Correcções vs v1:
//   ✅ deleteBot() para bots running E paused (e agora é async,
//      aguarda o stop e remove os listeners → sem leak de memória)
//   ✅ stopAll() também pára bots em pausa
//   ✅ stopAll()/destroy() usam allSettled: um bot que falha ao parar
//      não impede os restantes
//   ✅ even_odd_recovery (bot LITECOIN) NÃO estava registado no
//      buildStrategy → "Estratégia desconhecida". Se a classe tiver outro
//      nome, ajuste o import.
//   ✅ even_odd_smart (bot BITCOIN) registado
// ============================================================

import { EventEmitter } from 'events';
import { v4 as uuidv4 } from 'uuid';
import {
  BotConfig,
  BotEvent,
  BotState,
  BotStrategyType,
  BotSummary,
  CreateBotRequest,
  DerivWsAdapter,
} from './bot.types.js';
import { BaseStrategy, SymbolResolver } from './strategies/base.strategy.js';
import { ScalpingStrategy } from './strategies/scalping.strategy.js';
import { MartingaleStrategy } from './strategies/martingale.strategy.js';
import { TrendFollowingStrategy } from './strategies/trend-following.strategy.js';
import { ScalperStrategy } from './strategies/scalper.strategy.js';
import { AlfhaStrategy } from './strategies/alfha.strategy.js';
import { Over1RecoveryStrategy } from './strategies/over1-recovery.strategy.js';
import { EvenOddRecoveryStrategy } from './strategies/even-odd-recovery.strategy.js';
import { EvenOddSmartStrategy } from './strategies/even-odd-smart.strategy.js';
import logger from '@utils/logger.js';

const BOT_EVENTS = [
  'bot:started',
  'bot:stopped',
  'bot:paused',
  'bot:resumed',
  'bot:error',
  'bot:trade_opened',
  'bot:trade_closed',
  'bot:stats_updated',
  'bot:log',
  'bot:goal_reached',
  'bot:insufficient_balance',
] as const;

export class BotManager extends EventEmitter {
  private bots = new Map<string, BaseStrategy>();
  private derivWs: DerivWsAdapter;
  private symbolResolver: SymbolResolver | null = null;

  constructor(derivWs: DerivWsAdapter) {
    super();
    this.derivWs = derivWs;
  }

  // ─── CRUD ─────────────────────────────────────────────────

  createBot(req: CreateBotRequest): BotState {
    const id = uuidv4();
    const bot = this.buildStrategy(id, req.name, req.strategy, req.config);

    BOT_EVENTS.forEach((event) => {
      bot.on(event, (botEvent: BotEvent) => this.emit('bot_event', botEvent));
    });

    if (this.symbolResolver) bot.setSymbolResolver(this.symbolResolver);
    this.bots.set(id, bot);
    logger.info(`[BotManager] Bot criado — id: ${id}, estratégia: ${req.strategy}, nome: ${req.name}`);
    return bot.getState();
  }

  async startBot(botId: string): Promise<void> {
    await this.getOrThrow(botId).start();
  }

  async stopBot(botId: string): Promise<void> {
    await this.getOrThrow(botId).stop('manual');
  }

  pauseBot(botId: string): void {
    this.getOrThrow(botId).pause();
  }

  async resumeBot(botId: string): Promise<void> {
    await this.getOrThrow(botId).resume();
  }

  async deleteBot(botId: string): Promise<void> {
    const bot = this.getOrThrow(botId);
    const status = bot.getState().status;
    if (status === 'running' || status === 'paused') {
      try {
        await bot.stop('deleted');
      } catch (err) {
        logger.error('[BotManager] Falha ao parar bot antes de remover', { botId, error: err });
      }
    }
    bot.removeAllListeners();
    this.bots.delete(botId);
    logger.info(`[BotManager] Bot removido — id: ${botId}`);
  }

  async stopAll(): Promise<void> {
    const targets = Array.from(this.bots.values()).filter((b) => {
      const s = b.getState().status;
      return s === 'running' || s === 'paused';
    });
    const results = await Promise.allSettled(targets.map((b) => b.stop('bulk_stop')));
    results.forEach((r) => {
      if (r.status === 'rejected') logger.error('[BotManager] Falha em stopAll', { error: r.reason });
    });
  }

  // ─── Queries ──────────────────────────────────────────────

  getBotState(botId: string): BotState {
    return this.getOrThrow(botId).getState();
  }

  listBots(): BotSummary[] {
    return Array.from(this.bots.values()).map((b) => {
      const s = b.getState();
      return { id: s.id, name: s.name, strategy: s.strategy, status: s.status, stats: s.stats, startedAt: s.startedAt, stoppedAt: s.stoppedAt };
    });
  }

  getBotLogs(botId: string, limit = 100): BotState['logs'] {
    return this.getOrThrow(botId).getState().logs.slice(-limit);
  }

  /** Ativa a mudança automática de mercado (mercado fechado/indisponível → outro aberto). */
  setSymbolResolver(resolver: SymbolResolver | null): void {
    this.symbolResolver = resolver;
    this.bots.forEach((b) => b.setSymbolResolver(resolver));
  }

  // ─── Troca de conta (só afecta bots criados a seguir) ─────

  updateDerivWs(derivWs: DerivWsAdapter): void {
    this.derivWs = derivWs;
    logger.info('[BotManager] derivWs atualizado (troca de conta)');
  }

  // ─── Cleanup ──────────────────────────────────────────────

  async destroy(): Promise<void> {
    await this.stopAll();
    this.bots.forEach((b) => b.removeAllListeners());
    this.bots.clear();
    this.removeAllListeners();
  }

  // ─── Factory ──────────────────────────────────────────────

  private buildStrategy(
    id: string,
    name: string,
    strategy: BotStrategyType,
    config: BotConfig,
  ): BaseStrategy {
    switch (strategy) {
      case 'scalping':
        return new ScalpingStrategy(id, name, config, this.derivWs);
      case 'martingale':
      case 'anti_martingale':
        return new MartingaleStrategy(id, name, config, this.derivWs);
      case 'trend_following':
        return new TrendFollowingStrategy(id, name, config, this.derivWs);
      case 'scalper':
        return new ScalperStrategy(id, name, config, this.derivWs);
      case 'alfha':
        return new AlfhaStrategy(id, name, config, this.derivWs);
      case 'over1_recovery':
        return new Over1RecoveryStrategy(id, name, config, this.derivWs);
      case 'even_odd_recovery': // 'LITECOIN' — antes não registado → "Estratégia desconhecida"
        return new EvenOddRecoveryStrategy(id, name, config, this.derivWs);
      case 'even_odd_smart': // 'BITCOIN'
        return new EvenOddSmartStrategy(id, name, config, this.derivWs);
      default:
        throw new Error(`Estratégia desconhecida: ${strategy}`);
    }
  }

  private getOrThrow(botId: string): BaseStrategy {
    const bot = this.bots.get(botId);
    if (!bot) throw new Error(`Bot não encontrado: ${botId}`);
    return bot;
  }
}
