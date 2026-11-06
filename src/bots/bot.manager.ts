// ============================================================
// NEXORA FOREX — Bot Manager v3 (auto_* da Deriv)
// ============================================================
// Mesma API pública da v2 (rotas e server.ts continuam a funcionar).
// Só existem 'martingale' e 'dalembert', ambas via auto_start.
// ============================================================

import { EventEmitter } from 'events';
import { v4 as uuidv4 } from 'uuid';
import {
  BotEvent,
  BotState,
  BotStrategyType,
  BotSummary,
  CreateBotRequest,
  DerivWsAdapter,
} from './bot.types.js';
import { AutoRunBot } from './auto-run.bot.js';
import type { SymbolResolver } from './strategies/base.strategy.js';
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

const STRATEGIES: readonly BotStrategyType[] = ['martingale', 'dalembert'];

export class BotManager extends EventEmitter {
  private bots = new Map<string, AutoRunBot>();
  private derivWs: DerivWsAdapter;
  private symbolResolver: SymbolResolver | null = null;

  constructor(derivWs: DerivWsAdapter) {
    super();
    this.derivWs = derivWs;
  }

  // ─── CRUD ─────────────────────────────────────────────────

  createBot(req: CreateBotRequest): BotState {
    if (!STRATEGIES.includes(req.strategy)) {
      throw new Error(`Estratégia desconhecida: ${req.strategy} (disponíveis: ${STRATEGIES.join(', ')})`);
    }
    const id = uuidv4();
    const bot = new AutoRunBot(id, req.name, req.strategy, req.config, this.derivWs);

    BOT_EVENTS.forEach((event) => {
      bot.on(event, (botEvent: BotEvent) => this.emit('bot_event', botEvent));
    });

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

  /** Assíncrono (chama auto_pause); nunca rejeita. */
  async pauseBot(botId: string): Promise<void> {
    await this.getOrThrow(botId).pause();
  }

  async resumeBot(botId: string): Promise<void> {
    await this.getOrThrow(botId).resume();
  }

  async deleteBot(botId: string): Promise<void> {
    const bot = this.getOrThrow(botId);
    const status = bot.status;
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
    const targets = Array.from(this.bots.values()).filter(
      (b) => b.status === 'running' || b.status === 'paused',
    );
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

  /**
   * Mantido por compatibilidade com server.ts. No modelo auto_* o
   * símbolo é fixo no run; a troca automática de mercado já não existe.
   */
  setSymbolResolver(resolver: SymbolResolver | null): void {
    this.symbolResolver = resolver;
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

  private getOrThrow(botId: string): AutoRunBot {
    const bot = this.bots.get(botId);
    if (!bot) throw new Error(`Bot não encontrado: ${botId}`);
    return bot;
  }
}
