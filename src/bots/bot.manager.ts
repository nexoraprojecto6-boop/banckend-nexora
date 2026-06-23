// ============================================================
// NEXORA FOREX — Bot Manager
// ============================================================
// Cada cliente WS tem o seu próprio BotManager (instanciado
// no server.ts quando a sessão é autenticada).
//
// Isso garante isolamento total: os bots de um cliente não
// interferem com os bots de outro.
//
// O BotManager emite eventos que o server.ts reencaminha
// ao cliente via sendToClient().
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
import { BaseStrategy } from './strategies/base.strategy.js';
import { ScalpingStrategy } from './strategies/scalping.strategy.js';
import { MartingaleStrategy } from './strategies/martingale.strategy.js';
import { TrendFollowingStrategy } from './strategies/trend-following.strategy.js';
import logger from '@utils/logger.js';

// Todos os eventos emitidos pelas estratégias.
// bot:goal_reached e bot:insufficient_balance são emitidos pela
// BaseStrategy antes de bot:stopped, para o frontend distinguir o
// motivo exacto da paragem (meta atingida, stop de perda, saldo
// insuficiente) em vez de um stop genérico.
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

  constructor(derivWs: DerivWsAdapter) {
    super();
    this.derivWs = derivWs;
  }

  // ─── CRUD ─────────────────────────────────────────────────

  createBot(req: CreateBotRequest): BotState {
    const id = uuidv4();
    const bot = this.buildStrategy(id, req.name, req.strategy, req.config);

    // Borbulhar todos os eventos do bot para o manager
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

  pauseBot(botId: string): void {
    this.getOrThrow(botId).pause();
  }

  async resumeBot(botId: string): Promise<void> {
    await this.getOrThrow(botId).resume();
  }

  deleteBot(botId: string): void {
    const bot = this.getOrThrow(botId);
    if (bot.getState().status === 'running') bot.stop('deleted');
    this.bots.delete(botId);
    logger.info(`[BotManager] Bot removido — id: ${botId}`);
  }

  async stopAll(): Promise<void> {
    await Promise.all(
      Array.from(this.bots.values())
        .filter((b) => b.getState().status === 'running')
        .map((b) => b.stop('bulk_stop')),
    );
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

  // ─── Atualizar o derivWs quando o cliente trocar de conta ──

  updateDerivWs(derivWs: DerivWsAdapter): void {
    this.derivWs = derivWs;
    logger.info('[BotManager] derivWs atualizado (troca de conta)');
  }

  // ─── Cleanup ao desligar sessão ───────────────────────────

  async destroy(): Promise<void> {
    await this.stopAll();
    this.bots.clear();
    this.removeAllListeners();
  }

  // ─── Factory de estratégias ───────────────────────────────

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
