// ============================================================
// NEXORA FOREX — Bot Catalog
// ============================================================
// O catálogo de bots é gerido exclusivamente pelo admin.
// Os utilizadores apenas consultam, escolhem e executam.
//
// Persistência: memória (sem dependências extras do redis.ts).
// ============================================================

import { BotStrategyType, BotConfig } from './bot.types.js';
import { v4 as uuidv4 } from 'uuid';
import logger from '@utils/logger.js';

// ─── Tipos do catálogo ────────────────────────────────────────

export interface CatalogBot {
  id: string;
  name: string;
  description: string;
  strategy: BotStrategyType;
  defaultConfig: BotConfig;
  tags: string[];
  createdAt: string;
  updatedAt: string;
  createdBy: string;
  isActive: boolean;
}

export type CreateCatalogBotDto = Omit<CatalogBot, 'id' | 'createdAt' | 'updatedAt'>;
export type UpdateCatalogBotDto = Partial<Omit<CatalogBot, 'id' | 'createdAt' | 'updatedAt' | 'createdBy'>>;

// ─── Store em memória ─────────────────────────────────────────

const store = new Map<string, CatalogBot>();

// ─── BotCatalog — API pública ─────────────────────────────────

export const BotCatalog = {

  async listActive(): Promise<CatalogBot[]> {
    return Array.from(store.values()).filter(b => b.isActive);
  },

  async listAll(): Promise<CatalogBot[]> {
    return Array.from(store.values());
  },

  async getById(id: string): Promise<CatalogBot | null> {
    return store.get(id) ?? null;
  },

  async create(dto: CreateCatalogBotDto): Promise<CatalogBot> {
    const bot: CatalogBot = {
      ...dto,
      id:        uuidv4(),
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    store.set(bot.id, bot);
    logger.info(`[BotCatalog] Bot criado: ${bot.id} — ${bot.name}`);
    return bot;
  },

  async update(id: string, dto: UpdateCatalogBotDto): Promise<CatalogBot | null> {
    const existing = store.get(id);
    if (!existing) return null;
    const updated: CatalogBot = {
      ...existing,
      ...dto,
      updatedAt: new Date().toISOString(),
    };
    store.set(id, updated);
    logger.info(`[BotCatalog] Bot actualizado: ${id}`);
    return updated;
  },

  async delete(id: string): Promise<boolean> {
    if (!store.has(id)) return false;
    store.delete(id);
    logger.info(`[BotCatalog] Bot eliminado: ${id}`);
    return true;
  },

  async setActive(id: string, active: boolean): Promise<CatalogBot | null> {
    return this.update(id, { isActive: active });
  },
};
