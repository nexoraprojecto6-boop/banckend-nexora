// ============================================================
// NEXORA FOREX — Bot Catalog
// ============================================================
// O catálogo de bots é gerido exclusivamente pelo admin.
// Os utilizadores apenas consultam, escolhem e executam.
//
// Persistência: Redis (fallback: memória)
// ============================================================

import { getRedisClient, isRedisConnected } from '@utils/redis.js';
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
  tags: string[];              // ex: ['volátil', 'conservador', 'scalping']
  createdAt: string;
  updatedAt: string;
  createdBy: string;           // accountId do admin
  isActive: boolean;           // false = oculto dos utilizadores
}

export type CreateCatalogBotDto = Omit<CatalogBot, 'id' | 'createdAt' | 'updatedAt'>;
export type UpdateCatalogBotDto = Partial<Omit<CatalogBot, 'id' | 'createdAt' | 'updatedAt' | 'createdBy'>>;

// ─── Chaves Redis ─────────────────────────────────────────────

const KEY_ALL    = 'nexora:catalog:all';        // SET com todos os IDs
const keyBot = (id: string) => `nexora:catalog:bot:${id}`;

// ─── Fallback em memória ──────────────────────────────────────

const memStore = new Map<string, CatalogBot>();

// ─── Helpers Redis/Mem ────────────────────────────────────────

async function redisGet(key: string): Promise<string | null> {
  if (!isRedisConnected()) return null;
  try {
    const r = getRedisClient();
    return await r.get(key);
  } catch { return null; }
}

async function redisSet(key: string, value: string): Promise<void> {
  if (!isRedisConnected()) return;
  try {
    const r = getRedisClient();
    await r.set(key, value);
  } catch { /* ignora */ }
}

async function redisSAdd(key: string, member: string): Promise<void> {
  if (!isRedisConnected()) return;
  try {
    const r = getRedisClient();
    await r.sAdd(key, member);
  } catch { /* ignora */ }
}

async function redisSRem(key: string, member: string): Promise<void> {
  if (!isRedisConnected()) return;
  try {
    const r = getRedisClient();
    await r.sRem(key, member);
  } catch { /* ignora */ }
}

async function redisDel(key: string): Promise<void> {
  if (!isRedisConnected()) return;
  try {
    const r = getRedisClient();
    await r.del(key);
  } catch { /* ignora */ }
}

async function redisSMembers(key: string): Promise<string[]> {
  if (!isRedisConnected()) return [];
  try {
    const r = getRedisClient();
    return await r.sMembers(key);
  } catch { return []; }
}

// ─── BotCatalog — API pública ─────────────────────────────────

export const BotCatalog = {

  // ── Listar todos os bots activos (para utilizadores) ─────────
  async listActive(): Promise<CatalogBot[]> {
    const bots = await this._listAll();
    return bots.filter(b => b.isActive);
  },

  // ── Listar todos (para admin, inclui inactivos) ───────────────
  async listAll(): Promise<CatalogBot[]> {
    return this._listAll();
  },

  // ── Obter um bot por id ───────────────────────────────────────
  async getById(id: string): Promise<CatalogBot | null> {
    // Redis
    const raw = await redisGet(keyBot(id));
    if (raw) {
      try { return JSON.parse(raw) as CatalogBot; } catch { /* ignora */ }
    }
    // Memória
    return memStore.get(id) ?? null;
  },

  // ── Criar bot (admin) ─────────────────────────────────────────
  async create(dto: CreateCatalogBotDto): Promise<CatalogBot> {
    const bot: CatalogBot = {
      ...dto,
      id: uuidv4(),
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
    await this._save(bot);
    logger.info(`[BotCatalog] Bot criado: ${bot.id} — ${bot.name}`);
    return bot;
  },

  // ── Actualizar bot (admin) ────────────────────────────────────
  async update(id: string, dto: UpdateCatalogBotDto): Promise<CatalogBot | null> {
    const existing = await this.getById(id);
    if (!existing) return null;
    const updated: CatalogBot = {
      ...existing,
      ...dto,
      updatedAt: new Date().toISOString(),
    };
    await this._save(updated);
    logger.info(`[BotCatalog] Bot actualizado: ${id}`);
    return updated;
  },

  // ── Eliminar bot (admin) ──────────────────────────────────────
  async delete(id: string): Promise<boolean> {
    const exists = await this.getById(id);
    if (!exists) return false;

    // Redis
    await redisDel(keyBot(id));
    await redisSRem(KEY_ALL, id);

    // Memória
    memStore.delete(id);

    logger.info(`[BotCatalog] Bot eliminado: ${id}`);
    return true;
  },

  // ── Activar / Desactivar (admin) ──────────────────────────────
  async setActive(id: string, active: boolean): Promise<CatalogBot | null> {
    return this.update(id, { isActive: active });
  },

  // ─── Privados ─────────────────────────────────────────────────

  async _save(bot: CatalogBot): Promise<void> {
    const json = JSON.stringify(bot);
    await redisSet(keyBot(bot.id), json);
    await redisSAdd(KEY_ALL, bot.id);
    memStore.set(bot.id, bot); // mantém memória sincronizada
  },

  async _listAll(): Promise<CatalogBot[]> {
    // Tenta Redis primeiro
    const ids = await redisSMembers(KEY_ALL);
    if (ids.length > 0) {
      const bots = await Promise.all(ids.map(id => this.getById(id)));
      return bots.filter(Boolean) as CatalogBot[];
    }
    // Fallback: memória
    return Array.from(memStore.values());
  },
};
