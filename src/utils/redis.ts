import { createClient, type RedisClientType } from 'redis';
import logger from '@utils/logger.js';
import { getMemoryStore } from '@utils/memory-store.js';

// ─── Cliente Redis ──────────────────────────────────────────
let client: RedisClientType | null = null;
let connected = false;

export const initRedis = async (): Promise<void> => {
  const url = process.env.REDIS_URL;

  if (!url) {
    logger.warn('REDIS_URL não definida — a usar armazenamento em memória');
    return;
  }

  try {
    client = createClient({ url });

    client.on('error', (err) => {
      logger.error('[Redis] Erro de ligação', { err });
      connected = false;
    });

    client.on('connect', () => {
      logger.info('[Redis] Ligado com sucesso');
      connected = true;
    });

    client.on('end', () => {
      logger.warn('[Redis] Ligação encerrada');
      connected = false;
    });

    await client.connect();
    connected = true;
  } catch (err) {
    logger.error('[Redis] Falha ao iniciar ligação, a usar memória como fallback', { err });
    client = null;
    connected = false;
  }
};

// Lança erro se chamado sem ligação ativa — quem chama deve
// verificar isRedisConnected() antes, ou apanhar o erro.
export const getRedisClient = (): RedisClientType => {
  if (!client || !connected) {
    throw new Error('Redis não está conectado');
  }
  return client;
};

export const isRedisConnected = (): boolean => connected;

// ─── Helpers de alto nível (com fallback automático p/ memória) ─
export const redisGet = async (key: string): Promise<string | null> => {
  if (connected && client) {
    try {
      return await client.get(key);
    } catch (err) {
      logger.warn('[Redis] Falha no GET, a usar memória', { err });
    }
  }
  return getMemoryStore().get(key);
};

export const redisSet = async (
  key: string,
  value: string,
  ttl?: number
): Promise<void> => {
  if (connected && client) {
    try {
      if (ttl) {
        await client.set(key, value, { EX: ttl });
      } else {
        await client.set(key, value);
      }
      return;
    } catch (err) {
      logger.warn('[Redis] Falha no SET, a usar memória', { err });
    }
  }
  getMemoryStore().set(key, value, ttl);
};

export const redisDel = async (key: string): Promise<void> => {
  if (connected && client) {
    try {
      await client.del(key);
      return;
    } catch (err) {
      logger.warn('[Redis] Falha no DEL, a usar memória', { err });
    }
  }
  getMemoryStore().delete(key);
};

export const closeRedis = async (): Promise<void> => {
  if (client) {
    try {
      await client.quit();
    } catch (err) {
      logger.warn('[Redis] Erro ao fechar ligação', { err });
    } finally {
      connected = false;
      client = null;
    }
  }
};
