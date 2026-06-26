import { createClient } from 'redis';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';
import { getMemoryStore } from '@utils/memory-store.js';

let redisClient: any = null;
let isRedisAvailable = false;

// ─── Porque isto importa ──────────────────────────────────────
// isRedisAvailable só é actualizado pelos eventos 'connect'/'error'
// do cliente. Durante uma reconexão (rede instável, Redis a
// reiniciar), pode haver uma janela onde isRedisAvailable ainda diz
// "true" mas o cliente já não aceita comandos — resultando em
// "Redis get/set error, falling back to memory" mesmo com a ligação
// teoricamente disponível. isClientUsable() verifica o estado REAL
// do cliente (redisClient.isOpen, quando exposto pela lib) antes de
// cada comando, em vez de confiar só na flag.
function isClientUsable(): boolean {
  if (!isRedisAvailable || !redisClient) return false;
  // node-redis v4 expõe isOpen/isReady; usar quando disponível para
  // uma verificação mais fiável do que a flag global.
  if (typeof redisClient.isReady === 'boolean') return redisClient.isReady;
  if (typeof redisClient.isOpen === 'boolean') return redisClient.isOpen;
  return true;
}

function errMsg(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export const initRedis = async () => {
  try {
    redisClient = createClient({
      url: config.redis.url,
      socket: {
        reconnectStrategy: (retries: number) => Math.min(retries * 50, 500),
      },
    });
    redisClient.on('error', (err: any) => {
      logger.warn('Redis connection lost, using in-memory store', { error: errMsg(err) });
      isRedisAvailable = false;
    });
    redisClient.on('connect', () => {
      logger.info('Redis connected successfully');
      isRedisAvailable = true;
    });
    // node-redis v4: dispara quando o cliente está realmente pronto
    // para aceitar comandos (depois de connect + qualquer auth/select).
    // 'connect' por si só pode disparar antes disso em certas versões.
    redisClient.on('ready', () => {
      isRedisAvailable = true;
    });
    redisClient.on('end', () => {
      logger.warn('Redis connection ended, using in-memory store');
      isRedisAvailable = false;
    });

    await redisClient.connect();
    isRedisAvailable = true;
  } catch (error) {
    logger.warn('Redis not available, using in-memory store', {
      error: errMsg(error),
    });
    isRedisAvailable = false;
  }
};

export const redisGet = async (key: string): Promise<string | null> => {
  if (isClientUsable()) {
    try {
      return await redisClient.get(key);
    } catch (error) {
      logger.error('Redis get error, falling back to memory', { key, error: errMsg(error) });
    }
  }
  const memStore = getMemoryStore();
  return memStore.get(key);
};

export const redisSet = async (
  key: string,
  value: string,
  ttl?: number
): Promise<void> => {
  if (isClientUsable()) {
    try {
      if (ttl) {
        await redisClient.setEx(key, ttl, value);
      } else {
        await redisClient.set(key, value);
      }
      return;
    } catch (error) {
      logger.error('Redis set error, falling back to memory', { key, error: errMsg(error) });
    }
  }
  const memStore = getMemoryStore();
  memStore.set(key, value, ttl);
};

export const redisDel = async (key: string): Promise<void> => {
  if (isClientUsable()) {
    try {
      await redisClient.del(key);
      return;
    } catch (error) {
      logger.error('Redis del error, falling back to memory', { key, error: errMsg(error) });
    }
  }
  const memStore = getMemoryStore();
  memStore.delete(key);
};

export const isRedisConnected = (): boolean => isClientUsable();

export const closeRedis = async (): Promise<void> => {
  if (redisClient) {
    try {
      await redisClient.quit();
      logger.info('Redis connection closed');
    } catch (error) {
      logger.error('Error closing Redis', { error: errMsg(error) });
    } finally {
      // Sem isto, isRedisAvailable podia continuar "true" depois de
      // fechar deliberadamente a ligação (ex: shutdown gracioso),
      // fazendo chamadas seguintes tentar usar um cliente já fechado.
      isRedisAvailable = false;
    }
  }
};
