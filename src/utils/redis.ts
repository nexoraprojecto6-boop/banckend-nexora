import logger from '@utils/logger.js';
import { getMemoryStore } from '@utils/memory-store.js';

// ─── Redis removido ─────────────────────────────────────────
// O serviço passou a usar apenas armazenamento em memória.
// Estas funções mantêm a mesma assinatura/API que o resto do
// código já espera (initRedis, redisGet, redisSet, redisDel,
// isRedisConnected, closeRedis), para não exigir alterações em
// server.ts ou noutros ficheiros que as importam.

export const initRedis = async (): Promise<void> => {
  logger.info('Redis desactivado — a usar armazenamento em memória');
};

export const redisGet = async (key: string): Promise<string | null> => {
  const memStore = getMemoryStore();
  return memStore.get(key);
};

export const redisSet = async (
  key: string,
  value: string,
  ttl?: number
): Promise<void> => {
  const memStore = getMemoryStore();
  memStore.set(key, value, ttl);
};

export const redisDel = async (key: string): Promise<void> => {
  const memStore = getMemoryStore();
  memStore.delete(key);
};

export const isRedisConnected = (): boolean => false;

export const closeRedis = async (): Promise<void> => {
  // Nada a fazer — não há ligação Redis activa.
};
