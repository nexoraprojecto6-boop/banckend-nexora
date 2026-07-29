// ============================================================
// NEXORA — Notification Service
// Notificações broadcast (visíveis para todos os utilizadores),
// criadas/apagadas no painel admin. Guardadas como um único blob
// JSON no Redis — volume baixo, não justifica uma chave por item.
// ============================================================
import crypto from 'crypto';
import { redisGet, redisSet } from '@utils/redis.js';

const NOTIFICATIONS_KEY  = 'notifications:list';
const MAX_NOTIFICATIONS  = 200; // corta histórico antigo automaticamente

export interface Notification {
  id: string;
  title: string;
  message: string;
  createdAt: number;
}

async function readAll(): Promise<Notification[]> {
  const raw = await redisGet(NOTIFICATIONS_KEY);
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

async function writeAll(list: Notification[]): Promise<void> {
  await redisSet(NOTIFICATIONS_KEY, JSON.stringify(list));
}

export const NotificationService = {
  async listAll(): Promise<Notification[]> {
    const list = await readAll();
    return list.sort((a, b) => b.createdAt - a.createdAt);
  },

  async create(title: string, message: string): Promise<Notification> {
    const notification: Notification = {
      id: crypto.randomUUID(),
      title: title.trim().slice(0, 80),
      message: message.trim().slice(0, 400),
      createdAt: Date.now(),
    };
    const list = await readAll();
    list.unshift(notification);
    if (list.length > MAX_NOTIFICATIONS) list.length = MAX_NOTIFICATIONS;
    await writeAll(list);
    return notification;
  },

  async deleteById(id: string): Promise<boolean> {
    const list = await readAll();
    const next = list.filter((n) => n.id !== id);
    if (next.length === list.length) return false;
    await writeAll(next);
    return true;
  },
};
