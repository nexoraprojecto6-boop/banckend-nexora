// ============================================================
// NEXORA FOREX — Bot Routes  (/api/bots)
// ============================================================
//
// POST   /api/bots                  → criar bot
// GET    /api/bots                  → listar bots da sessão
// GET    /api/bots/:id              → estado completo do bot
// POST   /api/bots/:id/start        → iniciar
// POST   /api/bots/:id/stop         → parar
// POST   /api/bots/:id/pause        → pausar
// POST   /api/bots/:id/resume       → retomar
// DELETE /api/bots/:id              → remover
// GET    /api/bots/:id/logs         → logs (query: ?limit=N)
// POST   /api/bots/stop-all         → parar todos
//
// O BotManager é obtido via req.botManager (injectado pelo
// middleware attachBotManager em server.ts).
// ============================================================

import { Router, Request, Response, NextFunction } from 'express';
import type { BotManager } from '../bots/bot.manager.js';
import { CreateBotRequest } from '../bots/bot.types.js';
import logger from '@utils/logger.js';

declare module 'express' {
  interface Request {
    botManager?: BotManager;
  }
}

const router: Router = Router();

// ─── Helpers ─────────────────────────────────────────────────

function ok(res: Response, data: unknown, status = 200) {
  return res.status(status).json({ success: true, data });
}

function fail(res: Response, message: string, status = 400) {
  return res.status(status).json({ success: false, error: message });
}

function wrap(fn: (req: Request, res: Response) => Promise<unknown>) {
  return (req: Request, res: Response, next: NextFunction) =>
    fn(req, res).catch(next);
}

// Garante que o BotManager está disponível (cliente autenticado via WS)
function requireManager(req: Request, res: Response): BotManager | null {
  if (!req.botManager) {
    fail(res, 'Sessão WebSocket não autenticada. Conecte via WS primeiro.', 401);
    return null;
  }
  return req.botManager;
}

const VALID_STRATEGIES = ['scalping', 'martingale', 'anti_martingale', 'trend_following'];

// ─── POST /api/bots ──────────────────────────────────────────

router.post('/', wrap(async (req, res) => {
  const manager = requireManager(req, res);
  if (!manager) return;

  const { name, strategy, config }: CreateBotRequest = req.body;

  if (!name?.trim()) return fail(res, 'Campo obrigatório: name');
  if (!strategy || !VALID_STRATEGIES.includes(strategy)) {
    return fail(res, `Estratégia inválida. Opções: ${VALID_STRATEGIES.join(', ')}`);
  }
  if (!config?.symbol || !config?.initialStake || !config?.duration) {
    return fail(res, 'config deve conter: symbol, initialStake, duration, durationUnit, currency, contractType');
  }

  const state = manager.createBot({ name, strategy, config });
  logger.info(`[BotRoute] Bot criado: ${state.id}`);
  return ok(res, state, 201);
}));

// ─── GET /api/bots ───────────────────────────────────────────

router.get('/', wrap(async (req, res) => {
  const manager = requireManager(req, res);
  if (!manager) return;
  return ok(res, manager.listBots());
}));

// ─── POST /api/bots/stop-all ─────────────────────────────────

router.post('/stop-all', wrap(async (req, res) => {
  const manager = requireManager(req, res);
  if (!manager) return;
  await manager.stopAll();
  return ok(res, { message: 'Todos os bots foram parados' });
}));

// ─── GET /api/bots/:id ───────────────────────────────────────

router.get('/:id', wrap(async (req, res) => {
  const manager = requireManager(req, res);
  if (!manager) return;
  try {
    return ok(res, manager.getBotState(req.params.id));
  } catch {
    return fail(res, 'Bot não encontrado', 404);
  }
}));

// ─── POST /api/bots/:id/start ────────────────────────────────

router.post('/:id/start', wrap(async (req, res) => {
  const manager = requireManager(req, res);
  if (!manager) return;
  try {
    await manager.startBot(req.params.id);
    return ok(res, { message: 'Bot iniciado', id: req.params.id });
  } catch (err) {
    return fail(res, err instanceof Error ? err.message : String(err));
  }
}));

// ─── POST /api/bots/:id/stop ─────────────────────────────────

router.post('/:id/stop', wrap(async (req, res) => {
  const manager = requireManager(req, res);
  if (!manager) return;
  try {
    await manager.stopBot(req.params.id);
    return ok(res, { message: 'Bot parado', id: req.params.id });
  } catch (err) {
    return fail(res, err instanceof Error ? err.message : String(err));
  }
}));

// ─── POST /api/bots/:id/pause ────────────────────────────────

router.post('/:id/pause', wrap(async (req, res) => {
  const manager = requireManager(req, res);
  if (!manager) return;
  try {
    manager.pauseBot(req.params.id);
    return ok(res, { message: 'Bot pausado', id: req.params.id });
  } catch (err) {
    return fail(res, err instanceof Error ? err.message : String(err));
  }
}));

// ─── POST /api/bots/:id/resume ───────────────────────────────

router.post('/:id/resume', wrap(async (req, res) => {
  const manager = requireManager(req, res);
  if (!manager) return;
  try {
    await manager.resumeBot(req.params.id);
    return ok(res, { message: 'Bot retomado', id: req.params.id });
  } catch (err) {
    return fail(res, err instanceof Error ? err.message : String(err));
  }
}));

// ─── DELETE /api/bots/:id ────────────────────────────────────

router.delete('/:id', wrap(async (req, res) => {
  const manager = requireManager(req, res);
  if (!manager) return;
  try {
    manager.deleteBot(req.params.id);
    return ok(res, { message: 'Bot removido', id: req.params.id });
  } catch {
    return fail(res, 'Bot não encontrado', 404);
  }
}));

// ─── GET /api/bots/:id/logs ──────────────────────────────────

router.get('/:id/logs', wrap(async (req, res) => {
  const manager = requireManager(req, res);
  if (!manager) return;
  const limit = Math.min(parseInt(req.query.limit as string) || 100, 500);
  try {
    return ok(res, manager.getBotLogs(req.params.id, limit));
  } catch {
    return fail(res, 'Bot não encontrado', 404);
  }
}));

export default router;
