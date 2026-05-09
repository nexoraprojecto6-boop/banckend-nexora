// ============================================================
// NEXORA FOREX — Bot Routes (/api/bots)
// ============================================================
//
// Modelo de catálogo:
//   Admin  → POST/PUT/DELETE /api/bots/catalog/:id
//   User   → GET /api/bots/catalog           (lista activos)
//   User   → GET /api/bots/session           (bots da sessão WS)
//   User   → GET /api/bots/session/:id/logs
//
// O BotManager é injectado via middleware no server.ts.
// Apenas o admin pode criar/editar/eliminar bots do catálogo.
// ============================================================

import express, { Router, Request, Response } from 'express';
import { BotCatalog, CreateCatalogBotDto, UpdateCatalogBotDto } from '../bots/bot-catalog.js';
import { BotManager } from '../bots/bot.manager.js';
import logger from '@utils/logger.js';

// ─── Extensão do Request para BotManager e papel do utilizador ──

declare global {
  namespace Express {
    interface Request {
      botManager?: BotManager;
      isAdmin?: boolean;
    }
  }
}

const router: express.Router = Router();

// ─── Guard: requer BotManager (sessão WS activa) ─────────────

function requireManager(req: Request, res: Response, next: () => void): void {
  if (!req.botManager) {
    res.status(401).json({ error: 'Sessão WS não autenticada. Liga ao WebSocket primeiro.' });
    return;
  }
  next();
}

// ─── Guard: requer admin ──────────────────────────────────────

function requireAdmin(req: Request, res: Response, next: () => void): void {
  if (!req.isAdmin) {
    res.status(403).json({ error: 'Acesso restrito ao administrador.' });
    return;
  }
  next();
}

// ============================================================
// CATÁLOGO — Público (utilizadores autenticados)
// ============================================================

router.get('/catalog', async (_req, res) => {
  try {
    const bots = await BotCatalog.listActive();
    res.json({ data: bots, count: bots.length });
  } catch (err: any) {
    logger.error('[BotRoutes] Erro ao listar catálogo', { error: err.message });
    res.status(500).json({ error: 'Erro ao carregar catálogo de bots.' });
  }
});

router.get('/catalog/:id', async (req, res) => {
  try {
    const bot = await BotCatalog.getById(req.params.id);
    if (!bot || !bot.isActive) {
      res.status(404).json({ error: 'Bot não encontrado.' });
      return;
    }
    res.json({ data: bot });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// CATÁLOGO — Admin
// ============================================================

router.get('/catalog/admin/all', requireAdmin, async (_req, res) => {
  try {
    const bots = await BotCatalog.listAll();
    res.json({ data: bots, count: bots.length });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/catalog', requireAdmin, async (req, res) => {
  try {
    const dto = req.body as CreateCatalogBotDto;
    if (!dto.name || typeof dto.name !== 'string') {
      res.status(400).json({ error: 'Campo "name" é obrigatório.' });
      return;
    }
    if (!dto.strategy) {
      res.status(400).json({ error: 'Campo "strategy" é obrigatório.' });
      return;
    }
    if (!dto.defaultConfig?.symbol || !dto.defaultConfig?.contractType) {
      res.status(400).json({ error: 'defaultConfig.symbol e defaultConfig.contractType são obrigatórios.' });
      return;
    }
    const bot = await BotCatalog.create({
      ...dto,
      tags:     dto.tags     ?? [],
      isActive: dto.isActive ?? true,
    });
    logger.info(`[BotRoutes] Admin criou bot no catálogo: ${bot.id}`);
    res.status(201).json({ data: bot });
  } catch (err: any) {
    logger.error('[BotRoutes] Erro ao criar bot no catálogo', { error: err.message });
    res.status(500).json({ error: err.message });
  }
});

router.put('/catalog/:id', requireAdmin, async (req, res) => {
  try {
    const dto = req.body as UpdateCatalogBotDto;
    const updated = await BotCatalog.update(req.params.id, dto);
    if (!updated) {
      res.status(404).json({ error: 'Bot não encontrado no catálogo.' });
      return;
    }
    res.json({ data: updated });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

router.patch('/catalog/:id/toggle', requireAdmin, async (req, res) => {
  try {
    const { isActive } = req.body as { isActive: boolean };
    if (typeof isActive !== 'boolean') {
      res.status(400).json({ error: 'Campo "isActive" (boolean) é obrigatório.' });
      return;
    }
    const updated = await BotCatalog.setActive(req.params.id, isActive);
    if (!updated) {
      res.status(404).json({ error: 'Bot não encontrado.' });
      return;
    }
    res.json({ data: updated });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

router.delete('/catalog/:id', requireAdmin, async (req, res) => {
  try {
    const deleted = await BotCatalog.delete(req.params.id);
    if (!deleted) {
      res.status(404).json({ error: 'Bot não encontrado.' });
      return;
    }
    res.json({ message: 'Bot removido do catálogo com sucesso.' });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// SESSÃO — Bots activos do utilizador
// ============================================================

router.get('/session', requireManager, (req, res) => {
  try {
    const bots = req.botManager!.listBots();
    res.json({ data: bots, count: bots.length });
  } catch (err: any) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/session/:id', requireManager, (req, res) => {
  try {
    const state = req.botManager!.getBotState(req.params.id);
    res.json({ data: state });
  } catch (err: any) {
    res.status(err.message.includes('não encontrado') ? 404 : 500).json({ error: err.message });
  }
});

router.get('/session/:id/logs', requireManager, (req, res) => {
  try {
    const limit = parseInt(req.query.limit as string) || 100;
    const logs = req.botManager!.getBotLogs(req.params.id, limit);
    res.json({ data: logs, count: logs.length });
  } catch (err: any) {
    res.status(404).json({ error: err.message });
  }
});

export default router;
