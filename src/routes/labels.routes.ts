import { Router, Request, Response } from 'express';
import { redisGet, redisSet } from '@utils/redis.js';
import logger from '@utils/logger.js';

const router: Router = Router();

// ─── Lista de admins ──────────────────────────────────────────
// Mesma fonte usada em server.ts e admin.routes.ts.
const ADMIN_IDS = new Set<string>(
  (process.env.ADMIN_ACCOUNT_IDS ?? '').split(',').map(s => s.trim()).filter(Boolean)
);

const REDIS_KEY = 'nexora:account_labels';

// ─── Rótulos por defeito ──────────────────────────────────────
// IMPORTANTE: isto é só texto de exibição. Não altera, troca, nem
// inverte qual conta é "real" e qual é "demo" — essa informação
// (is_virtual / account_type) vem sempre directamente da Deriv e
// nunca é tocada aqui. Apenas o RÓTULO mostrado na UI é configurável.
const DEFAULT_LABELS = {
  realLabel: 'Real',
  demoLabel: 'Demo',
};

interface AccountLabels {
  realLabel: string;
  demoLabel: string;
}

function isAdminRequest(req: Request): boolean {
  const accountId = req.headers['x-cr'] as string | undefined;
  return !!accountId && ADMIN_IDS.has(accountId);
}

/**
 * GET /api/admin/labels
 *
 * Devolve os rótulos personalizados actuais (ou os defeito, se nunca
 * tiverem sido configurados). Acessível a qualquer utilizador
 * autenticado — os rótulos são só de exibição, não há informação
 * sensível aqui, e todos os utilizadores devem ver a mesma etiqueta.
 */
router.get('/', async (_req: Request, res: Response) => {
  try {
    const stored = await redisGet(REDIS_KEY);
    const labels: AccountLabels = stored ? JSON.parse(stored) : DEFAULT_LABELS;
    res.json(labels);
  } catch (error) {
    logger.error('Failed to read account labels', { error });
    res.json(DEFAULT_LABELS);
  }
});

/**
 * POST /api/admin/labels
 *
 * Actualiza os rótulos personalizados. Apenas admins (verificado via
 * header x-cr contra ADMIN_ACCOUNT_IDS). Persistido no Redis — sem
 * TTL, fica guardado indefinidamente até ser alterado de novo.
 *
 * Body: { realLabel?: string, demoLabel?: string }
 */
router.post('/', async (req: Request, res: Response) => {
  if (!isAdminRequest(req)) {
    res.status(403).json({ error: 'Apenas administradores podem alterar os rótulos.' });
    return;
  }

  const { realLabel, demoLabel } = req.body as Partial<AccountLabels>;

  if (
    (realLabel !== undefined && (typeof realLabel !== 'string' || realLabel.trim().length === 0)) ||
    (demoLabel !== undefined && (typeof demoLabel !== 'string' || demoLabel.trim().length === 0))
  ) {
    res.status(400).json({ error: 'Rótulos devem ser texto não vazio.' });
    return;
  }

  try {
    const stored = await redisGet(REDIS_KEY);
    const current: AccountLabels = stored ? JSON.parse(stored) : DEFAULT_LABELS;

    const updated: AccountLabels = {
      realLabel: realLabel?.trim() || current.realLabel,
      demoLabel: demoLabel?.trim() || current.demoLabel,
    };

    await redisSet(REDIS_KEY, JSON.stringify(updated));
    logger.info('Account labels updated', { updated });
    res.json(updated);
  } catch (error) {
    logger.error('Failed to update account labels', { error });
    res.status(500).json({ error: 'Falha ao guardar rótulos.' });
  }
});

export default router;
