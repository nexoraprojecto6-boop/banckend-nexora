import { Router, Request, Response } from 'express';
import logger from '@utils/logger.js';

const router: Router = Router();

// ─── Lista de admins ──────────────────────────────────────────
// Mesma fonte usada em server.ts (fluxo WebSocket: session.isAdmin).
// Configurada via variável de ambiente:
//   ADMIN_ACCOUNT_IDS=DOT90000001,DOT90000002
// Lida aqui directamente do process.env (não de config/index.ts)
// para evitar duplicar/desincronizar a definição entre os dois
// pontos do código que precisam dela.
const ADMIN_IDS = new Set<string>(
  (process.env.ADMIN_ACCOUNT_IDS ?? '').split(',').map(s => s.trim()).filter(Boolean)
);

/**
 * GET /api/admin/check
 *
 * Verifica se a conta identificada pelo header "x-cr" está na lista
 * de admins. Usado pelo frontend (api.checkAdmin) para decidir se
 * mostra o painel "DASHBOARD ADMIN" no menu.
 *
 * Não exige o accountId via query/body para manter consistência com
 * o que o frontend já envia (header x-cr), evitando duplicar a forma
 * de identificar a conta.
 */
router.get('/check', (req: Request, res: Response) => {
  const accountId = req.headers['x-cr'] as string | undefined;

  if (!accountId) {
    res.status(400).json({ isAdmin: false, error: 'Missing x-cr header' });
    return;
  }

  const isAdmin = ADMIN_IDS.has(accountId);
  logger.info('Admin check', { accountId, isAdmin });
  res.json({ isAdmin });
});

export default router;
