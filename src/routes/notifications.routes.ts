// ============================================================
// NEXORA — Notificações (leitura, para o dashboard)
// Montado em /api/notifications no server.ts
// ============================================================
import { Router, Request, Response, NextFunction } from 'express'
import { NotificationService } from '@services/notification.service.js'
import { requireLocalAuth } from '@middleware/auth-local.middleware.js'

const router: Router = Router()

// GET /api/notifications — todas as notificações broadcast
// (qualquer utilizador autenticado localmente pode ler; o
// "lido/não lido" é controlado no frontend via localStorage)
router.get('/', requireLocalAuth, async (_req: Request, res: Response, next: NextFunction) => {
  try {
    const notifications = await NotificationService.listAll()
    res.json({ success: true, notifications })
  } catch (error) {
    next(error)
  }
})

export default router
