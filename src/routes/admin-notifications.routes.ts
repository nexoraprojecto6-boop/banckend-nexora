// ============================================================
// NEXORA — Admin: gestão de notificações broadcast
// Montado em /api/admin/notifications no server.ts
// ============================================================
import { Router, Request, Response, NextFunction } from 'express'
import { NotificationService } from '@services/notification.service.js'
import { requireLocalAuth, requireAdmin } from '@middleware/auth-local.middleware.js'

const router: Router = Router()

// GET /api/admin/notifications — lista completa (gestão no painel admin)
router.get('/', requireLocalAuth, requireAdmin, async (_req: Request, res: Response, next: NextFunction) => {
  try {
    const notifications = await NotificationService.listAll()
    res.json({ success: true, notifications })
  } catch (error) {
    next(error)
  }
})

// POST /api/admin/notifications — criar
router.post('/', requireLocalAuth, requireAdmin, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { title, message } = req.body as { title?: string; message?: string }
    if (!title?.trim() || !message?.trim()) {
      return res.status(400).json({ error: 'title e message são obrigatórios' })
    }
    const notification = await NotificationService.create(title, message)
    res.json({ success: true, notification })
  } catch (error) {
    next(error)
  }
})

// DELETE /api/admin/notifications/:id — apagar
router.delete('/:id', requireLocalAuth, requireAdmin, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const deleted = await NotificationService.deleteById(req.params.id)
    if (!deleted) {
      return res.status(404).json({ error: 'Notificação não encontrada' })
    }
    res.json({ success: true })
  } catch (error) {
    next(error)
  }
})

export default router
