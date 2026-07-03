// ============================================================
// NEXORA — Admin: gestão e estatísticas de utilizadores locais
// Montado em /api/admin/users no server.ts
// ============================================================

import { Router, Request, Response, NextFunction } from 'express'
import { UserService } from '@services/user.service.js'
import { requireLocalAuth, requireAdmin } from '@middleware/auth-local.middleware.js'

const router: Router = Router()

// GET /api/admin/users/stats — total cadastrados, ativos, inativos
router.get('/stats', requireLocalAuth, requireAdmin, async (_req: Request, res: Response, next: NextFunction) => {
  try {
    const stats = await UserService.getStats()
    res.json({ success: true, stats })
  } catch (error) {
    next(error)
  }
})

// GET /api/admin/users — lista de utilizadores (sem passwordHash)
router.get('/', requireLocalAuth, requireAdmin, async (_req: Request, res: Response, next: NextFunction) => {
  try {
    const users = await UserService.listAll()
    const publicUsers = users.map(({ passwordHash, ...u }) => u)
    res.json({ success: true, users: publicUsers, total: publicUsers.length })
  } catch (error) {
    next(error)
  }
})

// PATCH /api/admin/users/:id/status — bloquear/reativar utilizador
router.patch('/:id/status', requireLocalAuth, requireAdmin, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { status } = req.body as { status: 'active' | 'blocked' }
    if (status !== 'active' && status !== 'blocked') {
      return res.status(400).json({ error: "status deve ser 'active' ou 'blocked'" })
    }
    const user = await UserService.setStatus(req.params.id, status)
    res.json({ success: true, user })
  } catch (error) {
    next(error)
  }
})

export default router
