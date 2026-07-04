// ============================================================
// NEXORA — Middleware JWT (sessão do login local)
//
// Usa o header "x-local-authorization" (não "Authorization")
// de propósito, para não colidir com o Bearer token da Deriv
// que já circula em "Authorization" nas rotas de trading.
// ============================================================
import { Request, Response, NextFunction } from 'express'
import jwt from 'jsonwebtoken'
import { config } from '@config/index.js'
import { AuthenticationError } from '../types/errors.js'
import { UserService } from '@services/user.service.js'

export interface LocalJwtPayload {
  sub: string        // user id
  email: string
  role: 'user' | 'admin'
}

declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace Express {
    interface Request {
      localUser?: LocalJwtPayload
    }
  }
}

export function signLocalToken(payload: LocalJwtPayload): string {
  return jwt.sign(payload, config.security.jwtSecret, { expiresIn: '7d' })
}

export function requireLocalAuth(req: Request, _res: Response, next: NextFunction) {
  try {
    const header = req.headers['x-local-authorization'] as string | undefined
    if (!header?.startsWith('Bearer ')) {
      throw new AuthenticationError('Sessão local não encontrada')
    }
    const token = header.slice(7)
    const decoded = jwt.verify(token, config.security.jwtSecret) as LocalJwtPayload
    req.localUser = decoded

    // Fire-and-forget: regista "última atividade" para as métricas
    // de online/frequência no painel admin. Nunca bloqueia o pedido.
    void UserService.touchActivity(decoded.sub)

    next()
  } catch (err) {
    next(new AuthenticationError('Sessão inválida ou expirada'))
  }
}

export function requireAdmin(req: Request, _res: Response, next: NextFunction) {
  if (req.localUser?.role !== 'admin') {
    return next(new AuthenticationError('Acesso restrito a administradores'))
  }
  next()
}
