// ============================================================
// NEXORA — Middleware JWT (sessão do login local)
//
// Usa o header "x-local-authorization" (não "Authorization")
// de propósito, para não colidir com o Bearer token da Deriv
// que já circula em "Authorization" nas rotas de trading.
//
// A cada pedido o utilizador é confirmado no Redis:
//   - utilizador inexistente -> 401 (o frontend volta ao login);
//   - conta bloqueada -> 401 na hora, sem esperar o token expirar;
//   - o role vem da base de dados, por isso retirar um admin
//     tem efeito imediato.
// ============================================================
import { Request, Response, NextFunction } from 'express'
import jwt, { type SignOptions } from 'jsonwebtoken'
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

export function signLocalToken(
  payload: LocalJwtPayload,
  expiresIn: SignOptions['expiresIn'] = '7d',
): string {
  return jwt.sign(payload, config.security.jwtSecret, { expiresIn })
}

async function authenticate(req: Request): Promise<void> {
  const header = req.headers['x-local-authorization'] as string | undefined
  if (!header?.startsWith('Bearer ')) {
    throw new AuthenticationError('Sessão local não encontrada')
  }

  const decoded = jwt.verify(header.slice(7), config.security.jwtSecret) as LocalJwtPayload

  const user = await UserService.findById(decoded.sub)
  if (!user) throw new AuthenticationError('Sessão inválida ou expirada')
  if (user.status === 'blocked') throw new AuthenticationError('Conta bloqueada. Contacta o suporte.')

  req.localUser = { sub: user.id, email: user.email, role: user.role }

  // Fire-and-forget (já limitado a 1 gravação/minuto no serviço).
  void UserService.touchActivity(user.id)
}

export function requireLocalAuth(req: Request, _res: Response, next: NextFunction) {
  authenticate(req)
    .then(() => next())
    .catch((err) => {
      next(err instanceof AuthenticationError ? err : new AuthenticationError('Sessão inválida ou expirada'))
    })
}

export function requireAdmin(req: Request, _res: Response, next: NextFunction) {
  if (req.localUser?.role !== 'admin') {
    return next(new AuthenticationError('Acesso restrito a administradores'))
  }
  next()
}
