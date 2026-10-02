// ============================================================
// NEXORA FOREX — Auth Routes
//
// 1) Deriv OAuth2 + PKCE — INTOCADO.
// 2) Login local (email + senha) — se o email ainda não existe, a
//    conta é criada na hora e gravada no Redis. Não há registo
//    separado e não há verificação de email.
// ============================================================

import { Router, Request, Response, NextFunction } from 'express'
import { AuthService } from '@services/auth.service.js'
import { DerivAPIService } from '@services/deriv-api.service.js'
import { UserService, WeakPasswordError } from '@services/user.service.js'
import { authLimiter } from '@middleware/security.js'
import { signLocalToken, requireLocalAuth } from '@middleware/auth-local.middleware.js'
import { config } from '@config/index.js'
import logger from '@utils/logger.js'
import { RegisterSchema, LocalLoginSchema } from '../types/schemas.js'

const router: Router = Router()
const FRONTEND_URL   = config.frontend.url

// ============================================================
// 1) DERIV OAUTH — sem alterações
// ============================================================

router.get('/login', authLimiter, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const prompt    = (req.query.prompt as 'login' | 'registration') || 'login'
    const isSignup  = prompt === 'registration'

    const sidc        = (req.query.sidc        as string | undefined) ?? (isSignup ? config.affiliate.sidc        : undefined)
    const utmCampaign = (req.query.utm_campaign as string | undefined) ?? (isSignup ? config.affiliate.utmCampaign : undefined)
    const utmMedium   = (req.query.utm_medium   as string | undefined) ?? (isSignup ? config.affiliate.utmMedium   : undefined)
    const utmSource   = (req.query.utm_source   as string | undefined) ?? (isSignup ? config.affiliate.utmSource   : undefined)

    const { url: authUrl } = await AuthService.buildAuthorizationUrl({
      prompt, sidc, utmCampaign, utmMedium, utmSource,
    })

    logger.info('[Auth] Login iniciado', { prompt, withAffiliate: isSignup })
    res.json({ authUrl })
  } catch (error) {
    logger.error('[Auth] Login failed', { error })
    next(error)
  }
})

router.get('/callback', authLimiter, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const code  = req.query.code  as string | undefined
    const state = req.query.state as string | undefined
    const error = req.query.error as string | undefined

    if (error) {
      logger.warn('[Auth] OAuth callback error', { error })
      return res.redirect(`${FRONTEND_URL}/?error=${encodeURIComponent(error)}`)
    }

    if (!code || !state) {
      logger.warn('[Auth] Missing code or state in callback')
      return res.redirect(`${FRONTEND_URL}/?error=missing_params`)
    }

    let codeVerifier: string
    try {
      codeVerifier = await AuthService.retrievePkce(state)
    } catch (pkceErr: any) {
      logger.warn('[Auth] PKCE Redis error', { message: pkceErr.message })
      return res.redirect(`${FRONTEND_URL}/?error=${encodeURIComponent(pkceErr.message)}`)
    }

    const token   = await AuthService.exchangeCodeForToken(code, codeVerifier)
    const session = AuthService.createSession(token.accessToken, token.expiresIn)

    logger.info('[Auth] User authenticated', { userId: session.userId })

    return res.redirect(
      `${FRONTEND_URL}/callback?token=${encodeURIComponent(session.accessToken)}`,
    )
  } catch (error) {
    logger.error('[Auth] Callback failed', { error })
    return res.redirect(`${FRONTEND_URL}/?error=auth_failed`)
  }
})

router.get('/signup', authLimiter, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const sidc        = (req.query.sidc        as string | undefined) ?? config.affiliate.sidc
    const utmCampaign = (req.query.utm_campaign as string | undefined) ?? config.affiliate.utmCampaign
    const utmMedium   = (req.query.utm_medium   as string | undefined) ?? config.affiliate.utmMedium
    const utmSource   = (req.query.utm_source   as string | undefined) ?? config.affiliate.utmSource

    const { url: signupUrl } = await AuthService.buildAuthorizationUrl({
      prompt: 'registration', sidc, utmCampaign, utmMedium, utmSource,
    })

    logger.info('[Auth] Signup iniciado', { withAffiliate: true })
    res.json({ signupUrl })
  } catch (error) {
    logger.error('[Auth] Signup failed', { error })
    next(error)
  }
})

router.post('/logout', (_req: Request, res: Response, next: NextFunction) => {
  try {
    logger.info('[Auth] User logged out')
    res.json({ success: true, message: 'Logged out successfully' })
  } catch (error) {
    logger.error('[Auth] Logout failed', { error })
    next(error)
  }
})

router.get('/validate', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const token    = AuthService.extractBearerToken(req.headers.authorization)
    const derivAPI = new DerivAPIService(token)
    const health   = await derivAPI.healthCheck()
    res.json({ success: true, valid: health.status === 'ok' })
  } catch (error) {
    logger.error('[Auth] Token validation failed', { error })
    next(error)
  }
})

router.post('/refresh-token', authLimiter, (_req: Request, res: Response) => {
  res.status(501).json({
    error:   'Token refresh not yet implemented',
    message: 'Please re-authenticate using the login endpoint',
  })
})

// ============================================================
// 2) LOGIN LOCAL (email + senha)
// ============================================================

// ── POST /api/auth/register ───────────────────────────────────
router.post('/register', authLimiter, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { fullName, whatsapp, email, password } = RegisterSchema.parse(req.body)
    const user = await UserService.register({ fullName, whatsapp, email, password })

    logger.info('[Auth] Registo local concluído', { id: user.id })
    res.status(201).json({
      success: true,
      message: 'Conta criada com sucesso!',
      user,
    })
  } catch (error) {
    next(error)
  }
})

// ── POST /api/auth/login-local ────────────────────────────────
// Entra se o email existe; cria a conta e entra se ainda não existe.
router.post('/login-local', authLimiter, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { email, password } = LocalLoginSchema.parse(req.body)
    const { user, created } = await UserService.enter(email, password)
    const token = signLocalToken({ sub: user.id, email: user.email, role: user.role })
    res.status(created ? 201 : 200).json({ success: true, created, token, user })
  } catch (error) {
    if (error instanceof WeakPasswordError) {
      return res.status(400).json({ success: false, error: { message: error.message } })
    }
    next(error)
  }
})

// ── GET /api/auth/me ──────────────────────────────────────────
router.get('/me', requireLocalAuth, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const user = await UserService.findById(req.localUser!.sub)
    if (!user) return res.status(404).json({ error: 'Utilizador não encontrado' })
    const { passwordHash, ...publicUser } = user
    res.json({ success: true, user: publicUser })
  } catch (error) {
    next(error)
  }
})

export default router
