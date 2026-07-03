// ============================================================
// NEXORA FOREX — Auth Routes
//
// 1) Deriv OAuth2 + PKCE — INTOCADO.
// 2) Login local (nome, WhatsApp, email + senha) com verificação
//    de email obrigatória antes do login funcionar.
// ============================================================

import { Router, Request, Response, NextFunction } from 'express'
import { AuthService } from '@services/auth.service.js'
import { DerivAPIService } from '@services/deriv-api.service.js'
import { UserService } from '@services/user.service.js'
import { authLimiter } from '@middleware/security.js'
import { signLocalToken, requireLocalAuth } from '@middleware/auth-local.middleware.js'
import { config } from '@config/index.js'
import logger from '@utils/logger.js'
import {
  RegisterSchema, LocalLoginSchema, VerifyEmailSchema, ResendVerificationSchema,
} from '../types/schemas.js'

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
// 2) LOGIN LOCAL (nome, WhatsApp, email + senha)
// ============================================================

// ── POST /api/auth/register ───────────────────────────────────
router.post('/register', authLimiter, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { fullName, whatsapp, email, password } = RegisterSchema.parse(req.body)
    const user = await UserService.register({ fullName, whatsapp, email, password })

    logger.info('[Auth] Registo local concluído, aguarda verificação de email', { id: user.id })
    res.status(201).json({
      success: true,
      message: 'Conta criada. Verifica o teu email para poderes entrar.',
      user,
    })
  } catch (error) {
    next(error)
  }
})

// ── GET /api/auth/verify-email?token=... ───────────────────────
// Aberto via link no email. Redireciona para o frontend com resultado.
router.get('/verify-email', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const token = req.query.token as string | undefined
    if (!token) {
      return res.redirect(`${FRONTEND_URL}/verify-email?status=missing_token`)
    }

    const user  = await UserService.verifyEmailToken(token)
    const authToken = signLocalToken({ sub: user.id, email: user.email, role: user.role })

    return res.redirect(
      `${FRONTEND_URL}/verify-email?status=success&token=${encodeURIComponent(authToken)}`,
    )
  } catch (error: any) {
    logger.warn('[Auth] Falha na verificação de email', { message: error.message })
    return res.redirect(`${FRONTEND_URL}/verify-email?status=error&message=${encodeURIComponent(error.message)}`)
  }
})

// ── POST /api/auth/verify-email ─────────────────────────────────
// Alternativa em JSON, caso o frontend prefira chamar via fetch
// em vez de seguir o redirect do link do email.
router.post('/verify-email', authLimiter, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { token } = VerifyEmailSchema.parse(req.body)
    const user = await UserService.verifyEmailToken(token)
    const authToken = signLocalToken({ sub: user.id, email: user.email, role: user.role })
    res.json({ success: true, token: authToken, user })
  } catch (error) {
    next(error)
  }
})

// ── POST /api/auth/resend-verification ──────────────────────────
router.post('/resend-verification', authLimiter, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { email } = ResendVerificationSchema.parse(req.body)
    await UserService.resendVerification(email)
    // Resposta genérica, não confirma se o email existe (evita enumeração de contas)
    res.json({ success: true, message: 'Se o email existir e não estiver verificado, foi enviado um novo link.' })
  } catch (error) {
    next(error)
  }
})

// ── POST /api/auth/login-local ────────────────────────────────
router.post('/login-local', authLimiter, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { email, password } = LocalLoginSchema.parse(req.body)
    const user  = await UserService.login(email, password)
    const token = signLocalToken({ sub: user.id, email: user.email, role: user.role })
    res.json({ success: true, token, user })
  } catch (error) {
    next(error)
  }
})

// ── GET /api/auth/me ────────────────────────────────────────────
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
