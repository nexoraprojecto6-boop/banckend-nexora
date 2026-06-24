import { Router, Request, Response, NextFunction } from 'express';
import { AuthService } from '@services/auth.service.js';
import { DerivAPIService } from '@services/deriv-api.service.js';
import { authLimiter } from '@middleware/security.js';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';

const router: Router = Router();

const FRONTEND_URL = process.env.FRONTEND_URL || 'https://teu-frontend.vercel.app';

/**
 * GET /api/auth/login
 *
 * Para signup (prompt=registration), aplica por defeito os parâmetros
 * de afiliado configurados em config.affiliate (sidc/utm_*), para que
 * todo novo registo feito através da plataforma seja atribuído à conta
 * de parceiro Deriv. O frontend pode sobrepor explicitamente via query
 * string se precisar de uma campanha diferente.
 */
router.get('/login', authLimiter, (req: Request, res: Response, next: NextFunction) => {
  try {
    const prompt = (req.query.prompt as 'login' | 'registration') || 'login';
    const isSignup = prompt === 'registration';

    const sidc = (req.query.sidc as string | undefined)
      ?? (isSignup ? config.affiliate.sidc : undefined);
    const utmCampaign = (req.query.utm_campaign as string | undefined)
      ?? (isSignup ? config.affiliate.utmCampaign : undefined);
    const utmMedium = (req.query.utm_medium as string | undefined)
      ?? (isSignup ? config.affiliate.utmMedium : undefined);
    const utmSource = (req.query.utm_source as string | undefined)
      ?? (isSignup ? config.affiliate.utmSource : undefined);

    const authUrl = AuthService.buildAuthorizationUrl({
      prompt,
      sidc,
      utmCampaign,
      utmMedium,
      utmSource,
    });

    logger.info('Login initiated', { prompt, withAffiliate: isSignup });
    res.json({ authUrl });
  } catch (error) {
    logger.error('Login failed', { error });
    next(error);
  }
});

/**
 * GET /api/auth/callback
 */
router.get('/callback', authLimiter, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const code = req.query.code as string | undefined;
    const state = req.query.state as string | undefined;
    const error = req.query.error as string | undefined;

    if (error) {
      logger.warn('OAuth callback error', { error });
      return res.redirect(`${FRONTEND_URL}/?error=${error}`);
    }

    if (!code || !state) {
      logger.warn('Missing authorization code or state');
      return res.redirect(`${FRONTEND_URL}/?error=missing_params`);
    }

    const token = await AuthService.exchangeCodeForToken(code, state);
    const session = AuthService.createSession(token.accessToken, token.expiresIn);

    logger.info('User authenticated successfully', { userId: session.userId });

    // Redireciona para o frontend — /callback salva o token e redireciona para /dashboard
    return res.redirect(`${FRONTEND_URL}/callback?token=${session.accessToken}`);

  } catch (error) {
    logger.error('Callback handling failed', { error });
    return res.redirect(`${FRONTEND_URL}/?error=auth_failed`);
  }
});

/**
 * GET /api/auth/signup
 *
 * Aplica sempre os parâmetros de afiliado por defeito (config.affiliate),
 * a menos que o chamador sobreponha explicitamente via query string.
 */
router.get('/signup', authLimiter, (req: Request, res: Response, next: NextFunction) => {
  try {
    const sidc = (req.query.sidc as string | undefined) ?? config.affiliate.sidc;
    const utmCampaign = (req.query.utm_campaign as string | undefined) ?? config.affiliate.utmCampaign;
    const utmMedium = (req.query.utm_medium as string | undefined) ?? config.affiliate.utmMedium;
    const utmSource = (req.query.utm_source as string | undefined) ?? config.affiliate.utmSource;

    const signupUrl = AuthService.buildAuthorizationUrl({
      prompt: 'registration',
      sidc,
      utmCampaign,
      utmMedium,
      utmSource,
    });

    logger.info('Signup initiated', { withAffiliate: true });
    res.json({ signupUrl });
  } catch (error) {
    logger.error('Signup failed', { error });
    next(error);
  }
});

/**
 * POST /api/auth/refresh-token
 */
router.post('/refresh-token', authLimiter, (_req: Request, res: Response) => {
  res.status(501).json({
    error: 'Token refresh not yet implemented',
    message: 'Please re-authenticate using the login endpoint',
  });
});

/**
 * POST /api/auth/logout
 */
router.post('/logout', (req: Request, res: Response, next: NextFunction) => {
  try {
    logger.info('User logged out');
    res.json({
      success: true,
      message: 'Logged out successfully',
    });
  } catch (error) {
    logger.error('Logout failed', { error });
    next(error);
  }
});

/**
 * GET /api/auth/validate
 */
router.get('/validate', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const authHeader = req.headers.authorization;
    const token = AuthService.extractBearerToken(authHeader);

    const derivAPI = new DerivAPIService(token);
    const health = await derivAPI.healthCheck();

    res.json({
      success: true,
      valid: health.status === 'ok',
    });
  } catch (error) {
    logger.error('Token validation failed', { error });
    next(error);
  }
});

export default router;
