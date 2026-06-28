import { Router, Request, Response, NextFunction } from 'express';
import { AuthService, setPkceCookie, readAndClearPkceCookie } from '@services/auth.service.js';
import { DerivAPIService } from '@services/deriv-api.service.js';
import { authLimiter } from '@middleware/security.js';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';

const router: Router = Router();

const FRONTEND_URL = config.frontend.url;

router.get('/login', authLimiter, (req: Request, res: Response, next: NextFunction) => {
  try {
    const prompt    = (req.query.prompt as 'login' | 'registration') || 'login';
    const isSignup  = prompt === 'registration';

    const sidc        = (req.query.sidc        as string | undefined) ?? (isSignup ? config.affiliate.sidc        : undefined);
    const utmCampaign = (req.query.utm_campaign as string | undefined) ?? (isSignup ? config.affiliate.utmCampaign : undefined);
    const utmMedium   = (req.query.utm_medium   as string | undefined) ?? (isSignup ? config.affiliate.utmMedium   : undefined);
    const utmSource   = (req.query.utm_source   as string | undefined) ?? (isSignup ? config.affiliate.utmSource   : undefined);

    const { url: authUrl, codeVerifier, state } = AuthService.buildAuthorizationUrl({
      prompt, sidc, utmCampaign, utmMedium, utmSource,
    });

    setPkceCookie(res, codeVerifier, state);

    logger.info('[Auth] Login initiated', { prompt, withAffiliate: isSignup });
    res.json({ authUrl });
  } catch (error) {
    logger.error('[Auth] Login failed', { error });
    next(error);
  }
});

router.get('/callback', authLimiter, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const code  = req.query.code  as string | undefined;
    const state = req.query.state as string | undefined;
    const error = req.query.error as string | undefined;

    if (error) {
      logger.warn('[Auth] OAuth callback error', { error });
      return res.redirect(`${FRONTEND_URL}/?error=${encodeURIComponent(error)}`);
    }

    if (!code || !state) {
      logger.warn('[Auth] Missing code or state in callback');
      return res.redirect(`${FRONTEND_URL}/?error=missing_params`);
    }

    let codeVerifier: string;
    try {
      ({ codeVerifier } = readAndClearPkceCookie(req, res, state));
    } catch (cookieErr: any) {
      logger.warn('[Auth] PKCE cookie error', { message: cookieErr.message });
      return res.redirect(`${FRONTEND_URL}/?error=${encodeURIComponent(cookieErr.message)}`);
    }

    const token   = await AuthService.exchangeCodeForToken(code, codeVerifier);
    const session = AuthService.createSession(token.accessToken, token.expiresIn);

    logger.info('[Auth] User authenticated', { userId: session.userId });

    return res.redirect(
      `${FRONTEND_URL}/callback?token=${encodeURIComponent(session.accessToken)}`,
    );
  } catch (error) {
    logger.error('[Auth] Callback failed', { error });
    return res.redirect(`${FRONTEND_URL}/?error=auth_failed`);
  }
});

router.get('/signup', authLimiter, (req: Request, res: Response, next: NextFunction) => {
  try {
    const sidc        = (req.query.sidc        as string | undefined) ?? config.affiliate.sidc;
    const utmCampaign = (req.query.utm_campaign as string | undefined) ?? config.affiliate.utmCampaign;
    const utmMedium   = (req.query.utm_medium   as string | undefined) ?? config.affiliate.utmMedium;
    const utmSource   = (req.query.utm_source   as string | undefined) ?? config.affiliate.utmSource;

    const { url: signupUrl, codeVerifier, state } = AuthService.buildAuthorizationUrl({
      prompt: 'registration', sidc, utmCampaign, utmMedium, utmSource,
    });

    setPkceCookie(res, codeVerifier, state);

    logger.info('[Auth] Signup initiated', { withAffiliate: true });
    res.json({ signupUrl });
  } catch (error) {
    logger.error('[Auth] Signup failed', { error });
    next(error);
  }
});

router.post('/refresh-token', authLimiter, (_req: Request, res: Response) => {
  res.status(501).json({
    error:   'Token refresh not yet implemented',
    message: 'Please re-authenticate using the login endpoint',
  });
});

router.post('/logout', (_req: Request, res: Response, next: NextFunction) => {
  try {
    res.clearCookie('nexora_pkce', { path: '/api/auth' });
    logger.info('[Auth] User logged out');
    res.json({ success: true, message: 'Logged out successfully' });
  } catch (error) {
    logger.error('[Auth] Logout failed', { error });
    next(error);
  }
});

router.get('/validate', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const token    = AuthService.extractBearerToken(req.headers.authorization);
    const derivAPI = new DerivAPIService(token);
    const health   = await derivAPI.healthCheck();
    res.json({ success: true, valid: health.status === 'ok' });
  } catch (error) {
    logger.error('[Auth] Token validation failed', { error });
    next(error);
  }
});

export default router;
