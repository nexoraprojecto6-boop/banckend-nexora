import express, { Request, Response, NextFunction, RequestHandler } from 'express';
import helmet from 'helmet';
import cors from 'cors';
import rateLimit from 'express-rate-limit';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';

// Helmet middleware
export const helmetMiddleware = helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", 'data:', 'https:'],
    },
  },
  hsts: {
    maxAge: 31536000,
    includeSubDomains: true,
    preload: true,
  },
  frameguard: { action: 'deny' },
  noSniff: true,
  xssFilter: true,
});

// ── CORS ─────────────────────────────────────────────────────

// Expande cada origin configurada para incluir a variante com e sem
// "www." — evita bloquear pedidos por engano quando o DNS aponta as
// duas formas (nexoraforex.pro e www.nexoraforex.pro) mas só uma
// delas foi definida nas env vars.
function withWwwVariants(url: string): string[] {
  try {
    const u = new URL(url);
    const bareHost = u.host.replace(/^www\./, '');
    const wwwHost = `www.${bareHost}`;
    return [
      `${u.protocol}//${bareHost}`,
      `${u.protocol}//${wwwHost}`,
    ];
  } catch {
    return [url];
  }
}

// IMPORTANTE: config.security.corsOrigin (env var CORS_ORIGIN) NÃO é
// usado aqui — as origens permitidas vêm de config.frontend.url e
// config.frontend.prodUrl. Para mudar o domínio aceite pelo CORS,
// atualiza FRONTEND_URL / FRONTEND_PROD_URL no .env, não CORS_ORIGIN.
const configuredOrigins = [
  config.frontend.url,
  config.frontend.prodUrl,
  'http://localhost:3000',
  'http://localhost:3001',
];

const allowedOrigins = Array.from(
  new Set(configuredOrigins.flatMap(withWwwVariants)),
);

// CORS middleware
export const corsMiddleware = cors({
  origin: (origin: string | undefined, callback: (err: Error | null, allow?: boolean) => void) => {
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('CORS not allowed'));
    }
  },
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
  // FIX: 'Deriv-App-ID' exigido pela Deriv em chamadas REST.
  // FIX: 'x-cr' — usado por api.checkAdmin() no frontend.
  // FIX: 'x-local-authorization' — usado por fetchWithLocalAuth()
  // no frontend (sessão do login local: /api/auth/me e todas as
  // rotas /api/admin/users/*). Sem estar aqui, o browser bloqueia
  // o preflight CORS antes mesmo do pedido chegar ao backend
  // (erro visto: "Request header field x-local-authorization is
  // not allowed by Access-Control-Allow-Headers").
  allowedHeaders: [
    'Content-Type',
    'Authorization',
    'Deriv-App-ID',
    'x-cr',
    'x-local-authorization',
  ],
  maxAge: 86400,
});

// Rate limiting
const createRateLimiter = (windowMs: number, max: number) =>
  rateLimit({
    windowMs,
    max,
    message: 'Too many requests from this IP, please try again later.',
    standardHeaders: true,
    legacyHeaders: false,
    skip: (_req: Request) => config.server.isDevelopment,
    keyGenerator: (req: Request) => {
      return (req.headers['x-forwarded-for'] as string) || req.ip || 'unknown';
    },
    handler: (req: Request, res: Response) => {
      logger.warn('Rate limit exceeded', { ip: req.ip, path: req.path });
      const rl = (req as any).rateLimit;
      res.status(429).json({
        error: 'Too many requests',
        retryAfter: rl?.resetTime,
      });
    },
  });

export const apiLimiter = createRateLimiter(
  config.rateLimit.windowMs,
  config.rateLimit.maxRequests
);
export const authLimiter = createRateLimiter(
  15 * 60 * 1000,
  30
);
export const wsLimiter = createRateLimiter(
  1000,
  10
);

// Request logging middleware
export const requestLoggerMiddleware = (
  req: Request,
  res: Response,
  next: NextFunction
) => {
  const start = Date.now();
  res.on('finish', () => {
    const duration = Date.now() - start;
    logger.info('Request completed', {
      method: req.method,
      path: req.path,
      statusCode: res.statusCode,
      duration: `${duration}ms`,
      ip: req.ip,
    });
  });
  next();
};

export const sanitizationMiddleware: RequestHandler = express.json({
  limit: '10mb',
});
export const urlEncodedMiddleware: RequestHandler = express.urlencoded({
  extended: true,
  limit: '10mb',
});
