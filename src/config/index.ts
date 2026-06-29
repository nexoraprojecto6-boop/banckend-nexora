import dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

const configSchema = z.object({
  server: z.object({
    port: z.number().default(3001),
    host: z.string().default('0.0.0.0'),
    nodeEnv: z.string().default('development'),
    isDevelopment: z.boolean().default(true),
    isProduction: z.boolean().default(false),
  }),
  deriv: z.object({
    apiBaseUrl: z.string().url().default('https://api.derivws.com'),
    wsPublicUrl: z.string().url().default('wss://api.derivws.com/trading/v1/options/ws/public'),
    wsAuthUrl: z.string().url().default('wss://api.derivws.com/trading/v1/options/ws'),
    authBaseUrl: z.string().url().default('https://auth.deriv.com/oauth2'),
    clientId: z.string().min(1, 'DERIV_CLIENT_ID is required'),
    clientSecret: z.string().min(1, 'DERIV_CLIENT_SECRET is required'),
    redirectUri: z.string().url().default('https://banckend-nexora.onrender.com/api/auth/callback'),
    appId: z.string().min(1, 'DERIV_APP_ID is required'),
  }),
  // ─── Afiliado ─────────────────────────────────────────────
  // Aplicados por defeito em todos os pedidos de signup
  // (prompt=registration), para atribuir novos registos à conta
  // de parceiro Deriv. O frontend pode sobrepor via query params
  // se precisar de uma campanha diferente; estes são só o fallback.
  affiliate: z.object({
    sidc: z.string().default(''),
    utmSource: z.string().default(''),
    utmMedium: z.string().default('affiliate'),
    utmCampaign: z.string().default('nexora'),
  }),
  frontend: z.object({
    url: z.string().url().default('http://localhost:3000'),
    prodUrl: z.string().url().default('https://yourdomain.com'),
  }),
  redis: z.object({
    url: z.string().default('redis://localhost:6379'),
    db: z.number().default(0),
    password: z.string().optional(),
  }),
  security: z.object({
    jwtSecret: z.string().min(1, 'JWT_SECRET is required'),
    corsOrigin: z.string().default('http://localhost:3000'),
  }),
  logging: z.object({
    level: z.string().default('info'),
    file: z.string().default('logs/app.log'),
  }),
  websocket: z.object({
    heartbeatInterval: z.number().default(30000),
    maxSubscriptions: z.number().default(100),
    reconnectMaxAttempts: z.number().default(5),
    reconnectDelay: z.number().default(1000),
  }),
  rateLimit: z.object({
    windowMs: z.number().default(900000),
    maxRequests: z.number().default(100),
  }),
});

const nodeEnv = process.env.NODE_ENV || 'development';
const isDev = nodeEnv !== 'production';

const envConfig = {
  server: {
    port: parseInt(process.env.PORT || '3001', 10),
    host: process.env.HOST || '0.0.0.0',
    nodeEnv,
    isDevelopment: isDev,
    isProduction: !isDev,
  },
  deriv: {
    apiBaseUrl: process.env.DERIV_API_BASE_URL || 'https://api.derivws.com',
    wsPublicUrl: process.env.DERIV_WS_PUBLIC_URL || 'wss://api.derivws.com/trading/v1/options/ws/public',
    wsAuthUrl: process.env.DERIV_WS_AUTH_URL || 'wss://api.derivws.com/trading/v1/options/ws',
    authBaseUrl: process.env.DERIV_AUTH_BASE_URL || 'https://auth.deriv.com/oauth2',
    clientId: process.env.DERIV_CLIENT_ID || '',
    clientSecret: process.env.DERIV_CLIENT_SECRET || '',
    redirectUri: process.env.DERIV_REDIRECT_URI || 'https://banckend-nexora.onrender.com/api/auth/callback',
    appId: process.env.DERIV_APP_ID || '',
  },
  // Valores por defeito já preenchidos com os dados confirmados.
  // Podem ser sobrepostos via env vars no Render sem precisar de rebuild.
  affiliate: {
    sidc: process.env.DERIV_AFFILIATE_SIDC || 'C52M9QNQNANN',
    utmSource: process.env.DERIV_AFFILIATE_UTM_SOURCE || '3224',
    utmMedium: process.env.DERIV_AFFILIATE_UTM_MEDIUM || 'affiliate',
    utmCampaign: process.env.DERIV_AFFILIATE_UTM_CAMPAIGN || 'nexora',
  },
  frontend: {
    url: process.env.FRONTEND_URL || 'http://localhost:3000',
    prodUrl: process.env.FRONTEND_PROD_URL || 'https://yourdomain.com',
  },
  redis: {
    // Usa REDIS_URL diretamente (Render injeta isso automaticamente, se usares Redis da Render)
    url: process.env.REDIS_URL || `redis://${process.env.REDIS_HOST || 'localhost'}:${process.env.REDIS_PORT || '6379'}`,
    db: parseInt(process.env.REDIS_DB || '0', 10),
    password: process.env.REDIS_PASSWORD,
  },
  security: {
    jwtSecret: process.env.JWT_SECRET || 'dev-secret-key',
    corsOrigin: process.env.CORS_ORIGIN || 'http://localhost:3000',
  },
  logging: {
    level: process.env.LOG_LEVEL || 'info',
    file: process.env.LOG_FILE || 'logs/app.log',
  },
  websocket: {
    heartbeatInterval: parseInt(process.env.WS_HEARTBEAT_INTERVAL || '30000', 10),
    maxSubscriptions: parseInt(process.env.WS_MAX_SUBSCRIPTIONS || '100', 10),
    reconnectMaxAttempts: parseInt(process.env.WS_RECONNECT_MAX_ATTEMPTS || '5', 10),
    reconnectDelay: parseInt(process.env.WS_RECONNECT_DELAY || '1000', 10),
  },
  rateLimit: {
    windowMs: parseInt(process.env.RATE_LIMIT_WINDOW_MS || '900000', 10),
    maxRequests: parseInt(process.env.RATE_LIMIT_MAX_REQUESTS || '100', 10),
  },
};

export const config = configSchema.parse(envConfig);

// Mantém exports diretos para compatibilidade
export const isDevelopment = config.server.isDevelopment;
export const isProduction = config.server.isProduction;
