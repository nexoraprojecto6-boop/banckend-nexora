import 'express-async-errors';
import express, { Application, Request, Response, NextFunction } from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';
import { initRedis, closeRedis, isRedisConnected } from '@utils/redis.js';
import { destroyMemoryStore } from '@utils/memory-store.js';
import {
  helmetMiddleware,
  corsMiddleware,
  sanitizationMiddleware,
  urlEncodedMiddleware,
  requestLoggerMiddleware,
  apiLimiter,
} from '@middleware/security.js';
import { errorHandler, notFoundHandler } from '@middleware/errorHandler.js';
import authRoutes from '@routes/auth.routes.js';
import accountRoutes from '@routes/accounts.routes.js';
import tradingRoutes from '@routes/trading.routes.js';
import botsRoutes from '@routes/bots.routes.js';                    // ← NOVO
import { AuthService } from '@services/auth.service.js';
import { DerivAPIService } from '@services/deriv-api.service.js';
import { BotManager } from './bots/bot.manager.js';                 // ← NOVO
import { createDerivWsAdapter } from './bots/deriv-ws.adapter.js';  // ← NOVO
import { BotEvent } from './bots/bot.types.js';                     // ← NOVO

// ============================================
// App & HTTP Server
// ============================================
const app: Application = express();
const server = http.createServer(app);

// ============================================
// WebSocket Server — com lógica completa
// ============================================
const wss = new WebSocketServer({ server });

// Mapa de sessões WS activas: clientWs → { derivWs, accountId, token, botManager }
interface ClientSession {
  derivWs: WebSocket | null;
  token: string;
  accountId: string | null;
  pingInterval: NodeJS.Timeout | null;
  authenticated: boolean;
  botManager: BotManager | null;    // ← NOVO — um manager por sessão
}

const sessions = new Map<WebSocket, ClientSession>();

// ── Helpers ──────────────────────────────────────────────────────────────────

function sendToClient(ws: WebSocket, type: string, payload: Record<string, unknown>) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type, ...payload }));
  }
}

function sendError(ws: WebSocket, message: string, code = 'ERROR') {
  sendToClient(ws, 'error', { code, message });
}

/**
 * Liga o WS do cliente ao WS da Deriv (wsUrl com OTP já incluído)
 * e faz proxy bidirecional de mensagens.
 */
async function connectToDerivWS(
  clientWs: WebSocket,
  wsUrl: string,
  session: ClientSession
): Promise<void> {
  return new Promise((resolve, reject) => {
    logger.info('[WS] Connecting to Deriv WS', { url: wsUrl.replace(/otp=[^&]+/, 'otp=***') });

    const derivWs = new WebSocket(wsUrl);
    session.derivWs = derivWs;

    const timeout = setTimeout(() => {
      derivWs.terminate();
      reject(new Error('Deriv WS connection timeout'));
    }, 10_000);

    derivWs.on('open', () => {
      clearTimeout(timeout);
      logger.info('[WS] Connected to Deriv WS');
      resolve();
    });

    // Proxy mensagens Deriv → Frontend
    derivWs.on('message', (raw: Buffer) => {
      try {
        const msg = JSON.parse(raw.toString());

        if (msg.msg_type === 'balance') {
          sendToClient(clientWs, 'balance', {
            balance: msg.balance?.balance ?? 0,
            currency: msg.balance?.currency ?? 'USD',
            loginid: msg.balance?.loginid ?? '',
          });
        } else if (msg.msg_type === 'tick') {
          sendToClient(clientWs, 'tick', {
            quote: msg.tick?.quote,
            epoch: msg.tick?.epoch,
            symbol: msg.tick?.symbol,
          });
        } else if (msg.msg_type === 'transaction') {
          sendToClient(clientWs, 'transaction', {
            balance_after: msg.transaction?.balance_after,
            action: msg.transaction?.action,
            amount: msg.transaction?.amount,
            contract_id: msg.transaction?.contract_id,
          });
        } else if (msg.msg_type === 'buy') {
          sendToClient(clientWs, 'buy', {
            contract_id: msg.buy?.contract_id,
            balance_after: msg.buy?.balance_after,
            purchase_price: msg.buy?.buy_price,
          });
        } else if (msg.msg_type === 'proposal_open_contract') {
          const poc = msg.proposal_open_contract;
          sendToClient(clientWs, 'proposal_open_contract', {
            contract_id: poc?.contract_id,
            is_sold: poc?.is_sold,
            profit: poc?.profit,
            status: poc?.status,
            entry_tick: poc?.entry_tick,
            exit_tick: poc?.exit_tick,
            payout: poc?.payout,
          });
        } else if (msg.msg_type === 'ohlc') {
          sendToClient(clientWs, 'ohlc', { ohlc: msg.ohlc });
        } else if (msg.msg_type === 'history') {
          sendToClient(clientWs, 'history', { history: msg.history });
        } else if (msg.msg_type === 'pong') {
          // silencioso
        } else {
          sendToClient(clientWs, msg.msg_type ?? 'message', msg);
        }
      } catch (e) {
        logger.error('[WS] Failed to parse Deriv message', { error: e });
      }
    });

    derivWs.on('close', () => {
      logger.warn('[WS] Deriv WS disconnected');
      session.derivWs = null;
      if (clientWs.readyState === WebSocket.OPEN) {
        sendToClient(clientWs, 'deriv_disconnected', { message: 'Deriv WS disconnected' });
      }
    });

    derivWs.on('error', (err) => {
      logger.error('[WS] Deriv WS error', { error: err.message });
      clearTimeout(timeout);
      reject(err);
    });
  });
}

// ── NOVO: Cria o BotManager para a sessão e liga os eventos ──────────────────

function initBotManagerForSession(clientWs: WebSocket, session: ClientSession): void {
  if (!session.derivWs) return;

  // Adapter que usa o derivWs da sessão (sempre o mais recente)
  const adapter = createDerivWsAdapter(() => session.derivWs);
  const manager = new BotManager(adapter);

  // Todos os eventos do bot chegam aqui e são reencaminhados ao frontend via WS
  manager.on('bot_event', (event: BotEvent) => {
    sendToClient(clientWs, event.type, { botId: event.botId, ...event.payload });
  });

  session.botManager = manager;
  logger.info('[WS] BotManager inicializado para sessão');
}

/**
 * Autentica o cliente: valida o token, vai buscar contas, obtém OTP e liga ao WS Deriv.
 */
async function authenticateClient(ws: WebSocket, token: string, accountId?: string): Promise<void> {
  const session = sessions.get(ws);
  if (!session) return;

  try {
    // 1. Validar token obtendo contas
    const derivAPI = new DerivAPIService(token);
    const accountsRes = await derivAPI.getAccounts();
    const accounts: any[] = accountsRes.data || [];

    if (!accounts.length) {
      sendError(ws, 'No accounts found', 'NO_ACCOUNTS');
      ws.close();
      return;
    }

    // 2. Escolher conta activa
    const targetAccount = accountId
      ? accounts.find((a: any) => a.account_id === accountId) || accounts[0]
      : accounts[0];

    session.token = token;
    session.accountId = targetAccount.account_id;
    session.authenticated = true;

    // 3. Obter URL WS autenticada via OTP
    const otpData = await derivAPI.getOTP(targetAccount.account_id);
    const wsUrl: string = otpData?.url || otpData?.wsUrl;

    if (!wsUrl) {
      sendError(ws, 'Failed to get WebSocket URL', 'OTP_FAILED');
      ws.close();
      return;
    }

    // 4. Ligar ao WS da Deriv
    await connectToDerivWS(ws, wsUrl, session);

    // 5. Inicializar BotManager para esta sessão  ← NOVO
    initBotManagerForSession(ws, session);

    // 6. Confirmar autenticação ao cliente com dados das contas
    sendToClient(ws, 'authenticated', {
      accounts: accounts.map((a: any) => ({
        account_id: a.account_id,
        loginid: a.loginid || a.account_id,
        balance: a.balance ?? 0,
        currency: a.currency ?? 'USD',
        is_virtual: a.is_virtual ?? false,
        account_type: a.account_type ?? (a.is_virtual ? 'demo' : 'real'),
      })),
      currentAccount: {
        account_id: targetAccount.account_id,
        balance: targetAccount.balance ?? 0,
        currency: targetAccount.currency ?? 'USD',
        is_virtual: targetAccount.is_virtual ?? false,
        account_type: targetAccount.account_type ?? (targetAccount.is_virtual ? 'demo' : 'real'),
      },
    });

    // 7. Subscrever balance e transações automaticamente
    if (session.derivWs?.readyState === WebSocket.OPEN) {
      session.derivWs.send(JSON.stringify({ balance: 1, subscribe: 1 }));
      session.derivWs.send(JSON.stringify({ transaction: 1, subscribe: 1 }));
    }

    logger.info('[WS] Client authenticated', {
      accountId: targetAccount.account_id,
      isVirtual: targetAccount.is_virtual,
    });

  } catch (err: any) {
    logger.error('[WS] Authentication failed', { error: err.message });
    sendError(ws, err.message || 'Authentication failed', 'AUTH_FAILED');
    ws.close();
  }
}

// ── Lidar com ligações de clientes ────────────────────────────────────────────

wss.on('connection', (ws: WebSocket, req: http.IncomingMessage) => {
  logger.info('[WS] Client connected', { ip: req.socket.remoteAddress });

  const urlParams = new URLSearchParams(req.url?.split('?')[1] || '');
  const tokenFromUrl = urlParams.get('token');
  const accountIdFromUrl = urlParams.get('accountId') || undefined;

  const session: ClientSession = {
    derivWs: null,
    token: '',
    accountId: null,
    pingInterval: null,
    authenticated: false,
    botManager: null,             // ← NOVO
  };
  sessions.set(ws, session);

  session.pingInterval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) ws.ping();
  }, config.websocket.heartbeatInterval);

  if (tokenFromUrl) {
    authenticateClient(ws, tokenFromUrl, accountIdFromUrl);
  }

  ws.on('message', async (raw: Buffer) => {
    let data: Record<string, unknown>;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      sendError(ws, 'Invalid JSON', 'PARSE_ERROR');
      return;
    }

    const type = data.type as string | undefined;

    if (type === 'auth') {
      const token = data.token as string;
      const accountId = data.accountId as string | undefined;
      if (!token) { sendError(ws, 'Missing token', 'MISSING_TOKEN'); return; }
      await authenticateClient(ws, token, accountId);
      return;
    }

    if (type === 'switch_account') {
      if (!session.authenticated) { sendError(ws, 'Not authenticated', 'NOT_AUTH'); return; }
      const newAccountId = data.accountId as string;
      if (!newAccountId) { sendError(ws, 'Missing accountId', 'MISSING_ACCOUNT'); return; }

      // Parar bots antes de trocar conta  ← NOVO
      if (session.botManager) {
        await session.botManager.stopAll();
      }

      session.derivWs?.close();
      session.derivWs = null;
      await authenticateClient(ws, session.token, newAccountId);
      return;
    }

    if (!session.authenticated || !session.derivWs) {
      sendError(ws, 'Not authenticated', 'NOT_AUTH');
      return;
    }

    if (session.derivWs.readyState === WebSocket.OPEN) {
      const { type: _type, ...derivPayload } = data;
      session.derivWs.send(JSON.stringify(derivPayload));
    } else {
      sendError(ws, 'Deriv WebSocket not connected', 'DERIV_DISCONNECTED');
    }
  });

  ws.on('pong', () => { /* cliente está vivo */ });

  ws.on('close', () => {
    logger.info('[WS] Client disconnected');
    const s = sessions.get(ws);
    if (s) {
      s.botManager?.destroy();      // ← NOVO: destruir todos os bots da sessão
      s.derivWs?.close();
      if (s.pingInterval) clearInterval(s.pingInterval);
    }
    sessions.delete(ws);
  });

  ws.on('error', (err) => {
    logger.error('[WS] Client error', { error: err.message });
    const s = sessions.get(ws);
    if (s) {
      s.botManager?.destroy();      // ← NOVO
      s.derivWs?.close();
      if (s.pingInterval) clearInterval(s.pingInterval);
    }
    sessions.delete(ws);
  });
});

// ============================================
// Security Middleware
// ============================================
app.use(helmetMiddleware);
app.use(corsMiddleware);
app.use(sanitizationMiddleware);
app.use(urlEncodedMiddleware);

// ============================================
// Request Logging & Rate Limiting
// ============================================
app.use(requestLoggerMiddleware);
app.use(apiLimiter);

// ============================================
// Middleware: anexar BotManager ao req  ← NOVO
// ============================================
// As rotas REST precisam do BotManager da sessão WS do cliente.
// O token JWT é usado para localizar a sessão correcta no mapa.
// (requer que o frontend envie o mesmo token no header Authorization)

app.use('/api/bots', (req: Request, _res: Response, next: NextFunction) => {
  const authHeader = req.headers.authorization;
  const token = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : null;

  if (token) {
    // Procurar sessão WS autenticada com este token
    for (const [, session] of sessions) {
      if (session.token === token && session.botManager) {
        req.botManager = session.botManager;
        break;
      }
    }
  }
  next();
});

// ============================================
// Health Check
// ============================================
app.get('/health', (_req, res) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    environment: config.server.nodeEnv,
    redis: isRedisConnected() ? 'connected' : 'unavailable (using memory)',
    uptime: process.uptime(),
    wsClients: wss.clients.size,
  });
});

// ============================================
// API Routes
// ============================================
app.use('/api/auth', authRoutes);
app.use('/api/accounts', accountRoutes);
app.use('/api/trading', tradingRoutes);
app.use('/api/bots', botsRoutes);    // ← NOVO

// ============================================
// Error Handlers
// ============================================
app.use(notFoundHandler);
app.use(errorHandler);

// ============================================
// Server Startup
// ============================================
const startServer = async () => {
  try {
    logger.info('Initializing data store...');
    await initRedis();
    logger.info(`Data store ready (Redis: ${isRedisConnected() ? 'connected' : 'unavailable'})`);

    server.listen(config.server.port, config.server.host, () => {
      logger.info('Server started successfully', {
        host: config.server.host,
        port: config.server.port,
        environment: config.server.nodeEnv,
      });

      if (config.server.isDevelopment) {
        console.log(`
╔════════════════════════════════════════════════════════════╗
║         Deriv Trading Backend Server Started               ║
║                                                            ║
║  🚀 API:      http://${config.server.host}:${config.server.port}                      ║
║  📡 WS:       ws://${config.server.host}:${config.server.port}?token=JWT              ║
║  📚 Health:   http://${config.server.host}:${config.server.port}/health               ║
║  🔐 Auth:     http://${config.server.host}:${config.server.port}/api/auth             ║
║  💼 Accounts: http://${config.server.host}:${config.server.port}/api/accounts         ║
║  📈 Trading:  http://${config.server.host}:${config.server.port}/api/trading          ║
║  🤖 Bots:     http://${config.server.host}:${config.server.port}/api/bots             ║
║                                                            ║
║  WS Auth: ?token=XXX  ou  { type:"auth", token:"XXX" }    ║
╚════════════════════════════════════════════════════════════╝
        `);
      }
    });
  } catch (error) {
    logger.error('Failed to start server', {
      message: error instanceof Error ? error.message : 'Unknown error',
      stack: error instanceof Error ? error.stack : undefined,
    });
    process.exit(1);
  }
};

// ============================================
// Graceful Shutdown
// ============================================
const gracefulShutdown = async (signal: string) => {
  logger.info(`Received ${signal}, shutting down gracefully...`);

  sessions.forEach((session, clientWs) => {
    session.botManager?.destroy();  // ← NOVO
    session.derivWs?.close();
    if (session.pingInterval) clearInterval(session.pingInterval);
    clientWs.close();
  });
  sessions.clear();

  wss.close(() => logger.info('WebSocket server closed'));

  server.close(async () => {
    logger.info('HTTP server closed');
    try { await closeRedis(); } catch { /* ignorar */ }
    destroyMemoryStore();
    process.exit(0);
  });

  setTimeout(() => {
    logger.error('Forced shutdown');
    process.exit(1);
  }, 30_000);
};

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('uncaughtException', (error) => {
  logger.error('Uncaught Exception', { message: error.message, stack: error.stack });
  process.exit(1);
});
process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled Rejection', { reason });
});

startServer();
export default app;
