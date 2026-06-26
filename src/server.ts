import 'express-async-errors';
import express, { Application, Request, Response, NextFunction } from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';
import { initRedis, closeRedis, isRedisConnected } from '@utils/redis.js';
import { destroyMemoryStore } from '@utils/memory-store.js';
import { WebSocketManager } from '@websocket/manager.js';
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
import botsRoutes from '@routes/bots.routes.js';
import adminRoutes from '@routes/admin.routes.js';
import labelsRoutes from '@routes/labels.routes.js';
import { AuthService } from '@services/auth.service.js';
import { DerivAPIService } from '@services/deriv-api.service.js';
import { BotManager } from './bots/bot.manager.js';
import { BotCatalog } from './bots/bot-catalog.js';
import { createDerivWsAdapter } from './bots/deriv-ws.adapter.js';
import { BotEvent, BotConfig, BotStrategyType } from './bots/bot.types.js';
import { seedBotCatalog } from './bots/catalog-seed.js';

const ADMIN_IDS = new Set<string>(
  (process.env.ADMIN_ACCOUNT_IDS ?? '').split(',').map(s => s.trim()).filter(Boolean)
);

const app: Application = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

interface ClientSession {
  wsManager:     WebSocketManager | null;
  token:         string;
  accountId:     string | null;
  pingInterval:  NodeJS.Timeout | null;
  authenticated: boolean;
  isAdmin:       boolean;
  botManager:    BotManager | null;
}

const sessions = new Map<WebSocket, ClientSession>();

function sendToClient(ws: WebSocket, type: string, payload: Record<string, unknown> = {}): void {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type, ...payload }));
  }
}

function sendError(ws: WebSocket, message: string, code = 'ERROR'): void {
  sendToClient(ws, 'error', { payload: { code, message } });
}

async function fetchFreshDerivWsUrl(token: string, accountId: string): Promise<string> {
  const derivAPI = new DerivAPIService(token);
  const otpData = await derivAPI.getOTP(accountId);
  const wsUrl: string = otpData?.url || otpData?.wsUrl;
  if (!wsUrl) {
    throw new Error('Failed to get WebSocket URL (OTP)');
  }
  return wsUrl;
}

async function connectToDerivWS(
  clientWs: WebSocket,
  session: ClientSession,
): Promise<void> {
  if (!session.token || !session.accountId) {
    throw new Error('Sessao sem token/accountId');
  }

  const wsManager = new WebSocketManager({
    getUrl: () => fetchFreshDerivWsUrl(session.token, session.accountId as string),
    autoConnect: false,
    heartbeatInterval: config.websocket.heartbeatInterval,
    reconnectMaxAttempts: config.websocket.reconnectMaxAttempts,
    reconnectDelay: config.websocket.reconnectDelay,
  });

  session.wsManager = wsManager;

  wsManager.on('message', (msg: any) => {
    if (msg.msg_type === 'balance') {
      sendToClient(clientWs, 'balance', {
        balance:  msg.balance?.balance  ?? 0,
        currency: msg.balance?.currency ?? 'USD',
        loginid:  msg.balance?.loginid  ?? '',
      });
    } else if (msg.msg_type === 'tick') {
      sendToClient(clientWs, 'tick', {
        quote:  msg.tick?.quote,
        epoch:  msg.tick?.epoch,
        symbol: msg.tick?.symbol,
      });
    } else if (msg.msg_type === 'transaction') {
      sendToClient(clientWs, 'transaction', {
        balance_after: msg.transaction?.balance_after,
        action:        msg.transaction?.action,
        amount:        msg.transaction?.amount,
        contract_id:   msg.transaction?.contract_id,
      });
    } else if (msg.msg_type === 'buy') {
      sendToClient(clientWs, 'buy', {
        contract_id:    msg.buy?.contract_id,
        balance_after:  msg.buy?.balance_after,
        purchase_price: msg.buy?.buy_price,
      });
    } else if (msg.msg_type === 'proposal_open_contract') {
      const poc = msg.proposal_open_contract;
      sendToClient(clientWs, 'proposal_open_contract', {
        contract_id:  poc?.contract_id,
        is_sold:      poc?.is_sold,
        profit:       poc?.profit,
        status:       poc?.status,
        entry_tick:   poc?.entry_tick,
        exit_tick:    poc?.exit_tick,
        payout:       poc?.payout,
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
  });

  wsManager.on('disconnected', (info: { code?: number; reason?: string } = {}) => {
    logger.warn('[WS] Deriv WS disconnected', { accountId: session.accountId, code: info.code, reason: info.reason });
    if (clientWs.readyState === WebSocket.OPEN) {
      sendToClient(clientWs, 'deriv_disconnected', { message: 'Deriv WS disconnected', code: info.code, reason: info.reason });
    }
  });

  wsManager.on('reconnected', () => {
    logger.info('[WS] Deriv WS reconectado', { accountId: session.accountId });
    wsManager.send({ balance: 1, subscribe: 1 }).catch((err: any) =>
      logger.error('[WS] Falha ao restaurar balance', { error: err?.message }),
    );
    wsManager.send({ transaction: 1, subscribe: 1 }).catch((err: any) =>
      logger.error('[WS] Falha ao restaurar transaction', { error: err?.message }),
    );
    if (clientWs.readyState === WebSocket.OPEN) {
      sendToClient(clientWs, 'deriv_reconnected', {});
    }
  });

  wsManager.on('reconnect_failed', () => {
    logger.error('[WS] Deriv WS falhou reconexao', { accountId: session.accountId });
    if (clientWs.readyState === WebSocket.OPEN) {
      sendToClient(clientWs, 'deriv_disconnected', {
        message: 'Nao foi possivel restabelecer a ligacao a Deriv',
        permanent: true,
      });
    }
  });

  await wsManager.connect();
  logger.info('[WS] Connected to Deriv WS', { accountId: session.accountId });
}

function initBotManagerForSession(clientWs: WebSocket, session: ClientSession): void {
  if (!session.wsManager) return;

  const adapter = createDerivWsAdapter(() => session.wsManager);
  const manager = new BotManager(adapter);

  manager.on('bot_event', (event: BotEvent) => {
    sendToClient(clientWs, event.type, { payload: { botId: event.botId, ...event.payload } });
  });

  session.botManager = manager;
  logger.info('[WS] BotManager inicializado para sessao');
}

async function handleBotMessage(
  ws: WebSocket,
  session: ClientSession,
  type: string,
  payload: Record<string, unknown>,
): Promise<void> {
  if (type === 'list_bots') {
    try {
      const bots = await BotCatalog.listActive();
      sendToClient(ws, 'bots_list', { payload: bots });
    } catch (err: any) {
      logger.error('[WS] Erro ao listar catalogo de bots', { error: err.message });
      sendError(ws, err.message ?? 'Erro ao listar catalogo', 'BOT_ERROR');
    }
    return;
  }

  const manager = session.botManager;
  if (!manager) {
    sendError(ws, 'BotManager nao disponivel. Aguarda autenticacao.', 'NO_MANAGER');
    return;
  }

  try {
    switch (type) {
      case 'list_session_bots': {
        const bots = manager.listBots();
        sendToClient(ws, 'session_bots_list', { payload: bots });
        break;
      }

      case 'start_catalog_bot': {
        const catalogBotId   = payload.catalogBotId as string;
        const sessionName    = payload.sessionName  as string | undefined;
        const configOverride = (payload.configOverride ?? {}) as Partial<BotConfig>;

        if (!catalogBotId) {
          sendError(ws, 'catalogBotId e obrigatorio.', 'MISSING_PARAM');
          break;
        }

        const catalogBot = await BotCatalog.getById(catalogBotId);
        if (!catalogBot || !catalogBot.isActive) {
          sendError(ws, 'Bot nao encontrado no catalogo.', 'BOT_NOT_FOUND');
          break;
        }

        const finalConfig: BotConfig = {
          ...catalogBot.defaultConfig,
          ...configOverride,
          strategyParams: {
            ...(catalogBot.defaultConfig.strategyParams ?? {}),
            ...((configOverride.strategyParams as Record<string, unknown>) ?? {}),
          },
        };

        const created = manager.createBot({
          name:     sessionName ?? catalogBot.name,
          strategy: catalogBot.strategy,
          config:   finalConfig,
        });

        await manager.startBot(created.id);

        const botState = manager.getBotState(created.id);

        sendToClient(ws, 'bot_created', {
          payload: {
            ...botState,
            catalogBotId,
          },
        });
        break;
      }

      case 'stop_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId e obrigatorio.', 'MISSING_PARAM'); break; }
        await manager.stopBot(botId);
        sendToClient(ws, 'bot_stopped', { payload: { botId } });
        break;
      }

      case 'pause_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId e obrigatorio.', 'MISSING_PARAM'); break; }
        manager.pauseBot(botId);
        sendToClient(ws, 'bot_paused', { payload: { botId } });
        break;
      }

      case 'resume_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId e obrigatorio.', 'MISSING_PARAM'); break; }
        await manager.resumeBot(botId);
        sendToClient(ws, 'bot_resumed', { payload: { botId } });
        break;
      }

      case 'delete_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId e obrigatorio.', 'MISSING_PARAM'); break; }
        manager.deleteBot(botId);
        sendToClient(ws, 'bot_deleted', { payload: { botId } });
        break;
      }

      case 'get_bot_logs': {
        const botId = payload.botId as string;
        const limit = (payload.limit as number) ?? 100;
        if (!botId) { sendError(ws, 'botId e obrigatorio.', 'MISSING_PARAM'); break; }
        const logs = manager.getBotLogs(botId, limit);
        sendToClient(ws, 'bot_logs', { payload: { botId, logs } });
        break;
      }

      case 'admin_add_catalog_bot': {
        if (!session.isAdmin) {
          sendError(ws, 'Acesso restrito ao administrador.', 'FORBIDDEN');
          break;
        }
        const dto = payload as {
          name: string;
          description: string;
          strategy: BotStrategyType;
          defaultConfig: BotConfig;
          tags?: string[];
          isActive?: boolean;
        };
        if (!dto.name || !dto.strategy || !dto.defaultConfig?.symbol) {
          sendError(ws, 'name, strategy e defaultConfig.symbol sao obrigatorios.', 'MISSING_PARAM');
          break;
        }
        const bot = await BotCatalog.create({
          ...dto,
          tags: dto.tags ?? [],
          isActive: dto.isActive ?? true,
          createdBy: session.accountId ?? 'admin',
        });
        sendToClient(ws, 'catalog_bot_added', { payload: bot });
        logger.info(`[WS] Admin adicionou bot ao catalogo: ${bot.id}`);
        break;
      }

      case 'admin_remove_catalog_bot': {
        if (!session.isAdmin) {
          sendError(ws, 'Acesso restrito ao administrador.', 'FORBIDDEN');
          break;
        }
        const id = payload.id as string;
        if (!id) { sendError(ws, 'id e obrigatorio.', 'MISSING_PARAM'); break; }
        const deleted = await BotCatalog.delete(id);
        if (!deleted) {
          sendError(ws, 'Bot nao encontrado no catalogo.', 'BOT_NOT_FOUND');
          break;
        }
        sendToClient(ws, 'catalog_bot_removed', { payload: { id } });
        logger.info(`[WS] Admin removeu bot do catalogo: ${id}`);
        break;
      }

      case 'admin_update_catalog_bot': {
        if (!session.isAdmin) {
          sendError(ws, 'Acesso restrito ao administrador.', 'FORBIDDEN');
          break;
        }
        const { id, ...updates } = payload as { id: string; [key: string]: unknown };
        if (!id) { sendError(ws, 'id e obrigatorio.', 'MISSING_PARAM'); break; }
        const updated = await BotCatalog.update(id, updates as any);
        if (!updated) {
          sendError(ws, 'Bot nao encontrado no catalogo.', 'BOT_NOT_FOUND');
          break;
        }
        sendToClient(ws, 'catalog_bot_updated', { payload: updated });
        break;
      }

      default:
        sendError(ws, `Tipo de mensagem de bot desconhecido: ${type}`, 'UNKNOWN_BOT_MSG');
    }
  } catch (err: any) {
    logger.error('[WS] Erro ao processar mensagem de bot', { type, error: err.message });
    sendError(ws, err.message ?? 'Erro interno no bot', 'BOT_ERROR');
  }
}

async function authenticateClient(
  ws: WebSocket,
  token: string,
  accountId?: string,
): Promise<void> {
  const session = sessions.get(ws);
  if (!session) return;

  try {
    const derivAPI = new DerivAPIService(token);
    const accountsRes = await derivAPI.getAccounts();
    const accounts: any[] = accountsRes.data || [];

    if (!accounts.length) {
      sendError(ws, 'No accounts found', 'NO_ACCOUNTS');
      ws.close();
      return;
    }

    const targetAccount = accountId
      ? accounts.find((a: any) => a.account_id === accountId) || accounts[0]
      : accounts[0];

    session.token     = token;
    session.accountId = targetAccount.account_id;
    session.isAdmin   = ADMIN_IDS.has(targetAccount.account_id);
    session.authenticated = true;

    await connectToDerivWS(ws, session);
    initBotManagerForSession(ws, session);

    sendToClient(ws, 'authenticated', {
      accounts: accounts.map((a: any) => ({
        account_id:   a.account_id,
        loginid:      a.loginid || a.account_id,
        balance:      a.balance ?? 0,
        currency:     a.currency ?? 'USD',
        is_virtual:   a.is_virtual ?? false,
        account_type: a.account_type ?? (a.is_virtual ? 'demo' : 'real'),
      })),
      currentAccount: {
        account_id:   targetAccount.account_id,
        balance:      targetAccount.balance ?? 0,
        currency:     targetAccount.currency ?? 'USD',
        is_virtual:   targetAccount.is_virtual ?? false,
        account_type: targetAccount.account_type ?? (targetAccount.is_virtual ? 'demo' : 'real'),
      },
      isAdmin: session.isAdmin,
    });

    session.wsManager?.send({ balance: 1, subscribe: 1 }).catch((err: any) =>
      logger.error('[WS] Falha ao subscrever balance', { error: err?.message }),
    );
    session.wsManager?.send({ transaction: 1, subscribe: 1 }).catch((err: any) =>
      logger.error('[WS] Falha ao subscrever transaction', { error: err?.message }),
    );

    logger.info('[WS] Client authenticated', {
      accountId: targetAccount.account_id,
      isVirtual: targetAccount.is_virtual,
      isAdmin:   session.isAdmin,
    });
  } catch (err: any) {
    logger.error('[WS] Authentication failed', { error: err.message });
    sendError(ws, err.message || 'Authentication failed', 'AUTH_FAILED');
    ws.close();
  }
}

const BOT_MESSAGE_TYPES = new Set([
  'list_bots',
  'list_session_bots',
  'start_catalog_bot',
  'stop_bot',
  'pause_bot',
  'resume_bot',
  'delete_bot',
  'get_bot_logs',
  'admin_add_catalog_bot',
  'admin_remove_catalog_bot',
  'admin_update_catalog_bot',
]);

wss.on('connection', (ws: WebSocket, req: http.IncomingMessage) => {
  logger.info('[WS] Client connected', { ip: req.socket.remoteAddress });

  const urlParams       = new URLSearchParams(req.url?.split('?')[1] || '');
  const tokenFromUrl    = urlParams.get('token');
  const accountIdFromUrl = urlParams.get('accountId') || undefined;

  const session: ClientSession = {
    wsManager:     null,
    token:         '',
    accountId:     null,
    pingInterval:  null,
    authenticated: false,
    isAdmin:       false,
    botManager:    null,
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

    const type    = data.type    as string | undefined;
    const payload = (data.payload ?? {}) as Record<string, unknown>;

    if (type === 'auth') {
      const token     = (payload.token ?? data.token) as string;
      const accountId = (payload.accountId ?? data.accountId) as string | undefined;
      if (!token) { sendError(ws, 'Missing token', 'MISSING_TOKEN'); return; }
      await authenticateClient(ws, token, accountId);
      return;
    }

    if (type === 'ping') {
      sendToClient(ws, 'pong');
      return;
    }

    if (type === 'switch_account') {
      if (!session.authenticated) { sendError(ws, 'Not authenticated', 'NOT_AUTH'); return; }
      const newAccountId = (payload.accountId ?? data.accountId) as string;
      if (!newAccountId) { sendError(ws, 'Missing accountId', 'MISSING_ACCOUNT'); return; }

      if (session.botManager) {
        await session.botManager.stopAll();
        session.botManager.removeAllListeners();
        session.botManager = null;
      }

      session.wsManager?.disconnect();
      session.wsManager?.removeAllListeners();
      session.wsManager = null;
      await authenticateClient(ws, session.token, newAccountId);
      return;
    }

    if (type && BOT_MESSAGE_TYPES.has(type)) {
      if (type !== 'list_bots' && !session.authenticated) {
        sendError(ws, 'Not authenticated', 'NOT_AUTH');
        return;
      }
      await handleBotMessage(ws, session, type, payload);
      return;
    }

    if (!session.authenticated || !session.wsManager) {
      sendError(ws, 'Not authenticated', 'NOT_AUTH');
      return;
    }

    const { type: _type, payload: _payload, ...rest } = data;
    const derivPayload = Object.keys(_payload as object ?? {}).length > 0
      ? _payload
      : rest;

    session.wsManager.send(derivPayload).catch((err: any) => {
      logger.error('[WS] Falha ao reencaminhar mensagem para a Deriv', { error: err?.message });
      sendError(ws, err?.message ?? 'Deriv WebSocket not available', 'DERIV_SEND_FAILED');
    });
  });

  ws.on('pong', () => { /* cliente esta vivo */ });

  ws.on('close', () => {
    logger.info('[WS] Client disconnected');
    const s = sessions.get(ws);
    if (s) {
      s.botManager?.destroy();
      s.wsManager?.disconnect();
      s.wsManager?.removeAllListeners();
      if (s.pingInterval) clearInterval(s.pingInterval);
    }
    sessions.delete(ws);
  });

  ws.on('error', (err) => {
    logger.error('[WS] Client error', { error: err.message });
    const s = sessions.get(ws);
    if (s) {
      s.botManager?.destroy();
      s.wsManager?.disconnect();
      s.wsManager?.removeAllListeners();
      if (s.pingInterval) clearInterval(s.pingInterval);
    }
    sessions.delete(ws);
  });
});

app.use(helmetMiddleware);
app.use(corsMiddleware);
app.use(sanitizationMiddleware);
app.use(urlEncodedMiddleware);
app.use(requestLoggerMiddleware);
app.use(apiLimiter);

app.use('/api/bots', (req: Request, _res: Response, next: NextFunction) => {
  const authHeader = req.headers.authorization;
  const token = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : null;

  if (token) {
    for (const [, session] of sessions) {
      if (session.token === token) {
        if (session.botManager) req.botManager = session.botManager;
        req.isAdmin = session.isAdmin;
        break;
      }
    }
  }
  next();
});

app.get('/api/online-count', (_req, res) => {
  let authenticated = 0;
  for (const [, session] of sessions) {
    if (session.authenticated) authenticated++;
  }
  res.json({
    count:          authenticated,
    totalConnections: wss.clients.size,
  });
});

app.get('/api/metrics', (_req, res) => {
  let activeBots = 0;
  let totalSubscriptions = 0;
  let totalPendingRequests = 0;
  let totalSendQueueDepth = 0;

  for (const [, session] of sessions) {
    if (session.botManager) {
      activeBots += session.botManager.listBots().filter(b => b.status === 'running').length;
    }
    if (session.wsManager) {
      const status = session.wsManager.getStatus();
      totalSubscriptions += status.subscriptions;
      totalPendingRequests += status.pendingRequests;
      totalSendQueueDepth += status.sendQueueDepth;
    }
  }

  const mem = process.memoryUsage();

  res.json({
    usersOnline: Array.from(sessions.values()).filter(s => s.authenticated).length,
    totalConnections: wss.clients.size,
    activeBots,
    totalSubscriptions,
    totalPendingRequests,
    totalSendQueueDepth,
    memory: {
      rssMB: Math.round(mem.rss / 1024 / 1024),
      heapUsedMB: Math.round(mem.heapUsed / 1024 / 1024),
      heapTotalMB: Math.round(mem.heapTotal / 1024 / 1024),
    },
    uptimeSeconds: Math.round(process.uptime()),
  });
});

app.get('/health', (_req, res) => {
  res.json({
    status:      'ok',
    timestamp:   new Date().toISOString(),
    environment: config.server.nodeEnv,
    redis:       isRedisConnected() ? 'connected' : 'unavailable (using memory)',
    uptime:      process.uptime(),
    wsClients:   wss.clients.size,
  });
});

app.use('/api/auth',     authRoutes);
app.use('/api/accounts', accountRoutes);
app.use('/api/trading',  tradingRoutes);
app.use('/api/bots',     botsRoutes);
app.use('/api/admin',    adminRoutes);
app.use('/api/admin/labels', labelsRoutes);

app.use(notFoundHandler);
app.use(errorHandler);

const startServer = async () => {
  try {
    logger.info('Initializing data store...');
    await initRedis();
    logger.info(`Data store ready (Redis: ${isRedisConnected() ? 'connected' : 'unavailable'})`);

    await seedBotCatalog();

    server.listen(config.server.port, config.server.host, () => {
      logger.info('Server started successfully', {
        host:        config.server.host,
        port:        config.server.port,
        environment: config.server.nodeEnv,
      });

      if (config.server.isDevelopment) {
        console.log('NEXORA FOREX backend started on port ' + config.server.port);
      }
    });
  } catch (error) {
    logger.error('Failed to start server', {
      message: error instanceof Error ? error.message : 'Unknown error',
      stack:   error instanceof Error ? error.stack  : undefined,
    });
    process.exit(1);
  }
};

const gracefulShutdown = async (signal: string) => {
  logger.info(`Received ${signal}, shutting down gracefully...`);

  for (const [clientWs, session] of sessions) {
    await session.botManager?.destroy();
    session.wsManager?.disconnect();
    session.wsManager?.removeAllListeners();
    if (session.pingInterval) clearInterval(session.pingInterval);
    clientWs.close();
  }
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
process.on('SIGINT',  () => gracefulShutdown('SIGINT'));
process.on('uncaughtException', (error) => {
  logger.error('Uncaught Exception', { message: error.message, stack: error.stack });
  process.exit(1);
});
process.on('unhandledRejection', (reason) => {
  logger.error('Unhandled Rejection', { reason });
});

startServer();
export default app;
