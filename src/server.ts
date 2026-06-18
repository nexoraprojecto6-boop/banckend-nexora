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
import botsRoutes from '@routes/bots.routes.js';
import { AuthService } from '@services/auth.service.js';
import { DerivAPIService } from '@services/deriv-api.service.js';
import { BotManager } from './bots/bot.manager.js';
import { BotCatalog } from './bots/bot-catalog.js';
import { createDerivWsAdapter } from './bots/deriv-ws.adapter.js';
import { BotEvent, BotConfig, BotStrategyType } from './bots/bot.types.js';
import { seedBotCatalog } from './bots/catalog-seed.js';

// ─── Identificar admins ───────────────────────────────────────
// Configura as contas admin via variável de ambiente:
//   ADMIN_ACCOUNT_IDS=DOT90000001,DOT90000002
const ADMIN_IDS = new Set<string>(
  (process.env.ADMIN_ACCOUNT_IDS ?? '').split(',').map(s => s.trim()).filter(Boolean)
);

// ============================================
// App & HTTP Server
// ============================================
const app: Application = express();
const server = http.createServer(app);

// ============================================
// WebSocket Server
// ============================================
const wss = new WebSocketServer({ server });

interface ClientSession {
  derivWs:       WebSocket | null;
  token:         string;
  accountId:     string | null;
  pingInterval:  NodeJS.Timeout | null;
  authenticated: boolean;
  isAdmin:       boolean;
  botManager:    BotManager | null;
}

const sessions = new Map<WebSocket, ClientSession>();

// ─── Helpers ─────────────────────────────────────────────────

function sendToClient(ws: WebSocket, type: string, payload: Record<string, unknown> = {}): void {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type, ...payload }));
  }
}

function sendError(ws: WebSocket, message: string, code = 'ERROR'): void {
  // O frontend lê msg.payload.message (ver use-nexora-ws.ts handleMessage).
  // sendToClient faz spread do 2º argumento na raiz da mensagem, por isso
  // aqui precisamos de aninhar explicitamente em "payload" para o formato
  // ficar { type: 'error', payload: { code, message } }.
  sendToClient(ws, 'error', { payload: { code, message } });
}

// ─── Proxy Deriv → Frontend ────────────────────────────────────

async function connectToDerivWS(
  clientWs: WebSocket,
  wsUrl: string,
  session: ClientSession,
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

    derivWs.on('message', (raw: Buffer) => {
      try {
        const msg = JSON.parse(raw.toString());

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

// ─── Inicializar BotManager para a sessão ─────────────────────

function initBotManagerForSession(clientWs: WebSocket, session: ClientSession): void {
  if (!session.derivWs) return;

  const adapter = createDerivWsAdapter(() => session.derivWs);
  const manager = new BotManager(adapter);

  // Reencaminhar todos os eventos do bot para o frontend
  // O frontend (use-nexora-ws.ts) lê msg.payload.botId e msg.payload.*,
  // por isso aninhamos tudo em "payload" — sendToClient faz spread do
  // 2º argumento na raiz, então passamos { payload: {...} } explicitamente.
  manager.on('bot_event', (event: BotEvent) => {
    sendToClient(clientWs, event.type, { payload: { botId: event.botId, ...event.payload } });
  });

  session.botManager = manager;
  logger.info('[WS] BotManager inicializado para sessão');
}

// ─── Handler das mensagens de bots (interceptadas antes do proxy Deriv) ───

async function handleBotMessage(
  ws: WebSocket,
  session: ClientSession,
  type: string,
  payload: Record<string, unknown>,
): Promise<void> {
  // ── Catálogo é público — não depende de autenticação nem do BotManager ──
  // O utilizador deve poder ver os bots disponíveis mesmo antes de
  // autenticar (ex: ao abrir a app, enquanto o login/OTP ainda decorre).
  if (type === 'list_bots') {
    try {
      const bots = await BotCatalog.listActive();
      sendToClient(ws, 'bots_list', { payload: bots });
    } catch (err: any) {
      logger.error('[WS] Erro ao listar catálogo de bots', { error: err.message });
      sendError(ws, err.message ?? 'Erro ao listar catálogo', 'BOT_ERROR');
    }
    return;
  }

  const manager = session.botManager;
  if (!manager) {
    sendError(ws, 'BotManager não disponível. Aguarda autenticação.', 'NO_MANAGER');
    return;
  }

  try {
    switch (type) {

      // ── Listar bots da sessão do utilizador ───────────────────
      case 'list_session_bots': {
        const bots = manager.listBots();
        sendToClient(ws, 'session_bots_list', { payload: bots });
        break;
      }

      // ── Iniciar bot a partir do catálogo ──────────────────────
      // O utilizador escolhe um bot do catálogo, define os seus
      // parâmetros personalizados (podem sobrepor defaultConfig),
      // e o backend cria uma instância na sessão.
      //
      // payload: {
      //   catalogBotId: string,    ← bot do catálogo
      //   sessionName:  string,    ← nome opcional para esta instância
      //   configOverride?: Partial<BotConfig>  ← parâmetros do utilizador
      // }
      case 'start_catalog_bot': {
        const catalogBotId   = payload.catalogBotId as string;
        const sessionName    = payload.sessionName  as string | undefined;
        const configOverride = (payload.configOverride ?? {}) as Partial<BotConfig>;

        if (!catalogBotId) {
          sendError(ws, 'catalogBotId é obrigatório.', 'MISSING_PARAM');
          break;
        }

        const catalogBot = await BotCatalog.getById(catalogBotId);
        if (!catalogBot || !catalogBot.isActive) {
          sendError(ws, 'Bot não encontrado no catálogo.', 'BOT_NOT_FOUND');
          break;
        }

        // Merge: defaultConfig do catálogo + overrides do utilizador
        const finalConfig: BotConfig = {
          ...catalogBot.defaultConfig,
          ...configOverride,
          // strategyParams faz merge profundo
          strategyParams: {
            ...(catalogBot.defaultConfig.strategyParams ?? {}),
            ...((configOverride.strategyParams as Record<string, unknown>) ?? {}),
          },
        };

        const botState = manager.createBot({
          name:     sessionName ?? catalogBot.name,
          strategy: catalogBot.strategy,
          config:   finalConfig,
        });

        // Inicia imediatamente após criar
        await manager.startBot(botState.id);

        sendToClient(ws, 'bot_created', {
          payload: {
            ...botState,
            catalogBotId,    // útil para o frontend correlacionar
          },
        });
        break;
      }

      // ── Parar bot ─────────────────────────────────────────────
      case 'stop_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId é obrigatório.', 'MISSING_PARAM'); break; }
        await manager.stopBot(botId);
        sendToClient(ws, 'bot_stopped', { payload: { botId } });
        break;
      }

      // ── Pausar bot ────────────────────────────────────────────
      case 'pause_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId é obrigatório.', 'MISSING_PARAM'); break; }
        manager.pauseBot(botId);
        sendToClient(ws, 'bot_paused', { payload: { botId } });
        break;
      }

      // ── Retomar bot ───────────────────────────────────────────
      case 'resume_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId é obrigatório.', 'MISSING_PARAM'); break; }
        await manager.resumeBot(botId);
        sendToClient(ws, 'bot_resumed', { payload: { botId } });
        break;
      }

      // ── Eliminar instância de bot da sessão ───────────────────
      case 'delete_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId é obrigatório.', 'MISSING_PARAM'); break; }
        manager.deleteBot(botId);
        sendToClient(ws, 'bot_deleted', { payload: { botId } });
        break;
      }

      // ── Logs de um bot ────────────────────────────────────────
      case 'get_bot_logs': {
        const botId = payload.botId as string;
        const limit = (payload.limit as number) ?? 100;
        if (!botId) { sendError(ws, 'botId é obrigatório.', 'MISSING_PARAM'); break; }
        const logs = manager.getBotLogs(botId, limit);
        sendToClient(ws, 'bot_logs', { payload: { botId, logs } });
        break;
      }

      // ── [ADMIN] Adicionar bot ao catálogo via WS ──────────────
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
          sendError(ws, 'name, strategy e defaultConfig.symbol são obrigatórios.', 'MISSING_PARAM');
          break;
        }
        const bot = await BotCatalog.create({
          ...dto,
          tags: dto.tags ?? [],
          isActive: dto.isActive ?? true,
          createdBy: session.accountId ?? 'admin',
        });
        sendToClient(ws, 'catalog_bot_added', { payload: bot });
        logger.info(`[WS] Admin adicionou bot ao catálogo: ${bot.id}`);
        break;
      }

      // ── [ADMIN] Remover bot do catálogo via WS ────────────────
      case 'admin_remove_catalog_bot': {
        if (!session.isAdmin) {
          sendError(ws, 'Acesso restrito ao administrador.', 'FORBIDDEN');
          break;
        }
        const id = payload.id as string;
        if (!id) { sendError(ws, 'id é obrigatório.', 'MISSING_PARAM'); break; }
        const deleted = await BotCatalog.delete(id);
        if (!deleted) {
          sendError(ws, 'Bot não encontrado no catálogo.', 'BOT_NOT_FOUND');
          break;
        }
        sendToClient(ws, 'catalog_bot_removed', { payload: { id } });
        logger.info(`[WS] Admin removeu bot do catálogo: ${id}`);
        break;
      }

      // ── [ADMIN] Actualizar bot do catálogo via WS ─────────────
      case 'admin_update_catalog_bot': {
        if (!session.isAdmin) {
          sendError(ws, 'Acesso restrito ao administrador.', 'FORBIDDEN');
          break;
        }
        const { id, ...updates } = payload as { id: string; [key: string]: unknown };
        if (!id) { sendError(ws, 'id é obrigatório.', 'MISSING_PARAM'); break; }
        const updated = await BotCatalog.update(id, updates as any);
        if (!updated) {
          sendError(ws, 'Bot não encontrado no catálogo.', 'BOT_NOT_FOUND');
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

// ─── Autenticar cliente ────────────────────────────────────────

async function authenticateClient(
  ws: WebSocket,
  token: string,
  accountId?: string,
): Promise<void> {
  const session = sessions.get(ws);
  if (!session) return;

  try {
    // 1. Validar token e obter contas
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

    session.token     = token;
    session.accountId = targetAccount.account_id;
    session.isAdmin   = ADMIN_IDS.has(targetAccount.account_id);
    session.authenticated = true;

    // 3. Obter URL WS autenticada
    const otpData = await derivAPI.getOTP(targetAccount.account_id);
    const wsUrl: string = otpData?.url || otpData?.wsUrl;

    if (!wsUrl) {
      sendError(ws, 'Failed to get WebSocket URL', 'OTP_FAILED');
      ws.close();
      return;
    }

    // 4. Ligar ao WS da Deriv
    await connectToDerivWS(ws, wsUrl, session);

    // 5. Inicializar BotManager
    initBotManagerForSession(ws, session);

    // 6. Confirmar autenticação ao cliente
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
      isAdmin: session.isAdmin,   // ← útil para o frontend mostrar/esconder UI de admin
    });

    // 7. Subscrever balance e transações automaticamente
    if (session.derivWs?.readyState === WebSocket.OPEN) {
      session.derivWs.send(JSON.stringify({ balance: 1, subscribe: 1 }));
      session.derivWs.send(JSON.stringify({ transaction: 1, subscribe: 1 }));
    }

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

// ─── Tipos de mensagem interceptadas (não passam para a Deriv) ─

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

// ─── Lidar com ligações de clientes ───────────────────────────

wss.on('connection', (ws: WebSocket, req: http.IncomingMessage) => {
  logger.info('[WS] Client connected', { ip: req.socket.remoteAddress });

  const urlParams       = new URLSearchParams(req.url?.split('?')[1] || '');
  const tokenFromUrl    = urlParams.get('token');
  const accountIdFromUrl = urlParams.get('accountId') || undefined;

  const session: ClientSession = {
    derivWs:       null,
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
    // O frontend envia { type, payload } — extrair payload correctamente
    const payload = (data.payload ?? {}) as Record<string, unknown>;

    // ── Auth ──────────────────────────────────────────────────
    if (type === 'auth') {
      const token     = (payload.token ?? data.token) as string;
      const accountId = (payload.accountId ?? data.accountId) as string | undefined;
      if (!token) { sendError(ws, 'Missing token', 'MISSING_TOKEN'); return; }
      await authenticateClient(ws, token, accountId);
      return;
    }

    // ── Ping do cliente ───────────────────────────────────────
    if (type === 'ping') {
      sendToClient(ws, 'pong');
      return;
    }

    // ── Switch de conta ───────────────────────────────────────
    if (type === 'switch_account') {
      if (!session.authenticated) { sendError(ws, 'Not authenticated', 'NOT_AUTH'); return; }
      const newAccountId = (payload.accountId ?? data.accountId) as string;
      if (!newAccountId) { sendError(ws, 'Missing accountId', 'MISSING_ACCOUNT'); return; }

      // Para todos os bots antes de trocar conta
      if (session.botManager) {
        await session.botManager.stopAll();
        session.botManager.removeAllListeners();
        session.botManager = null;
      }

      session.derivWs?.close();
      session.derivWs = null;
      await authenticateClient(ws, session.token, newAccountId);
      return;
    }

    // ── Mensagens de bots — INTERCEPTAR antes do proxy ────────
    if (type && BOT_MESSAGE_TYPES.has(type)) {
      // list_bots é público: o catálogo pode ser consultado mesmo
      // antes (ou durante) a autenticação do cliente.
      if (type !== 'list_bots' && !session.authenticated) {
        sendError(ws, 'Not authenticated', 'NOT_AUTH');
        return;
      }
      await handleBotMessage(ws, session, type, payload);
      return;   // ← NÃO passa para a Deriv
    }

    // ── Proxy para a Deriv (tudo o resto) ─────────────────────
    if (!session.authenticated || !session.derivWs) {
      sendError(ws, 'Not authenticated', 'NOT_AUTH');
      return;
    }

    if (session.derivWs.readyState === WebSocket.OPEN) {
      // Remove o campo "type" wrapper do Nexora, envia o payload nativo para a Deriv
      const { type: _type, payload: _payload, ...rest } = data;
      const derivPayload = Object.keys(_payload as object ?? {}).length > 0
        ? _payload
        : rest;
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
      s.botManager?.destroy();
      s.derivWs?.close();
      if (s.pingInterval) clearInterval(s.pingInterval);
    }
    sessions.delete(ws);
  });

  ws.on('error', (err) => {
    logger.error('[WS] Client error', { error: err.message });
    const s = sessions.get(ws);
    if (s) {
      s.botManager?.destroy();
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
// Middleware: anexar BotManager e isAdmin ao req
// ============================================
// O token JWT é usado para localizar a sessão WS activa do cliente.
// Isso permite às rotas REST aceder ao BotManager da sessão.

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

// ============================================
// Health Check
// ============================================
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

// ============================================
// API Routes
// ============================================
app.use('/api/auth',     authRoutes);
app.use('/api/accounts', accountRoutes);
app.use('/api/trading',  tradingRoutes);
app.use('/api/bots',     botsRoutes);

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

    await seedBotCatalog();

    server.listen(config.server.port, config.server.host, () => {
      logger.info('Server started successfully', {
        host:        config.server.host,
        port:        config.server.port,
        environment: config.server.nodeEnv,
      });

      if (config.server.isDevelopment) {
        console.log(`
╔══════════════════════════════════════════════════════════════╗
║          NEXORA FOREX — Backend Server Started               ║
║                                                              ║
║  🚀 API:      http://${config.server.host}:${config.server.port}                        ║
║  📡 WS:       ws://${config.server.host}:${config.server.port}?token=JWT                ║
║  📚 Health:   http://${config.server.host}:${config.server.port}/health                 ║
║  🔐 Auth:     http://${config.server.host}:${config.server.port}/api/auth               ║
║  💼 Accounts: http://${config.server.host}:${config.server.port}/api/accounts           ║
║  📈 Trading:  http://${config.server.host}:${config.server.port}/api/trading            ║
║  🤖 Bots:     http://${config.server.host}:${config.server.port}/api/bots               ║
║                                                              ║
║  WS Auth: ?token=XXX  ou  { type:"auth", payload:{token} }  ║
║                                                              ║
║  Fluxo de bots:                                              ║
║    list_bots → start_catalog_bot → stop_bot/pause_bot        ║
║    Admin: admin_add_catalog_bot / admin_remove_catalog_bot   ║
╚══════════════════════════════════════════════════════════════╝
        `);
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

// ============================================
// Graceful Shutdown
// ============================================
const gracefulShutdown = async (signal: string) => {
  logger.info(`Received ${signal}, shutting down gracefully...`);

  for (const [clientWs, session] of sessions) {
    await session.botManager?.destroy();
    session.derivWs?.close();
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
