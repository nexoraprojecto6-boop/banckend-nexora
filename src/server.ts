// ============================================================
// NEXORA FOREX — Server
// ============================================================
// CORRECÇÕES DESTA VERSÃO:
//   ✅ setupDerivMessageHandler: remove listener 'message' antigo
//      antes de adicionar novo — evita acumulação de listeners em
//      cada reconexão (cada reconexão criava um novo handler mas
//      nunca removia o anterior, causando mensagens duplicadas e
//      fuga de memória)
//   ✅ Deduplicação de sessões por accountId
//   ✅ Reconexão Deriv com novo OTP obrigatório
//   ✅ Bot aguarda WS disponível (waitForDerivWs)
//   ✅ Limite de sessões simultâneas (DoS protection)
//   ✅ Timeout de autenticação (30s)
//   ✅ Cleanup completo em todos os caminhos
//   ✅ NOVO: trust proxy + cookie-parser — corrige "Cookie PKCE
//      não encontrado" no fluxo OAuth entre Vercel (frontend) e
//      Render (backend, atrás de proxy reverso)
// ============================================================

import 'express-async-errors';
import express, { Application, Request, Response, NextFunction } from 'express';
import http from 'http';
import cookieParser from 'cookie-parser';
import { WebSocketServer, WebSocket } from 'ws';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';
import { initRedis, closeRedis, isRedisConnected } from '@utils/redis.js';
import { destroyMemoryStore } from '@utils/memory-store.js';
import {
  helmetMiddleware, corsMiddleware, sanitizationMiddleware,
  urlEncodedMiddleware, requestLoggerMiddleware, apiLimiter,
} from '@middleware/security.js';
import { errorHandler, notFoundHandler } from '@middleware/errorHandler.js';
import authRoutes    from '@routes/auth.routes.js';
import accountRoutes from '@routes/accounts.routes.js';
import tradingRoutes from '@routes/trading.routes.js';
import botsRoutes    from '@routes/bots.routes.js';
import adminRoutes   from '@routes/admin.routes.js';
import labelsRoutes  from '@routes/labels.routes.js';
import { DerivAPIService } from '@services/deriv-api.service.js';
import { BotManager }      from './bots/bot.manager.js';
import { BotCatalog }      from './bots/bot-catalog.js';
import { createDerivWsAdapter } from './bots/deriv-ws.adapter.js';
import { BotEvent, BotConfig, BotStrategyType } from './bots/bot.types.js';
import { seedBotCatalog } from './bots/catalog-seed.js';

// ─── Limites ──────────────────────────────────────────────────
const MAX_SESSIONS        = 100;
const AUTH_TIMEOUT_MS     = 30_000;
const RECONNECT_BASE_MS   = 3_000;
const RECONNECT_MAX_MS    = 30_000;
const RECONNECT_MAX_TRIES = 8;
const WS_WAIT_TIMEOUT_MS  = 20_000;

// ─── Admins ───────────────────────────────────────────────────
const ADMIN_IDS = new Set<string>(
  (process.env.ADMIN_ACCOUNT_IDS ?? '').split(',').map(s => s.trim()).filter(Boolean)
);

// ─── App & HTTP Server ────────────────────────────────────────
const app: Application = express();

// CRÍTICO: trust proxy ANTES de qualquer middleware que dependa
// de detecção correta de HTTPS (cookies secure, req.protocol,
// req.ip, etc.). O Render (como o Railway) fica atrás de um
// proxy reverso — sem isto, o Express não confia no header
// X-Forwarded-Proto e pode tratar a ligação como HTTP mesmo
// sendo HTTPS real, fazendo cookies com `secure: true` falharem
// silenciosamente. Isto era a causa de "Cookie PKCE não
// encontrado" no fluxo de login.
app.set('trust proxy', 1);

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ─── Sessão ───────────────────────────────────────────────────
interface ClientSession {
  derivWs:              WebSocket | null;
  derivMessageHandler:  ((raw: Buffer) => void) | null;
  token:                string;
  accountId:            string | null;
  pingInterval:         NodeJS.Timeout | null;
  authTimeout:          NodeJS.Timeout | null;
  reconnectTimer:       NodeJS.Timeout | null;
  reconnectAttempts:    number;
  authenticated:        boolean;
  isAdmin:              boolean;
  botManager:           BotManager | null;
  nextReqId:            number;
}

const sessions = new Map<WebSocket, ClientSession>();
const sessionsByAccount = new Map<string, WebSocket>();

// ─── Helpers ──────────────────────────────────────────────────

function sendToClient(ws: WebSocket, type: string, payload: Record<string, unknown> = {}): void {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type, ...payload }));
  }
}

function sendError(ws: WebSocket, message: string, code = 'ERROR'): void {
  sendToClient(ws, 'error', { payload: { code, message } });
}

function getNextReqId(session: ClientSession): number {
  return ++session.nextReqId;
}

// ─── Aguardar WS disponível ───────────────────────────────────

function waitForDerivWs(
  session: ClientSession,
  timeoutMs = WS_WAIT_TIMEOUT_MS,
): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    if (session.derivWs?.readyState === WebSocket.OPEN) {
      return resolve(session.derivWs);
    }

    const deadline = setTimeout(() => {
      clearInterval(poll);
      reject(new Error('Timeout: Deriv WS não ficou disponível a tempo'));
    }, timeoutMs);

    const poll = setInterval(() => {
      if (session.derivWs?.readyState === WebSocket.OPEN) {
        clearTimeout(deadline);
        clearInterval(poll);
        resolve(session.derivWs!);
      }
    }, 500);
  });
}

// ─── Subscrições automáticas ──────────────────────────────────

function subscribeAutomatic(derivWs: WebSocket, session: ClientSession): void {
  if (derivWs.readyState !== WebSocket.OPEN) return;
  derivWs.send(JSON.stringify({ balance: 1, subscribe: 1, req_id: getNextReqId(session) }));
  derivWs.send(JSON.stringify({ transaction: 1, subscribe: 1, req_id: getNextReqId(session) }));
  logger.debug('[WS] Subscrições automáticas enviadas');
}

// ─── Proxy Deriv → Frontend ───────────────────────────────────

function buildDerivMessageHandler(
  clientWs: WebSocket,
): (raw: Buffer) => void {
  return (raw: Buffer) => {
    let msg: Record<string, unknown>;
    try { msg = JSON.parse(raw.toString('utf8')); } catch { return; }

    if (msg.msg_type === 'ping' || msg.msg_type === 'pong') return;

    switch (msg.msg_type as string) {
      case 'balance':
        sendToClient(clientWs, 'balance', {
          balance:  (msg.balance as any)?.balance  ?? 0,
          currency: (msg.balance as any)?.currency ?? 'USD',
          loginid:  (msg.balance as any)?.loginid  ?? '',
        });
        break;
      case 'tick':
        sendToClient(clientWs, 'tick', {
          quote:  (msg.tick as any)?.quote,
          epoch:  (msg.tick as any)?.epoch,
          symbol: (msg.tick as any)?.symbol,
        });
        break;
      case 'transaction':
        sendToClient(clientWs, 'transaction', {
          balance_after: (msg.transaction as any)?.balance_after,
          action:        (msg.transaction as any)?.action,
          amount:        (msg.transaction as any)?.amount,
          contract_id:   (msg.transaction as any)?.contract_id,
        });
        break;
      case 'buy':
        sendToClient(clientWs, 'buy', {
          contract_id:    (msg.buy as any)?.contract_id,
          balance_after:  (msg.buy as any)?.balance_after,
          purchase_price: (msg.buy as any)?.buy_price,
        });
        break;
      case 'proposal_open_contract': {
        const poc = msg.proposal_open_contract as any;
        sendToClient(clientWs, 'proposal_open_contract', {
          contract_id: poc?.contract_id, is_sold: poc?.is_sold,
          profit: poc?.profit, status: poc?.status,
          entry_tick: poc?.entry_tick, exit_tick: poc?.exit_tick, payout: poc?.payout,
        });
        break;
      }
      case 'ohlc':    sendToClient(clientWs, 'ohlc',    { ohlc:    msg.ohlc    }); break;
      case 'history': sendToClient(clientWs, 'history', { history: msg.history }); break;
      default: sendToClient(clientWs, (msg.msg_type as string) ?? 'message', msg);
    }
  };
}

// ─── Conectar ao Deriv WS ─────────────────────────────────────

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
      reject(new Error('Timeout de conexão Deriv WS (10s)'));
    }, 10_000);

    derivWs.once('open', () => {
      clearTimeout(timeout);
      logger.info('[WS] Connected to Deriv WS');

      if (session.derivMessageHandler) {
        derivWs.removeListener('message', session.derivMessageHandler);
      }
      const handler = buildDerivMessageHandler(clientWs);
      session.derivMessageHandler = handler;
      derivWs.on('message', handler);

      subscribeAutomatic(derivWs, session);
      resolve();
    });

    derivWs.once('error', (err) => {
      clearTimeout(timeout);
      logger.error('[WS] Deriv WS error on connect', { message: err.message });
      reject(err);
    });

    derivWs.once('close', (code, reason) => {
      if (session.derivMessageHandler) {
        derivWs.removeListener('message', session.derivMessageHandler);
        session.derivMessageHandler = null;
      }
      session.derivWs = null;
      logger.warn('[WS] Deriv WS disconnected', { code, reason: reason?.toString() });
      if (clientWs.readyState === WebSocket.OPEN) {
        sendToClient(clientWs, 'deriv_disconnected', { message: 'Deriv WS desconectado. A reconectar...' });
      }
      scheduleDerivReconnect(clientWs, session);
    });
  });
}

// ─── Reconexão Deriv com novo OTP ────────────────────────────

function scheduleDerivReconnect(clientWs: WebSocket, session: ClientSession): void {
  if (clientWs.readyState !== WebSocket.OPEN) return;
  if (!session.authenticated || !session.token || !session.accountId) return;
  if (session.reconnectAttempts >= RECONNECT_MAX_TRIES) {
    logger.error('[WS] Máximo de reconexões atingido');
    sendError(clientWs, 'Conexão Deriv perdida. Faz login novamente.', 'DERIV_DISCONNECTED');
    clientWs.close();
    return;
  }

  const base  = Math.min(RECONNECT_BASE_MS * Math.pow(2, session.reconnectAttempts), RECONNECT_MAX_MS);
  const delay = Math.floor(base + Math.random() * 0.3 * base);
  session.reconnectAttempts++;

  logger.info('[WS] Reconexão Deriv agendada', {
    attempt: session.reconnectAttempts, delayMs: delay, accountId: session.accountId,
  });

  session.reconnectTimer = setTimeout(async () => {
    if (clientWs.readyState !== WebSocket.OPEN) return;
    try {
      const derivAPI = new DerivAPIService(session.token);
      const otpData  = await derivAPI.getOTP(session.accountId!);
      await connectToDerivWS(clientWs, otpData.url, session);
      session.reconnectAttempts = 0;

      if (session.botManager) {
        await session.botManager.stopAll();
        session.botManager.removeAllListeners();
      }
      initBotManagerForSession(clientWs, session);
      sendToClient(clientWs, 'deriv_reconnected', { message: 'Conexão Deriv restabelecida' });
    } catch (err: any) {
      logger.error('[WS] Falha na reconexão Deriv', { error: err.message });
      scheduleDerivReconnect(clientWs, session);
    }
  }, delay);
}

// ─── BotManager ───────────────────────────────────────────────

function initBotManagerForSession(clientWs: WebSocket, session: ClientSession): void {
  if (!session.derivWs) return;

  const adapter = createDerivWsAdapter(() => session.derivWs);

  const manager = new BotManager(adapter);
  manager.on('bot_event', (event: BotEvent) => {
    sendToClient(clientWs, event.type, { payload: { botId: event.botId, ...event.payload } });
  });

  session.botManager = manager;
  logger.info('[WS] BotManager inicializado para sessão');
}

// ─── Handler de mensagens de bots ─────────────────────────────

async function handleBotMessage(
  ws: WebSocket, session: ClientSession,
  type: string, payload: Record<string, unknown>,
): Promise<void> {
  if (type === 'list_bots') {
    try {
      sendToClient(ws, 'bots_list', { payload: await BotCatalog.listActive() });
    } catch (err: any) {
      sendError(ws, err.message ?? 'Erro ao listar catálogo', 'BOT_ERROR');
    }
    return;
  }

  const manager = session.botManager;
  if (!manager) { sendError(ws, 'BotManager não disponível.', 'NO_MANAGER'); return; }

  try {
    switch (type) {
      case 'list_session_bots':
        sendToClient(ws, 'session_bots_list', { payload: manager.listBots() }); break;

      case 'start_catalog_bot': {
        const catalogBotId   = payload.catalogBotId as string;
        const configOverride = (payload.configOverride ?? {}) as Partial<BotConfig>;
        if (!catalogBotId) { sendError(ws, 'catalogBotId obrigatório.', 'MISSING_PARAM'); break; }

        const catalogBot = await BotCatalog.getById(catalogBotId);
        if (!catalogBot?.isActive) { sendError(ws, 'Bot não encontrado.', 'BOT_NOT_FOUND'); break; }

        try {
          await waitForDerivWs(session);
        } catch {
          sendError(ws, 'Conexão Deriv não disponível. Aguarda reconexão.', 'DERIV_DISCONNECTED');
          break;
        }

        const finalConfig: BotConfig = {
          ...catalogBot.defaultConfig, ...configOverride,
          strategyParams: {
            ...(catalogBot.defaultConfig.strategyParams ?? {}),
            ...((configOverride.strategyParams as Record<string, unknown>) ?? {}),
          },
        };

        const created = manager.createBot({
          name: (payload.sessionName as string) ?? catalogBot.name,
          strategy: catalogBot.strategy, config: finalConfig,
        });
        await manager.startBot(created.id);
        sendToClient(ws, 'bot_created', { payload: { ...manager.getBotState(created.id), catalogBotId } });
        break;
      }

      case 'stop_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId obrigatório.', 'MISSING_PARAM'); break; }
        await manager.stopBot(botId);
        sendToClient(ws, 'bot_stopped', { payload: { botId } }); break;
      }

      case 'pause_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId obrigatório.', 'MISSING_PARAM'); break; }
        manager.pauseBot(botId);
        sendToClient(ws, 'bot_paused', { payload: { botId } }); break;
      }

      case 'resume_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId obrigatório.', 'MISSING_PARAM'); break; }
        try { await waitForDerivWs(session); } catch {
          sendError(ws, 'Conexão Deriv não disponível.', 'DERIV_DISCONNECTED'); break;
        }
        await manager.resumeBot(botId);
        sendToClient(ws, 'bot_resumed', { payload: { botId } }); break;
      }

      case 'delete_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId obrigatório.', 'MISSING_PARAM'); break; }
        manager.deleteBot(botId);
        sendToClient(ws, 'bot_deleted', { payload: { botId } }); break;
      }

      case 'get_bot_logs': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId obrigatório.', 'MISSING_PARAM'); break; }
        sendToClient(ws, 'bot_logs', { payload: { botId, logs: manager.getBotLogs(botId, (payload.limit as number) ?? 100) } }); break;
      }

      case 'admin_add_catalog_bot': {
        if (!session.isAdmin) { sendError(ws, 'Acesso restrito.', 'FORBIDDEN'); break; }
        const dto = payload as { name: string; description: string; strategy: BotStrategyType; defaultConfig: BotConfig; tags?: string[]; isActive?: boolean; };
        if (!dto.name || !dto.strategy || !dto.defaultConfig?.symbol) {
          sendError(ws, 'name, strategy e defaultConfig.symbol obrigatórios.', 'MISSING_PARAM'); break;
        }
        const bot = await BotCatalog.create({ ...dto, tags: dto.tags ?? [], isActive: dto.isActive ?? true, createdBy: session.accountId ?? 'admin' });
        sendToClient(ws, 'catalog_bot_added', { payload: bot }); break;
      }

      case 'admin_remove_catalog_bot': {
        if (!session.isAdmin) { sendError(ws, 'Acesso restrito.', 'FORBIDDEN'); break; }
        const id = payload.id as string;
        if (!id) { sendError(ws, 'id obrigatório.', 'MISSING_PARAM'); break; }
        if (!await BotCatalog.delete(id)) { sendError(ws, 'Bot não encontrado.', 'BOT_NOT_FOUND'); break; }
        sendToClient(ws, 'catalog_bot_removed', { payload: { id } }); break;
      }

      case 'admin_update_catalog_bot': {
        if (!session.isAdmin) { sendError(ws, 'Acesso restrito.', 'FORBIDDEN'); break; }
        const { id, ...updates } = payload as { id: string; [k: string]: unknown };
        if (!id) { sendError(ws, 'id obrigatório.', 'MISSING_PARAM'); break; }
        const updated = await BotCatalog.update(id, updates as any);
        if (!updated) { sendError(ws, 'Bot não encontrado.', 'BOT_NOT_FOUND'); break; }
        sendToClient(ws, 'catalog_bot_updated', { payload: updated }); break;
      }

      default: sendError(ws, `Tipo desconhecido: ${type}`, 'UNKNOWN_BOT_MSG');
    }
  } catch (err: any) {
    logger.error('[WS] Erro no handler de bot', { type, error: err.message });
    sendError(ws, err.message ?? 'Erro interno', 'BOT_ERROR');
  }
}

// ─── Autenticação ─────────────────────────────────────────────

async function authenticateClient(ws: WebSocket, token: string, accountId?: string): Promise<void> {
  const session = sessions.get(ws);
  if (!session) return;

  if (session.authTimeout) { clearTimeout(session.authTimeout); session.authTimeout = null; }

  if (session.botManager) {
    await session.botManager.stopAll();
    session.botManager.removeAllListeners();
    session.botManager = null;
  }
  if (session.derivWs) {
    if (session.derivMessageHandler) {
      session.derivWs.removeListener('message', session.derivMessageHandler);
      session.derivMessageHandler = null;
    }
    session.derivWs.removeAllListeners();
    session.derivWs.terminate();
    session.derivWs = null;
  }
  if (session.reconnectTimer) { clearTimeout(session.reconnectTimer); session.reconnectTimer = null; }

  try {
    const derivAPI    = new DerivAPIService(token);
    const accountsRes = await derivAPI.getAccounts();
    const accounts: any[] = accountsRes.data ?? [];

    if (!accounts.length) { sendError(ws, 'Nenhuma conta encontrada', 'NO_ACCOUNTS'); ws.close(); return; }

    const target = accountId
      ? (accounts.find((a: any) => a.account_id === accountId) ?? accounts[0])
      : accounts[0];

    const targetAccountId = target.account_id;

    const existingWs = sessionsByAccount.get(targetAccountId);
    if (existingWs && existingWs !== ws) {
      logger.warn('[WS] Sessão duplicada detectada, a fechar a antiga', { accountId: targetAccountId });
      await cleanupSession(existingWs);
      existingWs.close(1001, 'Sessão substituída por nova conexão');
    }

    session.token         = token;
    session.accountId     = targetAccountId;
    session.isAdmin       = ADMIN_IDS.has(targetAccountId);
    session.authenticated = true;
    session.reconnectAttempts = 0;

    sessionsByAccount.set(targetAccountId, ws);

    const otpData = await derivAPI.getOTP(targetAccountId);
    if (!otpData.url) { sendError(ws, 'Falha ao obter URL WS', 'OTP_FAILED'); ws.close(); return; }

    await connectToDerivWS(ws, otpData.url, session);
    initBotManagerForSession(ws, session);

    sendToClient(ws, 'authenticated', {
      accounts: accounts.map((a: any) => ({
        account_id:   a.account_id,
        loginid:      a.loginid ?? a.account_id,
        balance:      a.balance ?? 0,
        currency:     a.currency ?? 'USD',
        is_virtual:   a.is_virtual ?? false,
        account_type: a.account_type ?? (a.is_virtual ? 'demo' : 'real'),
      })),
      currentAccount: {
        account_id:   target.account_id,
        balance:      target.balance ?? 0,
        currency:     target.currency ?? 'USD',
        is_virtual:   target.is_virtual ?? false,
        account_type: target.account_type ?? (target.is_virtual ? 'demo' : 'real'),
      },
      isAdmin: session.isAdmin,
    });

    logger.info('[WS] Client authenticated', { accountId: targetAccountId, isAdmin: session.isAdmin });
  } catch (err: any) {
    logger.error('[WS] Authentication failed', { error: err.message });
    sendError(ws, err.message ?? 'Falha na autenticação', 'AUTH_FAILED');
    ws.close();
  }
}

// ─── Cleanup de sessão ────────────────────────────────────────

async function cleanupSession(ws: WebSocket): Promise<void> {
  const session = sessions.get(ws);
  if (!session) return;
  sessions.delete(ws);

  if (session.accountId && sessionsByAccount.get(session.accountId) === ws) {
    sessionsByAccount.delete(session.accountId);
  }

  if (session.pingInterval)   clearInterval(session.pingInterval);
  if (session.authTimeout)    clearTimeout(session.authTimeout);
  if (session.reconnectTimer) clearTimeout(session.reconnectTimer);

  if (session.botManager) {
    try { await session.botManager.stopAll(); } catch { /* ignorar */ }
    session.botManager.removeAllListeners();
  }

  if (session.derivWs) {
    if (session.derivMessageHandler) {
      session.derivWs.removeListener('message', session.derivMessageHandler);
      session.derivMessageHandler = null;
    }
    session.derivWs.removeAllListeners();
    session.derivWs.terminate();
  }
}

// ─── Tipos de mensagem de bots ────────────────────────────────
const BOT_MESSAGE_TYPES = new Set([
  'list_bots', 'list_session_bots', 'start_catalog_bot',
  'stop_bot', 'pause_bot', 'resume_bot', 'delete_bot', 'get_bot_logs',
  'admin_add_catalog_bot', 'admin_remove_catalog_bot', 'admin_update_catalog_bot',
]);

// ─── Novas conexões ───────────────────────────────────────────

wss.on('connection', (ws: WebSocket, req: http.IncomingMessage) => {
  if (sessions.size >= MAX_SESSIONS) {
    logger.warn('[WS] Limite de sessões atingido');
    ws.close(1013, 'Servidor cheio. Tenta novamente.');
    return;
  }

  const ip = req.socket.remoteAddress ?? 'unknown';
  logger.info('[WS] Client connected', { ip, sessions: sessions.size + 1 });

  const session: ClientSession = {
    derivWs: null, derivMessageHandler: null,
    token: '', accountId: null,
    pingInterval: null, authTimeout: null, reconnectTimer: null,
    reconnectAttempts: 0, authenticated: false, isAdmin: false,
    botManager: null, nextReqId: 1000,
  };
  sessions.set(ws, session);

  session.pingInterval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) ws.ping();
  }, config.websocket.heartbeatInterval);

  session.authTimeout = setTimeout(() => {
    if (!session.authenticated) {
      logger.warn('[WS] Auth timeout', { ip });
      ws.close(1008, 'Timeout de autenticação');
    }
  }, AUTH_TIMEOUT_MS);

  const urlParams = new URLSearchParams(req.url?.split('?')[1] ?? '');
  const tokenFromUrl = urlParams.get('token');
  if (tokenFromUrl) {
    authenticateClient(ws, tokenFromUrl, urlParams.get('accountId') ?? undefined);
  }

  ws.on('message', async (raw: Buffer) => {
    let data: Record<string, unknown>;
    try { data = JSON.parse(raw.toString('utf8')); } catch {
      sendError(ws, 'JSON inválido', 'PARSE_ERROR'); return;
    }

    const type    = data.type as string | undefined;
    const payload = (data.payload ?? {}) as Record<string, unknown>;

    if (type === 'auth') {
      const token = (payload.token ?? data.token) as string;
      if (!token) { sendError(ws, 'Token em falta', 'MISSING_TOKEN'); return; }
      await authenticateClient(ws, token, (payload.accountId ?? data.accountId) as string | undefined);
      return;
    }

    if (type === 'ping') { sendToClient(ws, 'pong'); return; }

    if (type === 'switch_account') {
      if (!session.authenticated) { sendError(ws, 'Não autenticado', 'NOT_AUTH'); return; }
      const newId = (payload.accountId ?? data.accountId) as string;
      if (!newId) { sendError(ws, 'accountId em falta', 'MISSING_ACCOUNT'); return; }
      await authenticateClient(ws, session.token, newId);
      return;
    }

    if (type && BOT_MESSAGE_TYPES.has(type)) {
      if (type !== 'list_bots' && !session.authenticated) {
        sendError(ws, 'Não autenticado', 'NOT_AUTH'); return;
      }
      await handleBotMessage(ws, session, type, payload);
      return;
    }

    if (!session.authenticated || !session.derivWs) {
      sendError(ws, 'Não autenticado', 'NOT_AUTH'); return;
    }
    if (session.derivWs.readyState === WebSocket.OPEN) {
      const { type: _t, payload: _p, ...rest } = data;
      const rawPayload = (_p && Object.keys(_p as object).length > 0)
        ? _p as Record<string, unknown>
        : rest as Record<string, unknown>;

      const existingReqId = rawPayload.req_id;
      const numericReqId = (typeof existingReqId === 'number' && Number.isInteger(existingReqId) && existingReqId > 0)
        ? existingReqId
        : getNextReqId(session);

      const derivPayload = { ...rawPayload, req_id: numericReqId };
      session.derivWs.send(JSON.stringify(derivPayload));
    } else {
      sendError(ws, 'Deriv WS não conectado', 'DERIV_DISCONNECTED');
    }
  });

  ws.on('pong', () => { /* vivo */ });

  ws.on('close', async () => {
    logger.info('[WS] Client disconnected', { ip });
    await cleanupSession(ws);
  });

  ws.on('error', async (err) => {
    logger.error('[WS] Client error', { ip, error: err.message });
    await cleanupSession(ws);
  });
});

// ============================================
// Middleware
// ============================================
app.use(helmetMiddleware);
app.use(corsMiddleware);
app.use(cookieParser());
app.use(sanitizationMiddleware);
app.use(urlEncodedMiddleware);
app.use(requestLoggerMiddleware);
app.use(apiLimiter);

app.use('/api/bots', (req: Request, _res: Response, next: NextFunction) => {
  const token = req.headers.authorization?.startsWith('Bearer ')
    ? req.headers.authorization.slice(7) : null;
  if (token) {
    for (const [, s] of sessions) {
      if (s.token === token) { if (s.botManager) req.botManager = s.botManager; req.isAdmin = s.isAdmin; break; }
    }
  }
  next();
});

app.get('/api/online-count', (_req, res) => {
  let authenticated = 0;
  for (const [, s] of sessions) { if (s.authenticated) authenticated++; }
  res.json({ count: authenticated, totalConnections: wss.clients.size });
});

app.get('/health', (_req, res) => {
  res.json({
    status: 'ok', timestamp: new Date().toISOString(),
    environment: config.server.nodeEnv,
    redis: isRedisConnected() ? 'connected' : 'unavailable (memory fallback)',
    uptime: process.uptime(), wsClients: wss.clients.size,
    wsSessions: sessions.size,
    wsAuthenticated: [...sessions.values()].filter(s => s.authenticated).length,
  });
});

app.use('/api/auth',         authRoutes);
app.use('/api/accounts',     accountRoutes);
app.use('/api/trading',      tradingRoutes);
app.use('/api/bots',         botsRoutes);
app.use('/api/admin',        adminRoutes);
app.use('/api/admin/labels', labelsRoutes);
app.use(notFoundHandler);
app.use(errorHandler);

// ============================================
// Startup
// ============================================
const startServer = async () => {
  try {
    await initRedis();
    logger.info(`Redis: ${isRedisConnected() ? 'conectado' : 'usando memória'}`);
    await seedBotCatalog();
    server.listen(config.server.port, config.server.host, () => {
      logger.info('Servidor iniciado', { port: config.server.port, maxSessions: MAX_SESSIONS });
    });
  } catch (error) {
    logger.error('Falha ao iniciar servidor', { error });
    process.exit(1);
  }
};

// ============================================
// Graceful Shutdown
// ============================================
const gracefulShutdown = async (signal: string) => {
  logger.info(`${signal} recebido, a encerrar...`);
  for (const [clientWs] of sessions) { await cleanupSession(clientWs); clientWs.close(); }
  wss.close(() => logger.info('WS server fechado'));
  server.close(async () => {
    try { await closeRedis(); } catch { /* ignorar */ }
    destroyMemoryStore();
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 30_000);
};

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT',  () => gracefulShutdown('SIGINT'));
process.on('uncaughtException',  (e) => { logger.error('Uncaught Exception',  { message: e.message, stack: e.stack }); process.exit(1); });
process.on('unhandledRejection', (r) => { logger.error('Unhandled Rejection', { reason: r }); });

startServer();
export default app;
