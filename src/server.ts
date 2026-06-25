// ============================================================
// NEXORA FOREX — Server
// ============================================================
// Correcções aplicadas vs versão anterior:
//   ✅ Reconexão automática ao Deriv WS com novo OTP obrigatório
//   ✅ Re-subscribe de balance/transaction após reconexão
//   ✅ Limite de sessões simultâneas (DoS protection)
//   ✅ req_id numérico nas subscrições automáticas
//   ✅ Ping do cliente respondido localmente (não vai para a Deriv)
//   ✅ Cleanup correcto do pingInterval e reconectTimer
//   ✅ Timeout de autenticação (evita sessões presas no handshake)
//   ✅ 1 WS por conta (padrão recomendado pela Deriv)
// ============================================================

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
import authRoutes     from '@routes/auth.routes.js';
import accountRoutes  from '@routes/accounts.routes.js';
import tradingRoutes  from '@routes/trading.routes.js';
import botsRoutes     from '@routes/bots.routes.js';
import adminRoutes    from '@routes/admin.routes.js';
import labelsRoutes   from '@routes/labels.routes.js';
import { DerivAPIService } from '@services/deriv-api.service.js';
import { BotManager }      from './bots/bot.manager.js';
import { BotCatalog }      from './bots/bot-catalog.js';
import { createDerivWsAdapter } from './bots/deriv-ws.adapter.js';
import { BotEvent, BotConfig, BotStrategyType } from './bots/bot.types.js';
import { seedBotCatalog } from './bots/catalog-seed.js';

// ─── Configurações de limite ──────────────────────────────────
const MAX_SESSIONS       = 100;   // máx de clientes simultâneos
const AUTH_TIMEOUT_MS    = 30_000; // 30s para autenticar após ligar
const RECONNECT_BASE_MS  = 2_000;  // backoff base para reconexão Deriv
const RECONNECT_MAX_MS   = 30_000; // tecto do backoff
const RECONNECT_MAX_TRIES = 8;    // tentativas antes de desistir

// ─── Admins ───────────────────────────────────────────────────
const ADMIN_IDS = new Set<string>(
  (process.env.ADMIN_ACCOUNT_IDS ?? '').split(',').map(s => s.trim()).filter(Boolean)
);

// ─── App & HTTP Server ────────────────────────────────────────
const app: Application = express();
const server = http.createServer(app);

// ─── WebSocket Server ─────────────────────────────────────────
const wss = new WebSocketServer({ server });

// ─── Sessão do cliente ────────────────────────────────────────
interface ClientSession {
  derivWs:          WebSocket | null;
  token:            string;
  accountId:        string | null;
  pingInterval:     NodeJS.Timeout | null;
  authTimeout:      NodeJS.Timeout | null;
  reconnectTimer:   NodeJS.Timeout | null;
  reconnectAttempts: number;
  authenticated:    boolean;
  isAdmin:          boolean;
  botManager:       BotManager | null;
  // req_id incremental por sessão (subscrições automáticas)
  nextReqId:        number;
}

const sessions = new Map<WebSocket, ClientSession>();

// ─── Helpers de envio ─────────────────────────────────────────

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

// ─── Subscrições automáticas após autenticação ────────────────

function subscribeAutomatic(derivWs: WebSocket, session: ClientSession): void {
  if (derivWs.readyState !== WebSocket.OPEN) return;

  // balance
  derivWs.send(JSON.stringify({
    balance:   1,
    subscribe: 1,
    req_id:    getNextReqId(session),
  }));

  // transaction
  derivWs.send(JSON.stringify({
    transaction: 1,
    subscribe:   1,
    req_id:      getNextReqId(session),
  }));

  logger.debug('[WS] Subscrições automáticas enviadas (balance, transaction)');
}

// ─── Proxy Deriv → Frontend ───────────────────────────────────

function setupDerivMessageHandler(
  clientWs: WebSocket,
  derivWs: WebSocket,
): void {
  derivWs.on('message', (raw: Buffer) => {
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(raw.toString('utf8'));
    } catch {
      logger.error('[WS] Mensagem Deriv não é JSON');
      return;
    }

    // Ping da Deriv: apenas ignorar (não reenviar pong — já tratado pelo WS)
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
          contract_id: poc?.contract_id,
          is_sold:     poc?.is_sold,
          profit:      poc?.profit,
          status:      poc?.status,
          entry_tick:  poc?.entry_tick,
          exit_tick:   poc?.exit_tick,
          payout:      poc?.payout,
        });
        break;
      }

      case 'ohlc':
        sendToClient(clientWs, 'ohlc', { ohlc: msg.ohlc });
        break;

      case 'history':
        sendToClient(clientWs, 'history', { history: msg.history });
        break;

      default:
        sendToClient(clientWs, (msg.msg_type as string) ?? 'message', msg);
    }
  });
}

// ─── Conectar ao Deriv WS ─────────────────────────────────────

async function connectToDerivWS(
  clientWs: WebSocket,
  wsUrl: string,
  session: ClientSession,
): Promise<void> {
  return new Promise((resolve, reject) => {
    logger.info('[WS] Conectando ao Deriv WS', {
      url: wsUrl.replace(/otp=[^&]+/, 'otp=***'),
    });

    const derivWs = new WebSocket(wsUrl);
    session.derivWs = derivWs;

    const timeout = setTimeout(() => {
      derivWs.terminate();
      reject(new Error('Timeout de conexão ao Deriv WS (10s)'));
    }, 10_000);

    derivWs.once('open', () => {
      clearTimeout(timeout);
      logger.info('[WS] Conectado ao Deriv WS');
      setupDerivMessageHandler(clientWs, derivWs);

      // Subscrições automáticas com req_id correctos
      subscribeAutomatic(derivWs, session);

      resolve();
    });

    derivWs.once('error', (err) => {
      clearTimeout(timeout);
      logger.error('[WS] Erro ao conectar Deriv WS', { message: err.message });
      reject(err);
    });

    derivWs.once('close', (code, reason) => {
      session.derivWs = null;
      logger.warn('[WS] Deriv WS desconectado', { code, reason: reason?.toString() });

      if (clientWs.readyState === WebSocket.OPEN) {
        sendToClient(clientWs, 'deriv_disconnected', {
          message: 'Deriv WS desconectado. A reconectar...',
        });
      }

      // Reconexão automática com novo OTP
      scheduleDerivReconnect(clientWs, session);
    });
  });
}

// ─── Reconexão automática ao Deriv WS ────────────────────────
// OBRIGATÓRIO: novo OTP a cada reconexão (OTPs são de uso único)

function scheduleDerivReconnect(clientWs: WebSocket, session: ClientSession): void {
  // Não reconectar se o cliente já fechou ou atingiu o limite
  if (clientWs.readyState !== WebSocket.OPEN) return;
  if (!session.authenticated || !session.token || !session.accountId) return;
  if (session.reconnectAttempts >= RECONNECT_MAX_TRIES) {
    logger.error('[WS] Máximo de reconexões atingido, a fechar sessão');
    sendError(clientWs, 'Conexão Deriv perdida permanentemente. Por favor, faz login novamente.', 'DERIV_DISCONNECTED');
    clientWs.close();
    return;
  }

  // Backoff exponencial com tecto
  const base  = Math.min(RECONNECT_BASE_MS * Math.pow(2, session.reconnectAttempts), RECONNECT_MAX_MS);
  const jitter = Math.random() * 0.3 * base;
  const delay  = Math.floor(base + jitter);

  session.reconnectAttempts++;
  logger.info('[WS] Reconexão Deriv agendada', {
    attempt: session.reconnectAttempts,
    delayMs: delay,
    accountId: session.accountId,
  });

  session.reconnectTimer = setTimeout(async () => {
    if (clientWs.readyState !== WebSocket.OPEN) return;

    try {
      // Sempre gerar novo OTP — OTPs expiram e são de uso único
      const derivAPI = new DerivAPIService(session.token);
      const otpData  = await derivAPI.getOTP(session.accountId!);
      const wsUrl    = otpData.url;

      await connectToDerivWS(clientWs, wsUrl, session);

      // Reconectou com sucesso: resetar contador
      session.reconnectAttempts = 0;

      // Reinicializar BotManager (o WS mudou)
      if (session.botManager) {
        await session.botManager.stopAll();
        session.botManager.removeAllListeners();
      }
      initBotManagerForSession(clientWs, session);

      sendToClient(clientWs, 'deriv_reconnected', {
        message: 'Conexão Deriv restabelecida',
      });

      logger.info('[WS] Deriv WS reconectado com sucesso', { accountId: session.accountId });
    } catch (err: any) {
      logger.error('[WS] Falha na reconexão Deriv', { error: err.message });
      // Tentará novamente via evento 'close' do novo WS
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
    sendToClient(clientWs, event.type, {
      payload: { botId: event.botId, ...event.payload },
    });
  });

  session.botManager = manager;
  logger.info('[WS] BotManager inicializado');
}

// ─── Handler de mensagens de bots ─────────────────────────────

async function handleBotMessage(
  ws: WebSocket,
  session: ClientSession,
  type: string,
  payload: Record<string, unknown>,
): Promise<void> {
  // Catálogo é público (não precisa de autenticação)
  if (type === 'list_bots') {
    try {
      const bots = await BotCatalog.listActive();
      sendToClient(ws, 'bots_list', { payload: bots });
    } catch (err: any) {
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
      case 'list_session_bots': {
        sendToClient(ws, 'session_bots_list', { payload: manager.listBots() });
        break;
      }

      case 'start_catalog_bot': {
        const catalogBotId   = payload.catalogBotId as string;
        const sessionName    = payload.sessionName  as string | undefined;
        const configOverride = (payload.configOverride ?? {}) as Partial<BotConfig>;

        if (!catalogBotId) { sendError(ws, 'catalogBotId é obrigatório.', 'MISSING_PARAM'); break; }

        const catalogBot = await BotCatalog.getById(catalogBotId);
        if (!catalogBot?.isActive) { sendError(ws, 'Bot não encontrado no catálogo.', 'BOT_NOT_FOUND'); break; }

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
        sendToClient(ws, 'bot_created', { payload: { ...botState, catalogBotId } });
        break;
      }

      case 'stop_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId é obrigatório.', 'MISSING_PARAM'); break; }
        await manager.stopBot(botId);
        sendToClient(ws, 'bot_stopped', { payload: { botId } });
        break;
      }

      case 'pause_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId é obrigatório.', 'MISSING_PARAM'); break; }
        manager.pauseBot(botId);
        sendToClient(ws, 'bot_paused', { payload: { botId } });
        break;
      }

      case 'resume_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId é obrigatório.', 'MISSING_PARAM'); break; }
        await manager.resumeBot(botId);
        sendToClient(ws, 'bot_resumed', { payload: { botId } });
        break;
      }

      case 'delete_bot': {
        const botId = payload.botId as string;
        if (!botId) { sendError(ws, 'botId é obrigatório.', 'MISSING_PARAM'); break; }
        manager.deleteBot(botId);
        sendToClient(ws, 'bot_deleted', { payload: { botId } });
        break;
      }

      case 'get_bot_logs': {
        const botId = payload.botId as string;
        const limit = (payload.limit as number) ?? 100;
        if (!botId) { sendError(ws, 'botId é obrigatório.', 'MISSING_PARAM'); break; }
        const logs = manager.getBotLogs(botId, limit);
        sendToClient(ws, 'bot_logs', { payload: { botId, logs } });
        break;
      }

      case 'admin_add_catalog_bot': {
        if (!session.isAdmin) { sendError(ws, 'Acesso restrito.', 'FORBIDDEN'); break; }
        const dto = payload as {
          name: string; description: string;
          strategy: BotStrategyType; defaultConfig: BotConfig;
          tags?: string[]; isActive?: boolean;
        };
        if (!dto.name || !dto.strategy || !dto.defaultConfig?.symbol) {
          sendError(ws, 'name, strategy e defaultConfig.symbol são obrigatórios.', 'MISSING_PARAM');
          break;
        }
        const bot = await BotCatalog.create({
          ...dto, tags: dto.tags ?? [], isActive: dto.isActive ?? true,
          createdBy: session.accountId ?? 'admin',
        });
        sendToClient(ws, 'catalog_bot_added', { payload: bot });
        break;
      }

      case 'admin_remove_catalog_bot': {
        if (!session.isAdmin) { sendError(ws, 'Acesso restrito.', 'FORBIDDEN'); break; }
        const id = payload.id as string;
        if (!id) { sendError(ws, 'id é obrigatório.', 'MISSING_PARAM'); break; }
        const deleted = await BotCatalog.delete(id);
        if (!deleted) { sendError(ws, 'Bot não encontrado.', 'BOT_NOT_FOUND'); break; }
        sendToClient(ws, 'catalog_bot_removed', { payload: { id } });
        break;
      }

      case 'admin_update_catalog_bot': {
        if (!session.isAdmin) { sendError(ws, 'Acesso restrito.', 'FORBIDDEN'); break; }
        const { id, ...updates } = payload as { id: string; [k: string]: unknown };
        if (!id) { sendError(ws, 'id é obrigatório.', 'MISSING_PARAM'); break; }
        const updated = await BotCatalog.update(id, updates as any);
        if (!updated) { sendError(ws, 'Bot não encontrado.', 'BOT_NOT_FOUND'); break; }
        sendToClient(ws, 'catalog_bot_updated', { payload: updated });
        break;
      }

      default:
        sendError(ws, `Tipo desconhecido: ${type}`, 'UNKNOWN_BOT_MSG');
    }
  } catch (err: any) {
    logger.error('[WS] Erro no handler de bot', { type, error: err.message });
    sendError(ws, err.message ?? 'Erro interno no bot', 'BOT_ERROR');
  }
}

// ─── Autenticação do cliente ──────────────────────────────────

async function authenticateClient(
  ws: WebSocket,
  token: string,
  accountId?: string,
): Promise<void> {
  const session = sessions.get(ws);
  if (!session) return;

  // Limpar timeout de autenticação se existir
  if (session.authTimeout) {
    clearTimeout(session.authTimeout);
    session.authTimeout = null;
  }

  // Parar bots e WS anterior se estava autenticado (ex: re-auth ou switch)
  if (session.botManager) {
    await session.botManager.stopAll();
    session.botManager.removeAllListeners();
    session.botManager = null;
  }
  if (session.derivWs) {
    session.derivWs.removeAllListeners();
    session.derivWs.terminate();
    session.derivWs = null;
  }
  if (session.reconnectTimer) {
    clearTimeout(session.reconnectTimer);
    session.reconnectTimer = null;
  }

  try {
    // 1. Validar token e obter contas
    const derivAPI    = new DerivAPIService(token);
    const accountsRes = await derivAPI.getAccounts();
    const accounts: any[] = accountsRes.data ?? [];

    if (!accounts.length) {
      sendError(ws, 'Nenhuma conta encontrada', 'NO_ACCOUNTS');
      ws.close();
      return;
    }

    // 2. Escolher conta
    const target = accountId
      ? (accounts.find((a: any) => a.account_id === accountId) ?? accounts[0])
      : accounts[0];

    session.token         = token;
    session.accountId     = target.account_id;
    session.isAdmin       = ADMIN_IDS.has(target.account_id);
    session.authenticated = true;
    session.reconnectAttempts = 0;

    // 3. Obter URL WS com OTP
    const otpData = await derivAPI.getOTP(target.account_id);
    const wsUrl   = otpData.url;
    if (!wsUrl) {
      sendError(ws, 'Falha ao obter URL WebSocket', 'OTP_FAILED');
      ws.close();
      return;
    }

    // 4. Ligar ao WS Deriv
    await connectToDerivWS(ws, wsUrl, session);

    // 5. BotManager
    initBotManagerForSession(ws, session);

    // 6. Confirmar ao cliente
    sendToClient(ws, 'authenticated', {
      accounts: accounts.map((a: any) => ({
        account_id:   a.account_id,
        loginid:      a.loginid      ?? a.account_id,
        balance:      a.balance      ?? 0,
        currency:     a.currency     ?? 'USD',
        is_virtual:   a.is_virtual   ?? false,
        account_type: a.account_type ?? (a.is_virtual ? 'demo' : 'real'),
      })),
      currentAccount: {
        account_id:   target.account_id,
        balance:      target.balance      ?? 0,
        currency:     target.currency     ?? 'USD',
        is_virtual:   target.is_virtual   ?? false,
        account_type: target.account_type ?? (target.is_virtual ? 'demo' : 'real'),
      },
      isAdmin: session.isAdmin,
    });

    logger.info('[WS] Cliente autenticado', {
      accountId: target.account_id,
      isVirtual: target.is_virtual,
      isAdmin:   session.isAdmin,
    });
  } catch (err: any) {
    logger.error('[WS] Falha na autenticação', { error: err.message });
    sendError(ws, err.message ?? 'Falha na autenticação', 'AUTH_FAILED');
    ws.close();
  }
}

// ─── Tipos interceptados (não passam para a Deriv) ────────────

const BOT_MESSAGE_TYPES = new Set([
  'list_bots', 'list_session_bots', 'start_catalog_bot',
  'stop_bot', 'pause_bot', 'resume_bot', 'delete_bot', 'get_bot_logs',
  'admin_add_catalog_bot', 'admin_remove_catalog_bot', 'admin_update_catalog_bot',
]);

// ─── Handler de novas conexões ────────────────────────────────

wss.on('connection', (ws: WebSocket, req: http.IncomingMessage) => {
  // Limite de sessões (DoS protection)
  if (sessions.size >= MAX_SESSIONS) {
    logger.warn('[WS] Limite de sessões atingido, a recusar nova conexão');
    ws.close(1013, 'Servidor cheio. Tenta novamente mais tarde.');
    return;
  }

  const ip = req.socket.remoteAddress ?? 'unknown';
  logger.info('[WS] Cliente conectado', { ip, totalSessions: sessions.size + 1 });

  const session: ClientSession = {
    derivWs:           null,
    token:             '',
    accountId:         null,
    pingInterval:      null,
    authTimeout:       null,
    reconnectTimer:    null,
    reconnectAttempts: 0,
    authenticated:     false,
    isAdmin:           false,
    botManager:        null,
    nextReqId:         0,
  };
  sessions.set(ws, session);

  // Ping ao cliente a cada heartbeatInterval
  session.pingInterval = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) ws.ping();
  }, config.websocket.heartbeatInterval);

  // Timeout: fechar se não autenticar em 30s
  session.authTimeout = setTimeout(() => {
    if (!session.authenticated) {
      logger.warn('[WS] Timeout de autenticação, a fechar sessão', { ip });
      ws.close(1008, 'Timeout de autenticação');
    }
  }, AUTH_TIMEOUT_MS);

  // Autenticação via query string (token no URL)
  const urlParams    = new URLSearchParams(req.url?.split('?')[1] ?? '');
  const tokenFromUrl = urlParams.get('token');
  const acctFromUrl  = urlParams.get('accountId') ?? undefined;
  if (tokenFromUrl) {
    authenticateClient(ws, tokenFromUrl, acctFromUrl);
  }

  // ── Mensagens do cliente ────────────────────────────────────
  ws.on('message', async (raw: Buffer) => {
    let data: Record<string, unknown>;
    try {
      data = JSON.parse(raw.toString('utf8'));
    } catch {
      sendError(ws, 'JSON inválido', 'PARSE_ERROR');
      return;
    }

    const type    = data.type as string | undefined;
    const payload = (data.payload ?? {}) as Record<string, unknown>;

    // Auth
    if (type === 'auth') {
      const token     = (payload.token     ?? data.token)     as string;
      const accountId = (payload.accountId ?? data.accountId) as string | undefined;
      if (!token) { sendError(ws, 'Token em falta', 'MISSING_TOKEN'); return; }
      await authenticateClient(ws, token, accountId);
      return;
    }

    // Ping do cliente → responder localmente (NÃO passar para a Deriv)
    if (type === 'ping') {
      sendToClient(ws, 'pong');
      return;
    }

    // Switch de conta
    if (type === 'switch_account') {
      if (!session.authenticated) { sendError(ws, 'Não autenticado', 'NOT_AUTH'); return; }
      const newAccountId = (payload.accountId ?? data.accountId) as string;
      if (!newAccountId) { sendError(ws, 'accountId em falta', 'MISSING_ACCOUNT'); return; }
      await authenticateClient(ws, session.token, newAccountId);
      return;
    }

    // Mensagens de bots
    if (type && BOT_MESSAGE_TYPES.has(type)) {
      if (type !== 'list_bots' && !session.authenticated) {
        sendError(ws, 'Não autenticado', 'NOT_AUTH');
        return;
      }
      await handleBotMessage(ws, session, type, payload);
      return;
    }

    // Proxy → Deriv (tudo o resto)
    if (!session.authenticated || !session.derivWs) {
      sendError(ws, 'Não autenticado', 'NOT_AUTH');
      return;
    }
    if (session.derivWs.readyState === WebSocket.OPEN) {
      const { type: _t, payload: _p, ...rest } = data;
      const derivPayload = (_p && Object.keys(_p as object).length > 0) ? _p : rest;
      session.derivWs.send(JSON.stringify(derivPayload));
    } else {
      sendError(ws, 'Deriv WS não conectado', 'DERIV_DISCONNECTED');
    }
  });

  ws.on('pong', () => { /* cliente está vivo */ });

  ws.on('close', () => {
    logger.info('[WS] Cliente desconectado', { ip });
    cleanupSession(ws);
  });

  ws.on('error', (err) => {
    logger.error('[WS] Erro de cliente', { ip, error: err.message });
    cleanupSession(ws);
  });
});

// ─── Cleanup de sessão ────────────────────────────────────────

async function cleanupSession(ws: WebSocket): Promise<void> {
  const session = sessions.get(ws);
  if (!session) return;

  sessions.delete(ws);

  if (session.pingInterval)   clearInterval(session.pingInterval);
  if (session.authTimeout)    clearTimeout(session.authTimeout);
  if (session.reconnectTimer) clearTimeout(session.reconnectTimer);

  if (session.botManager) {
    try { await session.botManager.stopAll(); } catch { /* ignorar */ }
    session.botManager.removeAllListeners();
  }

  if (session.derivWs) {
    session.derivWs.removeAllListeners();
    session.derivWs.terminate();
  }
}

// ============================================
// Security Middleware
// ============================================
app.use(helmetMiddleware);
app.use(corsMiddleware);
app.use(sanitizationMiddleware);
app.use(urlEncodedMiddleware);
app.use(requestLoggerMiddleware);
app.use(apiLimiter);

// ─── Injectar BotManager e isAdmin no req (rotas REST /api/bots) ─
app.use('/api/bots', (req: Request, _res: Response, next: NextFunction) => {
  const token = req.headers.authorization?.startsWith('Bearer ')
    ? req.headers.authorization.slice(7)
    : null;

  if (token) {
    for (const [, s] of sessions) {
      if (s.token === token) {
        if (s.botManager) req.botManager = s.botManager;
        req.isAdmin = s.isAdmin;
        break;
      }
    }
  }
  next();
});

// ─── Online Count ─────────────────────────────────────────────
app.get('/api/online-count', (_req, res) => {
  let authenticated = 0;
  for (const [, s] of sessions) {
    if (s.authenticated) authenticated++;
  }
  res.json({ count: authenticated, totalConnections: wss.clients.size });
});

// ─── Health Check ─────────────────────────────────────────────
app.get('/health', (_req, res) => {
  res.json({
    status:      'ok',
    timestamp:   new Date().toISOString(),
    environment: config.server.nodeEnv,
    redis:       isRedisConnected() ? 'connected' : 'unavailable',
    uptime:      process.uptime(),
    wsClients:   wss.clients.size,
    wsSessions:  sessions.size,
    wsAuthenticated: [...sessions.values()].filter(s => s.authenticated).length,
  });
});

// ─── API Routes ───────────────────────────────────────────────
app.use('/api/auth',          authRoutes);
app.use('/api/accounts',      accountRoutes);
app.use('/api/trading',       tradingRoutes);
app.use('/api/bots',          botsRoutes);
app.use('/api/admin',         adminRoutes);
app.use('/api/admin/labels',  labelsRoutes);

// ─── Error Handlers ───────────────────────────────────────────
app.use(notFoundHandler);
app.use(errorHandler);

// ============================================
// Startup
// ============================================
const startServer = async () => {
  try {
    await initRedis();
    logger.info(`Redis: ${isRedisConnected() ? 'conectado' : 'indisponível (usando memória)'}`);

    await seedBotCatalog();

    server.listen(config.server.port, config.server.host, () => {
      logger.info('Servidor iniciado', {
        host:        config.server.host,
        port:        config.server.port,
        environment: config.server.nodeEnv,
        maxSessions: MAX_SESSIONS,
      });

      if (config.server.isDevelopment) {
        console.log(`
╔══════════════════════════════════════════════════════════════╗
║          NEXORA FOREX — Backend Server Started               ║
║                                                              ║
║  🚀 API:    http://${config.server.host}:${config.server.port}                          ║
║  📡 WS:     ws://${config.server.host}:${config.server.port}?token=JWT                  ║
║  📚 Health: http://${config.server.host}:${config.server.port}/health                   ║
║  👥 Online: http://${config.server.host}:${config.server.port}/api/online-count         ║
║                                                              ║
║  Max sessões: ${MAX_SESSIONS}  |  Reconexão: backoff até ${RECONNECT_MAX_MS / 1000}s        ║
╚══════════════════════════════════════════════════════════════╝
        `);
      }
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
  logger.info(`Sinal ${signal} recebido, a encerrar...`);

  // Parar todos os bots e fechar sessões
  for (const [clientWs] of sessions) {
    await cleanupSession(clientWs);
    clientWs.close();
  }

  wss.close(() => logger.info('WebSocket server fechado'));

  server.close(async () => {
    logger.info('HTTP server fechado');
    try { await closeRedis(); } catch { /* ignorar */ }
    destroyMemoryStore();
    process.exit(0);
  });

  setTimeout(() => { logger.error('Encerramento forçado'); process.exit(1); }, 30_000);
};

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT',  () => gracefulShutdown('SIGINT'));
process.on('uncaughtException', (error) => {
  logger.error('Excepção não capturada', { message: error.message, stack: error.stack });
  process.exit(1);
});
process.on('unhandledRejection', (reason) => {
  logger.error('Promise rejeitada não tratada', { reason });
});

startServer();
export default app;
