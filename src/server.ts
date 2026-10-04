// ============================================================
// NEXORA FOREX — Server v2.1
// ============================================================
// Correcções vs v1 (além das já documentadas na versão anterior):
//   ✅ Tráfego dos bots já NÃO é reencaminhado ao frontend: o router
//      devolve se consumiu a mensagem (buy/proposal/poc/ticks dos bots
//      geravam 3–5 mensagens inúteis por trade e por tick)
//   ✅ Parse JSON único por mensagem Deriv (router.dispatch)
//   ✅ Os bots SOBREVIVEM a reconexões do WS Deriv (antes: stopAll() +
//      novo BotManager a cada queda → bots perdidos). O adapter volta a
//      subscrever ticks/contratos na nova ligação.
//   ✅ Keep-alive para a Deriv (ping a cada 30 s com deteção de ligação
//      morta). Antes só se fazia ping ao cliente.
//   ✅ Reconexão: 1ª tentativa em ~0,5 s (era 3 s); sem dupla contagem
//      quando a ligação falha antes de abrir
//   ✅ forget_all do frontend só cancela as subscrições DO FRONTEND
//      (antes matava os ticks dos bots, que partilham a ligação)
//   ✅ req_id do frontend < 1.000.000; o alocador interno usa >= 1.000.000
//      (um req_id pequeno do cliente podia resolver o pedido de um bot)
//   ✅ Operar Manual usa o adapter (buy directo, 1 viagem; dígito final
//      a partir de exit_spot com as casas decimais reais do símbolo —
//      lastDigitFromPrice usava toFixed(2): errado para R_10/R_50)
//   ✅ Autenticação não reentra (url token + msg auth em simultâneo
//      criavam dois WS Deriv, um deles órfão)
//   ✅ Cliente "morto" detectado (ping/pong com isAlive) → sessão e bots
//      libertados; limite de payload e de mensagens/s por cliente
//
// Novo na v2.1 (anti-429 da Deriv — "Unexpected server response: 429"):
//   ✅ FUGA de ligações corrigida: se o cliente saía a meio da
//      autenticação (reload/fechar separador), a ligação à Deriv abria
//      depois da limpeza e ficava órfã para sempre (ping incluído),
//      ocupando um dos 5 slots por utilizador. Agora é fechada.
//   ✅ Fecho limpo (close frame + espera pelo 'close', máx. 1,5 s) do WS
//      da Deriv em trocas de conta, sessão duplicada e limpeza. Antes:
//      terminate() (TCP destruído sem close frame) e a nova ligação
//      abria logo a seguir, com o slot ainda ocupado do lado da Deriv.
//   ✅ Lock por conta: sessão duplicada + OTP + handshake correm UM de
//      cada vez por accountId (antes: rajada de pedidos em paralelo).
//   ✅ Espaçamento mínimo de 1,5 s entre handshakes da mesma conta.
//   ✅ Retry NO SERVIDOR (até 4 tentativas, OTP novo em cada uma, backoff
//      com jitter; em 429 o dobro). Antes: 1 falha → AUTH_TEMPORARY →
//      ws.close() → o frontend reconectava e repetia o ciclo todo.
//   ✅ Reconexão da Deriv respeita o lock/espaçamento e não corre em
//      paralelo com uma autenticação; em 429 espera ≥ 5 s.
//   ✅ Pedido de auth durante outra auth já não é descartado: guarda o
//      último e corre-o a seguir (trocas de conta demo ↔ real).
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
import authRoutes              from '@routes/auth.routes.js';
import accountRoutes           from '@routes/accounts.routes.js';
import tradingRoutes           from '@routes/trading.routes.js';
import botsRoutes              from '@routes/bots.routes.js';
import adminRoutes             from '@routes/admin.routes.js';
import labelsRoutes            from '@routes/labels.routes.js';
import adminUsersRoutes        from '@routes/admin-users.routes.js';
import notificationsRoutes     from '@routes/notifications.routes.js';
import adminNotificationsRoutes from '@routes/admin-notifications.routes.js';
import { DerivAPIService, DerivTokenExpiredError, isHandshakeRateLimited } from '@services/deriv-api.service.js';
import { BotManager }      from './bots/bot.manager.js';
import { BotCatalog }      from './bots/bot-catalog.js';
import { createDerivWsAdapter } from './bots/deriv-ws.adapter.js';
import { BotEvent, BotConfig, BotStrategyType, DerivWsAdapter } from './bots/bot.types.js';
import type { SymbolResolution } from './bots/strategies/base.strategy.js';
import { seedBotCatalog } from './bots/catalog-seed.js';
import { attachExternalRouter, destroyMessageRouter, getMessageRouter } from './bots/message-router.js';
import { classifyDerivError } from '@utils/deriv-errors.js';
import { stakeSanityError } from '@utils/currency.js';
import { nextReqId } from '@utils/req-id.js';

// ─── Limites ──────────────────────────────────────────────────
const MAX_SESSIONS         = 100;
const AUTH_TIMEOUT_MS      = 30_000;
const RECONNECT_BASE_MS    = 500;
const RECONNECT_MAX_MS     = 30_000;
const RECONNECT_MAX_TRIES  = 8;
const WS_WAIT_TIMEOUT_MS   = 20_000;
const DERIV_PING_MS        = 30_000;   // Deriv: ping a cada 30 s
const DERIV_PING_TIMEOUT   = 10_000;
const CLIENT_MAX_PAYLOAD   = 1_000_000;
const CLIENT_MSGS_PER_SEC  = 60;
const CLIENT_REQ_ID_LIMIT  = 1_000_000; // req_id do cliente < 1M; internos >= 1M
const MAX_CLIENT_SUBS      = 200;

// ─── Ligação à Deriv (anti-429) ───────────────────────────────
const UPSTREAM_CLOSE_WAIT_MS        = 1_500; // espera pelo close frame antes de terminate()
const UPSTREAM_MIN_GAP_MS           = 1_500; // mínimo entre handshakes da mesma conta
const UPSTREAM_CONNECT_TRIES        = 4;     // tentativas por autenticação
const UPSTREAM_RETRY_BASE_MS        = 1_500;
const UPSTREAM_RETRY_MAX_MS         = 8_000;
const UPSTREAM_RATE_LIMIT_FLOOR_MS  = 5_000; // reconexão após 429: espera mínima

// ─── Admins ───────────────────────────────────────────────────
const ADMIN_IDS = new Set<string>(
  (process.env.ADMIN_ACCOUNT_IDS ?? '').split(',').map(s => s.trim()).filter(Boolean)
);

// ─── App & HTTP Server ────────────────────────────────────────
const app: Application = express();
app.set('trust proxy', 1);

const server = http.createServer(app);
const wss = new WebSocketServer({
  server,
  maxPayload: CLIENT_MAX_PAYLOAD,
  perMessageDeflate: false,
});

// ─── Sessão ───────────────────────────────────────────────────
interface ClientSession {
  derivWs:              WebSocket | null;
  derivMessageHandler:  ((raw: Buffer) => void) | null;
  derivPingInterval:    NodeJS.Timeout | null;
  adapter:              DerivWsAdapter | null;
  token:                string;
  accountId:            string | null;
  currency:             string;
  pingInterval:         NodeJS.Timeout | null;
  authTimeout:          NodeJS.Timeout | null;
  reconnectTimer:       NodeJS.Timeout | null;
  reconnectAttempts:    number;
  authenticated:        boolean;
  authenticating:       boolean;
  pendingAuth:          { token: string; accountId?: string } | null; // último auth pedido durante outro
  isAdmin:              boolean;
  isAlive:              boolean;
  botManager:           BotManager | null;
  nextReqId:            number;
  clientSubs:           Map<string, string>; // subscription.id → msg_type (só do frontend)
  symbolsCache:         { at: number; list: ActiveSymbol[] } | null;
  symbolsByTypes:       Map<string, { at: number; list: ActiveSymbol[] }>;
  msgWindowStart:       number;
  msgCount:             number;
}

interface ActiveSymbol {
  underlying_symbol: string;
  underlying_symbol_name?: string;
  market?: string;
  submarket?: string;
  exchange_is_open?: number;
  is_trading_suspended?: number;
  pip_size?: number;
}

const sessions = new Map<WebSocket, ClientSession>();
const sessionsByAccount = new Map<string, WebSocket>();

// ─── Helpers ──────────────────────────────────────────────────

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function sendToClient(ws: WebSocket, type: string, payload: Record<string, unknown> = {}): void {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type, ...payload }));
  }
}

function sendError(ws: WebSocket, message: string, code = 'ERROR'): void {
  sendToClient(ws, 'error', { payload: { code, message } });
}

// ids internos do server (< 1M): 900000+ para não colidir com os do frontend
function getNextReqId(session: ClientSession): number {
  session.nextReqId = session.nextReqId >= CLIENT_REQ_ID_LIMIT - 1 ? 900_000 : session.nextReqId + 1;
  return session.nextReqId;
}

/** 429 do handshake do WS ou do REST (OTP/contas). */
function isRateLimited(err: unknown): boolean {
  return isHandshakeRateLimited(err) || (err as { status?: number } | null)?.status === 429;
}

// ─── Encerrar derivWs com segurança ───────────────────────────
// terminate() imediato: usar só em ligações já mortas/falhadas ou
// quando não vale a pena esperar. Para trocas de conta/limpeza de
// sessão usar closeDerivWsGracefully().
function safeTerminateDerivWs(session: ClientSession): void {
  if (session.derivPingInterval) {
    clearInterval(session.derivPingInterval);
    session.derivPingInterval = null;
  }

  const ws = session.derivWs;
  if (!ws) return;

  if (session.derivMessageHandler) {
    ws.removeListener('message', session.derivMessageHandler);
    session.derivMessageHandler = null;
  }

  // removeAllListeners() também remove o 'close' do router → destruir à mão
  destroyMessageRouter(ws);
  ws.removeAllListeners();

  // Absorve o 'error' do abortHandshake (CONNECTING) e qualquer erro tardio.
  ws.on('error', () => { /* esperado */ });

  try {
    ws.terminate();
  } catch (err: any) {
    logger.warn('[WS] Erro ao terminar derivWs (ignorado)', { message: err?.message });
  }
}

/**
 * Fecha a ligação à Deriv com close frame e espera (máx. waitMs) pelo
 * 'close', para a Deriv libertar o slot ANTES de abrirmos outra. Limite
 * da Deriv: 5 ligações simultâneas por utilizador.
 * Também faz session.derivWs = null.
 */
function closeDerivWsGracefully(
  session: ClientSession,
  waitMs = UPSTREAM_CLOSE_WAIT_MS,
): Promise<void> {
  if (session.derivPingInterval) {
    clearInterval(session.derivPingInterval);
    session.derivPingInterval = null;
  }

  const ws = session.derivWs;
  if (!ws) return Promise.resolve();
  session.derivWs = null;

  // Ainda a ligar: terminate() faz o handshake falhar logo e o listener de
  // erro de connectToDerivWS rejeita a promessa (sem ficar 10 s pendurada).
  // Os listeners ficam de propósito.
  if (ws.readyState === WebSocket.CONNECTING) {
    destroyMessageRouter(ws);
    ws.on('error', () => { /* esperado */ });
    try { ws.terminate(); } catch { /* ignorar */ }
    return Promise.resolve();
  }

  if (session.derivMessageHandler) {
    ws.removeListener('message', session.derivMessageHandler);
    session.derivMessageHandler = null;
  }
  destroyMessageRouter(ws);
  ws.removeAllListeners();
  ws.on('error', () => { /* esperado */ });

  if (ws.readyState === WebSocket.CLOSED) return Promise.resolve();

  return new Promise<void>((resolve) => {
    const timer = setTimeout(() => {
      try { ws.terminate(); } catch { /* ignorar */ }
      resolve();
    }, waitMs);
    ws.once('close', () => {
      clearTimeout(timer);
      resolve();
    });
    try {
      ws.close(1000, 'Sessão encerrada');
    } catch {
      clearTimeout(timer);
      try { ws.terminate(); } catch { /* ignorar */ }
      resolve();
    }
  });
}

// ─── Aguardar WS disponível ───────────────────────────────────

function waitForDerivWs(session: ClientSession, timeoutMs = WS_WAIT_TIMEOUT_MS): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    if (session.derivWs?.readyState === WebSocket.OPEN) return resolve(session.derivWs);

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
    }, 100);
  });
}

// ─── Subscrições automáticas ──────────────────────────────────

function subscribeAutomatic(derivWs: WebSocket, session: ClientSession): void {
  if (derivWs.readyState !== WebSocket.OPEN) return;
  derivWs.send(JSON.stringify({ balance: 1, subscribe: 1, req_id: getNextReqId(session) }));
  derivWs.send(JSON.stringify({ transaction: 1, subscribe: 1, req_id: getNextReqId(session) }));
  logger.debug('[WS] Subscrições automáticas enviadas');
}

// ─── forget_all do frontend: só as subscrições do frontend ───
const FORGET_TYPE_TO_MSG: Record<string, string> = {
  ticks: 'tick', ticks_history: 'ohlc', candles: 'ohlc', ohlc: 'ohlc',
};

function handleClientForgetAll(ws: WebSocket, session: ClientSession, payload: Record<string, unknown>): void {
  const raw = payload.forget_all;
  const types = (Array.isArray(raw) ? raw : [raw]).map(String);
  const wanted = new Set(types.map((t) => FORGET_TYPE_TO_MSG[t] ?? t));
  const forgotten: string[] = [];

  for (const [subId, msgType] of session.clientSubs) {
    if (!wanted.has(msgType)) continue;
    if (session.derivWs?.readyState === WebSocket.OPEN) {
      session.derivWs.send(JSON.stringify({ forget: subId, req_id: getNextReqId(session) }));
    }
    session.clientSubs.delete(subId);
    forgotten.push(subId);
  }

  sendToClient(ws, 'forget_all', {
    msg_type: 'forget_all',
    forget_all: forgotten,
    req_id: payload.req_id,
    echo_req: payload,
  });
}

// ─── Proxy Deriv → Frontend ───────────────────────────────────

function buildDerivMessageHandler(
  clientWs: WebSocket,
  session: ClientSession,
  router: ReturnType<typeof attachExternalRouter>,
): (raw: Buffer) => void {
  return (raw: Buffer) => {
    let msg: Record<string, unknown>;
    try { msg = JSON.parse(raw.toString('utf8')); } catch { return; }

    // Parse único: o router trata o tráfego dos bots (e do Operar Manual).
    // Se consumiu a mensagem, não a reencaminhamos ao frontend — EXCETO os
    // ticks: o gráfico/dígitos do dashboard alimentam-se dos ticks que os
    // bots subscrevem, por isso continuam a ser reenviados.
    const consumed = router.dispatch(msg);
    if (consumed && msg.msg_type !== 'tick') return;

    if (msg.msg_type === 'ping' || msg.msg_type === 'pong') return;

    // Regista subscrições abertas PELO frontend (para o forget_all seletivo).
    // Ticks consumidos pelo router são dos BOTS: nunca entram aqui, senão um
    // forget_all do frontend cancelaria a subscrição dos bots.
    const subId = (msg.subscription as { id?: string } | undefined)?.id;
    const mt = msg.msg_type as string | undefined;
    if (!consumed && subId && mt && mt !== 'balance' && mt !== 'transaction'
        && session.clientSubs.get(subId) !== mt
        && (session.clientSubs.has(subId) || session.clientSubs.size < MAX_CLIENT_SUBS)) {
      session.clientSubs.set(subId, mt);
    }

    switch (mt) {
      case 'balance':
        sendToClient(clientWs, 'balance', {
          balance:  (msg.balance as any)?.balance  ?? 0,
          currency: (msg.balance as any)?.currency ?? 'USD',
          loginid:  (msg.balance as any)?.loginid  ?? '',
        });
        break;
      case 'tick':
        sendToClient(clientWs, 'tick', {
          quote:    (msg.tick as any)?.quote,
          epoch:    (msg.tick as any)?.epoch,
          symbol:   (msg.tick as any)?.symbol,
          pip_size: (msg.tick as any)?.pip_size,
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
          entry_tick: poc?.entry_spot ?? poc?.entry_tick,
          exit_tick:  poc?.exit_spot ?? poc?.exit_tick,
          entry_spot: poc?.entry_spot,
          exit_spot:  poc?.exit_spot,
          payout: poc?.payout,
        });
        break;
      }
      case 'ohlc':    sendToClient(clientWs, 'ohlc',    { ohlc:    msg.ohlc    }); break;
      case 'history': sendToClient(clientWs, 'history', { history: msg.history }); break;
      default: sendToClient(clientWs, mt ?? 'message', msg);
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

    const derivWs = new WebSocket(wsUrl, { perMessageDeflate: false, handshakeTimeout: 10_000 });
    session.derivWs = derivWs;
    const router = attachExternalRouter(derivWs);
    let opened = false;

    // listener de erro PERMANENTE e registado já (antes de qualquer terminate)
    derivWs.on('error', (err) => {
      clearTimeout(timeout);
      logger.error('[WS] Deriv WS error', { message: err.message });
      reject(err); // no-op se já resolvida
    });

    const timeout = setTimeout(() => {
      reject(new Error('Timeout de conexão Deriv WS (10s)'));
      try { derivWs.terminate(); } catch { /* ignorar */ }
    }, 10_000);

    derivWs.once('open', () => {
      clearTimeout(timeout);
      opened = true;
      logger.info('[WS] Connected to Deriv WS');

      const handler = buildDerivMessageHandler(clientWs, session, router);
      session.derivMessageHandler = handler;
      derivWs.on('message', handler);

      // Keep-alive (Deriv recomenda ping a cada 30 s). Se não responder,
      // termina a ligação → o 'close' dispara a reconexão.
      if (session.derivPingInterval) clearInterval(session.derivPingInterval);
      session.derivPingInterval = setInterval(() => {
        if (derivWs.readyState !== WebSocket.OPEN) return;
        router.request({ ping: 1 }, nextReqId(), DERIV_PING_TIMEOUT).catch((err) => {
          logger.warn('[WS] Ping Deriv sem resposta — a reiniciar ligação', { error: err.message });
          try { derivWs.terminate(); } catch { /* ignorar */ }
        });
      }, DERIV_PING_MS);
      session.derivPingInterval.unref?.();

      subscribeAutomatic(derivWs, session);
      resolve();
    });

    derivWs.once('close', (code, reason) => {
      clearTimeout(timeout);
      if (session.derivMessageHandler) {
        derivWs.removeListener('message', session.derivMessageHandler);
        session.derivMessageHandler = null;
      }
      // socket substituído entretanto: este close já não interessa
      if (session.derivWs !== derivWs) return;

      if (session.derivPingInterval) {
        clearInterval(session.derivPingInterval);
        session.derivPingInterval = null;
      }
      session.derivWs = null;
      session.clientSubs.clear(); // ids de subscrição morrem com a ligação
      logger.warn('[WS] Deriv WS disconnected', { code, reason: reason?.toString(), opened });

      if (!opened) return; // falhou ao ligar: o chamador (reject) trata da nova tentativa

      if (clientWs.readyState === WebSocket.OPEN) {
        sendToClient(clientWs, 'deriv_disconnected', { message: 'Deriv WS desconectado. A reconectar...' });
      }
      scheduleDerivReconnect(clientWs, session);
    });
  });
}

// ─── Lock por conta + espaçamento entre handshakes ────────────
// Sessão duplicada + OTP + handshake de UMA conta correm um de cada vez;
// e há um intervalo mínimo entre handshakes da mesma conta.

const accountLocks = new Map<string, Promise<unknown>>();
const lastHandshakeAt = new Map<string, number>();

function withAccountLock<T>(accountId: string, fn: () => Promise<T>): Promise<T> {
  const prev = accountLocks.get(accountId) ?? Promise.resolve();
  const run = prev.catch(() => undefined).then(fn);
  const tail = run.catch(() => undefined);
  accountLocks.set(accountId, tail);
  void tail.then(() => {
    if (accountLocks.get(accountId) === tail) accountLocks.delete(accountId);
  });
  return run;
}

function noteHandshake(accountId: string): void {
  const now = Date.now();
  lastHandshakeAt.set(accountId, now);
  if (lastHandshakeAt.size > 500) {
    for (const [id, at] of lastHandshakeAt) {
      if (now - at > 60_000) lastHandshakeAt.delete(id);
    }
  }
}

async function waitHandshakeGap(accountId: string): Promise<void> {
  const wait = UPSTREAM_MIN_GAP_MS - (Date.now() - (lastHandshakeAt.get(accountId) ?? 0));
  if (wait > 0) await sleep(wait);
}

/**
 * OTP novo + handshake com a Deriv, com retry no servidor (backoff com
 * jitter; o dobro quando é 429). O cliente do frontend não é fechado
 * entre tentativas. Chamar SEMPRE dentro de withAccountLock.
 */
async function openUpstream(
  clientWs: WebSocket,
  session: ClientSession,
  derivAPI: DerivAPIService,
  accountId: string,
): Promise<void> {
  let lastErr: unknown;

  for (let attempt = 1; attempt <= UPSTREAM_CONNECT_TRIES; attempt++) {
    if (clientWs.readyState !== WebSocket.OPEN || !sessions.has(clientWs)) {
      throw new Error('Cliente desligou durante a autenticação');
    }

    await waitHandshakeGap(accountId);

    try {
      // OTP é de uso único: um novo em CADA tentativa
      const otpData = await derivAPI.getOTP(accountId);
      noteHandshake(accountId);
      await connectToDerivWS(clientWs, otpData.url, session);

      // O cliente pode ter saído enquanto ligávamos: fechar já, senão a
      // ligação à Deriv ficava órfã (e ocupava um dos 5 slots).
      if (clientWs.readyState !== WebSocket.OPEN || !sessions.has(clientWs)) {
        await closeDerivWsGracefully(session);
        throw new Error('Cliente desligou durante a autenticação');
      }
      return;
    } catch (err) {
      lastErr = err;
      if (err instanceof DerivTokenExpiredError) throw err; // repetir não adianta
      if (clientWs.readyState !== WebSocket.OPEN || !sessions.has(clientWs)) throw err;

      // socket falhado ainda pendurado em session.derivWs
      if (session.derivWs) safeTerminateDerivWs(session);
      session.derivWs = null;

      if (attempt === UPSTREAM_CONNECT_TRIES) break;

      const limited = isRateLimited(err);
      const base = Math.min(
        UPSTREAM_RETRY_BASE_MS * 2 ** (attempt - 1) * (limited ? 2 : 1),
        UPSTREAM_RETRY_MAX_MS,
      );
      const delay = Math.round(base * (0.8 + Math.random() * 0.4));
      logger.warn('[WS] Ligação à Deriv falhou — nova tentativa', {
        attempt, of: UPSTREAM_CONNECT_TRIES, delayMs: delay, rateLimited: limited,
        error: (err as Error)?.message,
      });
      sendToClient(clientWs, 'deriv_retrying', { payload: { attempt, delayMs: delay, rateLimited: limited } });
      await sleep(delay);
    }
  }

  throw lastErr;
}

// ─── Reconexão Deriv com novo OTP ────────────────────────────

function scheduleDerivReconnect(clientWs: WebSocket, session: ClientSession, rateLimited = false): void {
  if (clientWs.readyState !== WebSocket.OPEN) return;
  if (!session.authenticated || !session.token || !session.accountId) return;
  if (session.reconnectAttempts >= RECONNECT_MAX_TRIES) {
    logger.error('[WS] Máximo de reconexões atingido');
    sendError(clientWs, 'Conexão Deriv perdida. Faz login novamente.', 'DERIV_DISCONNECTED');
    clientWs.close();
    return;
  }

  const base  = Math.max(
    Math.min(RECONNECT_BASE_MS * Math.pow(2, session.reconnectAttempts), RECONNECT_MAX_MS),
    rateLimited ? UPSTREAM_RATE_LIMIT_FLOOR_MS : 0,
  );
  const delay = Math.floor(base + Math.random() * 0.3 * base);
  session.reconnectAttempts++;

  logger.info('[WS] Reconexão Deriv agendada', {
    attempt: session.reconnectAttempts, delayMs: delay, accountId: session.accountId, rateLimited,
  });

  if (session.reconnectTimer) clearTimeout(session.reconnectTimer);

  session.reconnectTimer = setTimeout(async () => {
    session.reconnectTimer = null;
    // Se há uma autenticação em curso, é ela que trata da ligação
    if (clientWs.readyState !== WebSocket.OPEN || session.authenticating) return;
    const accountId = session.accountId;
    if (!accountId) return;

    try {
      await withAccountLock(accountId, async () => {
        if (!sessions.has(clientWs) || session.authenticating) return;

        await waitHandshakeGap(accountId);
        const derivAPI = new DerivAPIService(session.token);
        const otpData  = await derivAPI.getOTP(accountId);
        // descarta um socket falhado que ainda esteja pendurado
        if (session.derivWs) safeTerminateDerivWs(session);
        noteHandshake(accountId);
        await connectToDerivWS(clientWs, otpData.url, session);

        // cliente saiu enquanto ligávamos: não deixar a ligação órfã
        if (clientWs.readyState !== WebSocket.OPEN || !sessions.has(clientWs)) {
          await closeDerivWsGracefully(session);
          return;
        }

        session.reconnectAttempts = 0;

        // Os bots NÃO são parados: o adapter lê session.derivWs e volta a
        // subscrever ticks/contratos sozinho na nova ligação.
        if (!session.botManager) initBotManagerForSession(clientWs, session);
        sendToClient(clientWs, 'deriv_reconnected', { message: 'Conexão Deriv restabelecida' });
      });
    } catch (err: any) {
      logger.error('[WS] Falha na reconexão Deriv', { error: err.message });
      scheduleDerivReconnect(clientWs, session, isRateLimited(err));
    }
  }, delay);
}

// ─── BotManager ───────────────────────────────────────────────

function getAdapter(session: ClientSession): DerivWsAdapter {
  if (!session.adapter) session.adapter = createDerivWsAdapter(() => session.derivWs);
  return session.adapter;
}

function initBotManagerForSession(clientWs: WebSocket, session: ClientSession): void {
  if (!session.derivWs) return;

  const manager = new BotManager(getAdapter(session));
  manager.on('bot_event', (event: BotEvent) => {
    sendToClient(clientWs, event.type, { payload: { botId: event.botId, ...event.payload } });
  });

  manager.setSymbolResolver((strategy, config, current) => resolveSymbol(session, strategy, config, current));

  session.botManager = manager;
  logger.info('[WS] BotManager inicializado para sessão');
}

// ─── Disponibilidade por conta/país ───────────────────────────
// O que a Deriv oferece varia por país, conta e hora (forex fecha ao
// fim de semana). Consultamos active_symbols da PRÓPRIA conta.

const SYMBOLS_TTL_MS = 5 * 60_000;

async function getAccountSymbols(session: ClientSession): Promise<ActiveSymbol[]> {
  if (session.symbolsCache && Date.now() - session.symbolsCache.at < SYMBOLS_TTL_MS) {
    return session.symbolsCache.list;
  }
  const ws = await waitForDerivWs(session, 5_000);
  const res = await getMessageRouter(ws).request({ active_symbols: 'brief' }, nextReqId(), 15_000);
  const list = (res.active_symbols as ActiveSymbol[] | undefined) ?? [];
  session.symbolsCache = { at: Date.now(), list };
  return list;
}

// ─── Mudança automática de mercado ────────────────────────────
// Se o mercado do bot estiver fechado, suspenso ou não for oferecido a
// esta conta/país, o bot passa para outro mercado ABERTO que ofereça o
// mesmo tipo de contrato. Ordem de preferência:
//   1) strategyParams.fallbackSymbols (lista do catálogo, por ordem)
//   2) mesmo submercado  →  3) mesmo mercado  →  4) qualquer mercado aberto
// Variáveis de ambiente:
//   SYMBOL_FAILOVER=0               desliga a mudança automática
//   SYMBOL_FAILOVER_ANY_MARKET=0    só muda dentro do mesmo mercado
const FAILOVER_ENABLED    = process.env.SYMBOL_FAILOVER !== '0';
const FAILOVER_ANY_MARKET = process.env.SYMBOL_FAILOVER_ANY_MARKET !== '0';
const TYPED_SYMBOLS_TTL_MS = 30_000;

function contractTypesFor(strategy: BotStrategyType, config: BotConfig): string[] {
  const p = (config.strategyParams ?? {}) as Record<string, unknown>;
  switch (strategy) {
    case 'alfha':             return ['DIGITOVER', 'DIGITUNDER'];
    case 'scalper':
    case 'over1_recovery':    return ['DIGITOVER'];
    case 'even_odd_recovery': return [p.targetParity === 'odd' ? 'DIGITODD' : 'DIGITEVEN'];
    default:                  return ['CALL', 'PUT']; // martingale, scalping, trend_following
  }
}

/** Símbolos que a conta pode operar para estes tipos de contrato (cache curto: o estado aberto/fechado muda). */
async function getSymbolsFor(session: ClientSession, types: string[]): Promise<ActiveSymbol[]> {
  const key = types.join(',');
  const cached = session.symbolsByTypes.get(key);
  if (cached && Date.now() - cached.at < TYPED_SYMBOLS_TTL_MS) return cached.list;

  const ws = await waitForDerivWs(session, 5_000);
  let list: ActiveSymbol[];
  try {
    const res = await getMessageRouter(ws).request(
      { active_symbols: 'brief', contract_type: types }, nextReqId(), 15_000,
    );
    list = (res.active_symbols as ActiveSymbol[] | undefined) ?? [];
  } catch (err: any) {
    // filtro por contrato recusado → lista completa da conta
    logger.warn('[WS] active_symbols com contract_type falhou — a usar lista completa', { error: err?.message });
    list = await getAccountSymbols(session);
  }
  session.symbolsByTypes.set(key, { at: Date.now(), list });
  return list;
}

async function resolveSymbol(
  session: ClientSession, strategy: BotStrategyType, config: BotConfig, current: string,
): Promise<SymbolResolution> {
  let list: ActiveSymbol[];
  try {
    list = await getSymbolsFor(session, contractTypesFor(strategy, config));
  } catch (err: any) {
    logger.warn('[WS] Não foi possível verificar mercados — mantém o atual', { current, error: err?.message });
    return { symbol: current, changed: false };
  }
  if (!list.length) return { symbol: current, changed: false };

  const usable = (s: ActiveSymbol) => s.exchange_is_open !== 0 && s.is_trading_suspended !== 1;
  const cur = list.find((s) => s.underlying_symbol === current);
  if (cur && usable(cur)) return { symbol: current, changed: false };

  const reason = !cur
    ? 'não disponível para a sua conta/país'
    : cur.is_trading_suspended === 1 ? 'negociação suspensa' : 'mercado fechado';

  if (!FAILOVER_ENABLED) return { symbol: null, changed: false, reason };

  const open = list.filter((s) => usable(s) && s.underlying_symbol !== current);
  const pick = (pred: (s: ActiveSymbol) => boolean) => open.find(pred)?.underlying_symbol;

  const params = (config.strategyParams ?? {}) as Record<string, unknown>;
  const explicit = Array.isArray(params.fallbackSymbols) ? params.fallbackSymbols.map(String) : [];

  const chosen =
    explicit.find((sym) => open.some((s) => s.underlying_symbol === sym))
    ?? (cur ? pick((s) => s.submarket === cur.submarket) : undefined)
    ?? (cur ? pick((s) => s.market === cur.market) : undefined)
    ?? (FAILOVER_ANY_MARKET || !cur ? pick(() => true) : undefined);

  if (!chosen) return { symbol: null, changed: false, reason };
  return { symbol: chosen, changed: true, reason };
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
          // SEMPRE a moeda da conta ativa (EUR, GBP, BTC…): o catálogo traz "USD"
          // e comprar com a moeda errada faz a Deriv rejeitar todos os contratos.
          currency: session.currency || 'USD',
          strategyParams: {
            ...(catalogBot.defaultConfig.strategyParams ?? {}),
            ...((configOverride.strategyParams as Record<string, unknown>) ?? {}),
          },
        };

        const stakeErr = stakeSanityError(finalConfig.currency, Number(finalConfig.initialStake));
        if (stakeErr) { sendError(ws, stakeErr, 'INVALID_STAKE'); break; }

        const originalSymbol = finalConfig.symbol;
        const resolution = await resolveSymbol(session, catalogBot.strategy, finalConfig, originalSymbol);
        if (resolution.symbol === null) {
          sendError(ws,
            `O mercado ${originalSymbol} está indisponível (${resolution.reason}) e não há outro mercado aberto para este bot agora.`,
            'SYMBOL_UNAVAILABLE');
          break;
        }
        if (resolution.changed) {
          finalConfig.symbol = resolution.symbol;
          sendToClient(ws, 'bot_symbol_fallback', {
            payload: { from: originalSymbol, to: resolution.symbol, reason: resolution.reason },
          });
        }

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
        await manager.deleteBot(botId); // agora async (aguarda o stop)
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
    logger.error('[WS] Erro no handler de bot', { type, error: err.message, code: err.code });
    sendError(ws, classifyDerivError(err).userMessage, 'BOT_ERROR');
  }
}

// ─── Compra manual ────────────────────────────────────────────
// Usa o adapter (buy directo: 1 viagem de rede) e acompanha o fecho
// com o mesmo mecanismo dos bots. O dígito final vem de exit_spot
// (string) com as casas decimais reais do símbolo.

const DIGIT_CONTRACT_TYPES = new Set([
  'DIGITOVER', 'DIGITUNDER', 'DIGITMATCH', 'DIGITDIFF', 'DIGITODD', 'DIGITEVEN',
]);

async function handleManualBuy(
  ws: WebSocket, session: ClientSession, payload: Record<string, unknown>,
): Promise<void> {
  const symbol       = payload.symbol as string;
  const contractType = payload.contractType as string;
  const duration     = Number(payload.duration);
  const durationUnit = payload.durationUnit as string;
  const stake        = Number(payload.stake);
  const barrier      = payload.barrier;
  // o frontend envia um clientRef e recebe-o de volta: liga cada resposta ao pedido
  // certo (várias compras seguidas já não trocam os dados entre si)
  const clientRef    = typeof payload.clientRef === 'string' ? payload.clientRef : undefined;

  const fail = (error: string) =>
    sendToClient(ws, 'manual_buy_result', { payload: { clientRef, success: false, error } });

  if (!symbol || !contractType || !duration || !durationUnit || !stake) {
    fail('Parâmetros em falta.');
    return;
  }
  if (!session.derivWs || session.derivWs.readyState !== WebSocket.OPEN) {
    fail('Conexão Deriv indisponível.');
    return;
  }

  const stakeErr = stakeSanityError(session.currency || 'USD', stake);
  if (stakeErr) { fail(stakeErr); return; }

  const adapter = getAdapter(session);

  try {
    const bought = await adapter.buyContract({
      symbol,
      contractType,
      stake,
      duration,
      durationUnit, // o adapter converte 'ticks' → 't'
      currency: session.currency || 'USD',
      ...(DIGIT_CONTRACT_TYPES.has(contractType) && barrier !== undefined && barrier !== null && barrier !== ''
        ? { barrier: String(barrier) }
        : {}),
    });

    sendToClient(ws, 'manual_buy_result', {
      payload: {
        clientRef,
        success: true,
        contractId: bought.contractId,
        payout: (bought as { payout?: number }).payout,
      },
    });

    // Fecho do contrato (não bloqueia o handler)
    adapter.waitForContract(bought.contractId)
      .then((res) => {
        sendToClient(ws, 'manual_trade_closed', {
          payload: {
            clientRef,
            contractId: bought.contractId,
            profit: res.profit,
            won: res.won,
            stake,
            exitTick: res.exitTick || undefined,
            exitDigit: (res as { exitDigit?: number }).exitDigit,
          },
        });
      })
      .catch((err: Error) => {
        logger.error('[WS] Erro ao acompanhar contrato manual', { contractId: bought.contractId, error: err.message });
        // o frontend desbloqueia o ecrã (o contrato pode ainda estar aberto na Deriv)
        sendToClient(ws, 'manual_trade_error', {
          payload: {
            clientRef,
            contractId: bought.contractId,
            message: `Não foi possível acompanhar o contrato ${bought.contractId}. Confirme o resultado na Deriv.`,
          },
        });
      });
  } catch (err: any) {
    logger.error('[WS] Erro na compra manual', { error: err.message });
    fail(classifyDerivError(err).userMessage || 'Erro ao comprar.');
  }
}

// ─── Autenticação ─────────────────────────────────────────────

async function authenticateClient(ws: WebSocket, token: string, accountId?: string): Promise<void> {
  const session = sessions.get(ws);
  if (!session) return;

  if (session.authenticating) {
    // Autenticação em curso: guarda o ÚLTIMO pedido (ex: troca demo ↔ real)
    // e corre-o quando a atual terminar, em vez de o descartar.
    session.pendingAuth = { token, accountId };
    return;
  }
  session.authenticating = true;

  try {
    if (session.authTimeout) { clearTimeout(session.authTimeout); session.authTimeout = null; }

    if (session.botManager) {
      await session.botManager.stopAll();
      session.botManager.removeAllListeners();
      session.botManager = null;
    }

    if (session.reconnectTimer) { clearTimeout(session.reconnectTimer); session.reconnectTimer = null; }
    // fecho limpo: a Deriv liberta o slot antes de abrirmos a nova ligação
    await closeDerivWsGracefully(session);
    session.clientSubs.clear();
    session.symbolsCache = null;
    session.symbolsByTypes.clear();

    const derivAPI    = new DerivAPIService(token);
    const accountsRes = await derivAPI.getAccounts();
    const accounts: any[] = accountsRes.data ?? [];

    if (!accounts.length) { sendError(ws, 'Nenhuma conta encontrada', 'NO_ACCOUNTS'); ws.close(); return; }

    const target = accountId
      ? (accounts.find((a: any) => a.account_id === accountId) ?? accounts[0])
      : accounts[0];

    const targetAccountId = target.account_id;

    // Sessão duplicada + OTP + handshake: um de cada vez por conta
    const connected = await withAccountLock(targetAccountId, async (): Promise<boolean> => {
      if (!sessions.has(ws)) return false; // o cliente já saiu enquanto esperava

      const existingWs = sessionsByAccount.get(targetAccountId);
      if (existingWs && existingWs !== ws) {
        logger.warn('[WS] Sessão duplicada detectada, a fechar a antiga', { accountId: targetAccountId });
        await cleanupSession(existingWs); // espera o fecho da ligação à Deriv
        existingWs.close(1001, 'Sessão substituída por nova conexão');
      }

      // troca de conta: libertar o registo da conta anterior
      if (session.accountId && session.accountId !== targetAccountId
          && sessionsByAccount.get(session.accountId) === ws) {
        sessionsByAccount.delete(session.accountId);
      }

      session.token         = token;
      session.accountId     = targetAccountId;
      session.currency      = target.currency ?? 'USD';
      session.isAdmin       = ADMIN_IDS.has(targetAccountId);
      session.authenticated = true;
      session.reconnectAttempts = 0;

      sessionsByAccount.set(targetAccountId, ws);

      await openUpstream(ws, session, derivAPI, targetAccountId);
      return true;
    });

    if (!connected) return;

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
    if (err instanceof DerivTokenExpiredError) {
      // token inválido/expirado: o frontend deve terminar a sessão
      sendError(ws, 'A sessão Deriv expirou. Faça login novamente.', 'AUTH_FAILED');
    } else {
      // falha temporária (rede, Deriv em baixo, 429): NÃO terminar a sessão.
      // O frontend reconecta com backoff longo ao receber AUTH_TEMPORARY.
      sendError(
        ws,
        isRateLimited(err)
          ? 'A Deriv limitou temporariamente as ligações. A tentar de novo dentro de instantes…'
          : 'Não foi possível ligar à Deriv agora. A tentar de novo…',
        'AUTH_TEMPORARY',
      );
    }
    ws.close();
  } finally {
    session.authenticating = false;

    // houve outro pedido de auth enquanto este corria: processa o último
    const next = session.pendingAuth;
    session.pendingAuth = null;
    if (next && ws.readyState === WebSocket.OPEN && sessions.has(ws)) {
      const same = session.authenticated
        && next.token === session.token
        && (!next.accountId || next.accountId === session.accountId);
      if (!same) void authenticateClient(ws, next.token, next.accountId);
    }
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
  session.pendingAuth = null;

  if (session.botManager) {
    try { await session.botManager.stopAll(); } catch { /* ignorar */ }
    session.botManager.removeAllListeners();
  }

  // fecho limpo (close frame): liberta o slot na Deriv de imediato
  await closeDerivWsGracefully(session);
  session.adapter = null;
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
    derivWs: null, derivMessageHandler: null, derivPingInterval: null, adapter: null,
    token: '', accountId: null, currency: 'USD',
    pingInterval: null, authTimeout: null, reconnectTimer: null,
    reconnectAttempts: 0, authenticated: false, authenticating: false, pendingAuth: null,
    isAdmin: false, isAlive: true,
    botManager: null, nextReqId: 900_000,
    clientSubs: new Map(), symbolsCache: null, symbolsByTypes: new Map(), msgWindowStart: 0, msgCount: 0,
  };
  sessions.set(ws, session);

  // Ping ao cliente com deteção de ligação morta (half-open)
  session.pingInterval = setInterval(() => {
    if (!session.isAlive) {
      logger.warn('[WS] Cliente sem pong — a terminar sessão', { ip });
      ws.terminate();
      return;
    }
    session.isAlive = false;
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
    session.isAlive = true; // qualquer tráfego prova que está vivo

    // limite de mensagens por segundo
    const now = Date.now();
    if (now - session.msgWindowStart >= 1000) { session.msgWindowStart = now; session.msgCount = 0; }
    if (++session.msgCount > CLIENT_MSGS_PER_SEC) {
      sendError(ws, 'Demasiadas mensagens. Abranda.', 'RATE_LIMITED');
      return;
    }

    let data: Record<string, unknown>;
    try { data = JSON.parse(raw.toString('utf8')); } catch {
      sendError(ws, 'JSON inválido', 'PARSE_ERROR'); return;
    }

    const type    = data.type as string | undefined;
    const payload = (data.payload ?? {}) as Record<string, unknown>;

    try {
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

      if (type === 'manual_buy') {
        if (!session.authenticated) { sendError(ws, 'Não autenticado', 'NOT_AUTH'); return; }
        await handleManualBuy(ws, session, payload);
        return;
      }

      if (type === 'get_available_symbols') {
        if (!session.authenticated) { sendError(ws, 'Não autenticado', 'NOT_AUTH'); return; }
        try {
          const list = await getAccountSymbols(session);
          sendToClient(ws, 'available_symbols', {
            payload: {
              currency: session.currency,
              symbols: list.map((s) => ({
                symbol: s.underlying_symbol,
                name: s.underlying_symbol_name,
                market: s.market,
                submarket: s.submarket,
                isOpen: s.exchange_is_open === 1 && s.is_trading_suspended !== 1,
                pipSize: s.pip_size,
              })),
            },
          });
        } catch (err: any) {
          sendError(ws, classifyDerivError(err).userMessage, 'SYMBOLS_ERROR');
        }
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
      if (session.derivWs.readyState !== WebSocket.OPEN) {
        sendError(ws, 'Deriv WS não conectado', 'DERIV_DISCONNECTED'); return;
      }

      // ── Proxy genérico frontend → Deriv ──
      const { type: _t, payload: _p, ...rest } = data;
      const rawPayload = (_p && Object.keys(_p as object).length > 0)
        ? _p as Record<string, unknown>
        : rest as Record<string, unknown>;

      // forget_all do frontend: só as suas subscrições (os bots partilham o WS)
      if ('forget_all' in rawPayload) {
        handleClientForgetAll(ws, session, rawPayload);
        return;
      }
      if (typeof rawPayload.forget === 'string') session.clientSubs.delete(rawPayload.forget);

      // req_id do frontend fica abaixo de 1M; ids internos (bots) são >= 1M
      const existingReqId = rawPayload.req_id;
      const numericReqId = (typeof existingReqId === 'number' && Number.isInteger(existingReqId)
        && existingReqId > 0 && existingReqId < CLIENT_REQ_ID_LIMIT)
        ? existingReqId
        : getNextReqId(session);

      session.derivWs.send(JSON.stringify({ ...rawPayload, req_id: numericReqId }));
    } catch (err: any) {
      logger.error('[WS] Erro ao processar mensagem do cliente', { type, error: err?.message });
      sendError(ws, err?.message ?? 'Erro interno', 'INTERNAL_ERROR');
    }
  });

  ws.on('pong', () => { session.isAlive = true; });

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

// /health ANTES do rate limiter (o polling do Render recebia 429)
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

app.use('/api/auth',                 authRoutes);
app.use('/api/accounts',             accountRoutes);
app.use('/api/trading',              tradingRoutes);
app.use('/api/bots',                 botsRoutes);
app.use('/api/admin',                adminRoutes);
app.use('/api/admin/labels',         labelsRoutes);
app.use('/api/admin/users',          adminUsersRoutes);
app.use('/api/notifications',        notificationsRoutes);
app.use('/api/admin/notifications',  adminNotificationsRoutes);
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
  const forceExit = setTimeout(() => process.exit(1), 30_000);
  forceExit.unref?.();
  // cleanupSession fecha o WS da Deriv com close frame (liberta o slot
  // antes de a instância nova arrancar no deploy do Render)
  await Promise.allSettled([...sessions.keys()].map(async (clientWs) => {
    await cleanupSession(clientWs);
    clientWs.close();
  }));
  wss.close(() => logger.info('WS server fechado'));
  server.close(async () => {
    try { await closeRedis(); } catch { /* ignorar */ }
    destroyMemoryStore();
    process.exit(0);
  });
};

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT',  () => gracefulShutdown('SIGINT'));
process.on('uncaughtException',  (e) => { logger.error('Uncaught Exception',  { message: e.message, stack: e.stack }); process.exit(1); });
process.on('unhandledRejection', (r) => { logger.error('Unhandled Rejection', { reason: r }); });

startServer();
export default app;
