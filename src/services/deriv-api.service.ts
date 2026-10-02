// ============================================================
// NEXORA FOREX — Deriv API Service v2
// ============================================================
// Correcções vs v1:
//   ✅ Erros REST lidos do envelope oficial (errors[0].code/message).
//      Antes lia data.message → mensagens genéricas "HTTP 401".
//   ✅ 401/403 em getAccounts/getOTP → DerivTokenExpiredError
//   ✅ Retry com backoff em GET e OTP (rede, 429 com Retry-After,
//      502/503/504). OTP é seguro repetir; buy/sell NÃO passam por aqui.
//   ✅ createAuthenticatedWebSocket: parse JSON UMA vez e entrega ao
//      MessageRouter (antes: server.ts e router faziam parse cada um
//      → 2× JSON.parse por tick)
//   ✅ perMessageDeflate desligado + handshakeTimeout
//   ✅ healthCheck sem Authorization (endpoint público)
// ============================================================

import axios, { AxiosError } from 'axios';
import WebSocket from 'ws';
import logger from '@utils/logger.js';
import { config } from '@config/index.js';
import { attachExternalRouter } from '../bots/message-router.js';

const DERIV_REST_BASE = 'https://api.derivws.com';
const REQUEST_TIMEOUT = 15_000;

// ─── Erros dedicados ────────────────────────────────────────

export class DerivTokenExpiredError extends Error {
  constructor(message = 'Token da Deriv inválido, expirado ou sem permissão suficiente') {
    super(message);
    this.name = 'DerivTokenExpiredError';
  }
}

export class DerivRestError extends Error {
  readonly status?: number;
  readonly code?: string;
  constructor(message: string, status?: number, code?: string) {
    super(message);
    this.name = 'DerivRestError';
    this.status = status;
    this.code = code;
  }
}

const TOKEN_ERROR_CODES = ['InvalidToken', 'AuthorizationRequired', 'Unauthorized', 'PermissionDenied'];

function describeError(error: AxiosError): { status?: number; code: string; message: string } {
  const data = error.response?.data as any;
  const first = data?.errors?.[0];
  return {
    status: error.response?.status,
    code: String(first?.code ?? data?.error?.code ?? ''),
    message: String(first?.message ?? data?.error?.message ?? data?.message ?? error.message),
  };
}

function isTokenError(error: unknown): boolean {
  if (!axios.isAxiosError(error)) return false;
  const { status, code } = describeError(error);
  return status === 401 || status === 403 || TOKEN_ERROR_CODES.includes(code);
}

function isRetryable(error: unknown): boolean {
  if (!axios.isAxiosError(error)) return false;
  const status = error.response?.status;
  if (!status) return true; // rede / timeout
  return status === 429 || status === 502 || status === 503 || status === 504;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function withRetry<T>(fn: () => Promise<T>, attempts = 3): Promise<T> {
  let lastErr: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      if (!isRetryable(err) || i === attempts - 1) throw err;
      let wait = Math.min(300 * 2 ** i, 2_000) + Math.random() * 100;
      if (axios.isAxiosError(err) && err.response?.status === 429) {
        const ra = Number(err.response.headers?.['retry-after']);
        if (Number.isFinite(ra) && ra > 0) wait = Math.min(ra * 1000, 5_000);
      }
      logger.warn('[DerivAPI] Falha transitória — a repetir', { attempt: i + 1, wait: Math.round(wait) });
      await sleep(wait);
    }
  }
  throw lastErr;
}

// ─── Tipos ───────────────────────────────────────────────────

export interface DerivAccount {
  account_id:   string;
  balance:      number;
  currency:     string;
  account_type: 'demo' | 'real';
  status:       string;
  group:        string;
  is_virtual?:  boolean;
  loginid?:     string;
  email?:       string;
  name?:        string;
}

interface OTPResponse { data: { url: string } }
interface AccountsResponse { data: DerivAccount[] }

export interface MarkupStatisticsBreakdown {
  app_id:          string;
  app_markup_usd:  number;
  volume_usd:      number;
  payout_usd:      number;
  contract_count:  number;
  client_count:    number;
}

export interface MarkupStatisticsData {
  total_app_markup_usd: number;
  total_volume_usd:     number;
  total_payout_usd:     number;
  total_contract_count: number;
  total_client_count:   number;
  breakdown:            MarkupStatisticsBreakdown[];
}

export interface MarkupStatisticsResponse {
  data: MarkupStatisticsData;
  meta: { endpoint: string; method: string; timing: number };
}

// ─── DerivAPIService ──────────────────────────────────────────

export class DerivAPIService {
  private readonly token: string;

  constructor(token: string) {
    if (!token) throw new Error('DerivAPIService: token é obrigatório');
    this.token = token;
  }

  // Deriv-App-ID: obrigatório com PAT, dispensável com OAuth.
  private get headers() {
    return {
      Authorization:  `Bearer ${this.token}`,
      'Content-Type': 'application/json',
      ...(config.deriv.appId ? { 'Deriv-App-ID': config.deriv.appId } : {}),
    };
  }

  // ── Contas ───────────────────────────────────────────────

  async getAccounts(): Promise<AccountsResponse> {
    try {
      const response = await withRetry(() =>
        axios.get<AccountsResponse>(`${DERIV_REST_BASE}/trading/v1/options/accounts`, {
          headers: this.headers,
          timeout: REQUEST_TIMEOUT,
        }),
      );
      logger.info('[DerivAPI] Contas obtidas', { count: response.data.data?.length ?? 0 });
      return response.data;
    } catch (error) {
      if (isTokenError(error)) throw new DerivTokenExpiredError();
      if (axios.isAxiosError(error)) {
        const d = describeError(error);
        logger.error('[DerivAPI] Falha ao obter contas', d);
        throw new DerivRestError(d.message || `Falha ao obter contas (HTTP ${d.status})`, d.status, d.code);
      }
      throw error;
    }
  }

  // ── OTP → URL WebSocket autenticada ──────────────────────

  /** OTP é de uso único e expira depressa: pedir um novo a CADA ligação. */
  async getOTP(accountId: string): Promise<{ url: string; wsUrl: string }> {
    try {
      const response = await withRetry(() =>
        axios.post<OTPResponse>(
          `${DERIV_REST_BASE}/trading/v1/options/accounts/${accountId}/otp`,
          {},
          { headers: this.headers, timeout: REQUEST_TIMEOUT },
        ),
      );

      const url = response.data?.data?.url;
      if (!url || !url.startsWith('wss://')) {
        throw new Error('URL OTP inválida ou em formato inesperado');
      }
      logger.info('[DerivAPI] OTP obtido', { accountId });
      return { url, wsUrl: url };
    } catch (error) {
      if (isTokenError(error)) throw new DerivTokenExpiredError();
      if (axios.isAxiosError(error)) {
        const d = describeError(error);
        logger.error('[DerivAPI] Falha ao obter OTP', { accountId, ...d });
        throw new DerivRestError(d.message || `Falha ao obter OTP (HTTP ${d.status})`, d.status, d.code);
      }
      throw error;
    }
  }

  // ── Health ───────────────────────────────────────────────

  async healthCheck(): Promise<{ status: string }> {
    try {
      const response = await axios.get<{ status: string }>(`${DERIV_REST_BASE}/v1/health`, {
        timeout: 5_000,
      });
      return response.data;
    } catch {
      return { status: 'error' };
    }
  }

  // ── Markup statistics ────────────────────────────────────

  async getMarkupStatistics(dateFrom: string, dateTo: string): Promise<MarkupStatisticsResponse> {
    try {
      const response = await withRetry(() =>
        axios.get<MarkupStatisticsResponse>(`${DERIV_REST_BASE}/applications/v1/markup-statistics`, {
          headers: this.headers,
          params:  { date_from: dateFrom, date_to: dateTo },
          timeout: REQUEST_TIMEOUT,
        }),
      );
      logger.info('[DerivAPI] Markup statistics obtidas', { dateFrom, dateTo });
      return response.data;
    } catch (error) {
      if (isTokenError(error)) {
        logger.warn('[DerivAPI] Token de markup rejeitado (expirado/inválido)');
        throw new DerivTokenExpiredError();
      }
      if (axios.isAxiosError(error)) {
        const d = describeError(error);
        logger.error('[DerivAPI] Falha ao obter markup statistics', d);
        throw new DerivRestError(
          d.message || `Falha ao obter estatísticas de comissão (HTTP ${d.status})`,
          d.status,
          d.code,
        );
      }
      throw error;
    }
  }

  // ── WebSocket autenticado (via OTP) ──────────────────────

  /**
   * NÃO enviar mensagem de autenticação: a URL com OTP já autentica.
   * Heartbeat (ping) fica a cargo do server.ts.
   * O JSON é parseado UMA vez aqui e entregue ao MessageRouter e ao onMessage.
   */
  createAuthenticatedWebSocket(
    wsUrl: string,
    onMessage: (data: Record<string, unknown>) => void,
    onError?:  (error: Error) => void,
    onClose?:  () => void,
    onOpen?:   () => void,
  ): WebSocket {
    logger.info('[DerivAPI] A criar WS autenticado', {
      url: wsUrl.replace(/otp=[^&]+/, 'otp=***'),
    });

    const ws = new WebSocket(wsUrl, { perMessageDeflate: false, handshakeTimeout: 10_000 });
    const router = attachExternalRouter(ws);

    ws.on('open', () => {
      logger.info('[DerivAPI] WS autenticado conectado');
      onOpen?.();
    });

    ws.on('message', (raw: WebSocket.RawData) => {
      let msg: Record<string, unknown>;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        logger.warn('[DerivAPI] Mensagem WS não é JSON');
        return;
      }

      router.dispatch(msg);

      if (msg.msg_type === 'ping' || msg.msg_type === 'pong') return;
      onMessage(msg);
    });

    ws.on('error', (err: Error) => {
      logger.error('[DerivAPI] Erro no WS autenticado', { message: err.message });
      onError?.(err);
    });

    ws.on('close', () => {
      logger.info('[DerivAPI] WS autenticado fechado');
      onClose?.();
    });

    return ws;
  }

  // ── WebSocket público (sem autenticação) ─────────────────

  createPublicWebSocket(
    onMessage: (data: Record<string, unknown>) => void,
    onError?:  (error: Error) => void,
    onOpen?:   () => void,
    onClose?:  () => void,
  ): WebSocket {
    const PUBLIC_WS_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';

    logger.info('[DerivAPI] A criar WS público');
    const ws = new WebSocket(PUBLIC_WS_URL, { perMessageDeflate: false, handshakeTimeout: 10_000 });

    ws.on('open', () => {
      logger.info('[DerivAPI] WS público conectado');
      onOpen?.();
    });

    ws.on('message', (raw: WebSocket.RawData) => {
      let msg: Record<string, unknown>;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        logger.warn('[DerivAPI] Mensagem WS público não é JSON');
        return;
      }
      if (msg.msg_type === 'ping' || msg.msg_type === 'pong') return;
      onMessage(msg);
    });

    ws.on('error', (err: Error) => {
      logger.error('[DerivAPI] Erro no WS público', { message: err.message });
      onError?.(err);
    });

    ws.on('close', () => {
      logger.info('[DerivAPI] WS público fechado');
      onClose?.();
    });

    return ws;
  }
}
