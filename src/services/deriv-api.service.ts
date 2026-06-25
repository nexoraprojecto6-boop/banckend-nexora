// ============================================================
// NEXORA FOREX — Deriv API Service
// ============================================================
// Correcções aplicadas vs versão anterior:
//   ✅ Timeouts em todos os axios calls (evita travamentos)
//   ✅ createAuthenticatedWebSocket: removido ping duplicado
//      (o server.ts já gere o heartbeat — dois pings causavam
//       rate limit em sessões com muitos bots activos)
//   ✅ Mensagens de erro mais detalhadas nos logs
//   ✅ createPublicWebSocket: sem re-exportar — usar apenas
//      para dados públicos (active_symbols, etc.)
// ============================================================

import axios from 'axios';
import WebSocket from 'ws';
import logger from '@utils/logger.js';
import { config } from '@config/index.js';

const DERIV_REST_BASE = 'https://api.derivws.com';
const REQUEST_TIMEOUT = 15_000; // 15s — axios default é infinito

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

interface OTPResponse {
  data: { url: string };
}

interface AccountsResponse {
  data: DerivAccount[];
}

// ─── DerivAPIService ──────────────────────────────────────────

export class DerivAPIService {
  private readonly token: string;

  constructor(token: string) {
    if (!token) throw new Error('DerivAPIService: token é obrigatório');
    this.token = token;
  }

  // ── Headers REST ─────────────────────────────────────────

  private get headers() {
    return {
      Authorization:  `Bearer ${this.token}`,
      'Content-Type': 'application/json',
      // Deriv-App-ID apenas se configurado (novo endpoint pode não exigir)
      ...(config.deriv.appId ? { 'Deriv-App-ID': config.deriv.appId } : {}),
    };
  }

  // ── Contas ───────────────────────────────────────────────

  /**
   * GET /trading/v1/options/accounts
   * Lista todas as contas Options do utilizador.
   */
  async getAccounts(): Promise<AccountsResponse> {
    try {
      const response = await axios.get<AccountsResponse>(
        `${DERIV_REST_BASE}/trading/v1/options/accounts`,
        { headers: this.headers, timeout: REQUEST_TIMEOUT },
      );
      logger.info('[DerivAPI] Contas obtidas', { count: response.data.data?.length ?? 0 });
      return response.data;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        logger.error('[DerivAPI] Falha ao obter contas', {
          status: error.response?.status,
          data:   error.response?.data,
        });
        throw new Error(
          error.response?.data?.message ?? `Falha ao obter contas (HTTP ${error.response?.status})`,
        );
      }
      throw error;
    }
  }

  // ── OTP → URL WebSocket Privada ──────────────────────────

  /**
   * POST /trading/v1/options/accounts/{accountId}/otp
   * Gera OTP e retorna a URL WebSocket autenticada.
   *
   * IMPORTANTE: cada OTP é de uso único e expira rapidamente.
   * Deve ser chamado SEMPRE que se abre ou reconecta um WS.
   */
  async getOTP(accountId: string): Promise<{ url: string; wsUrl: string }> {
    try {
      const response = await axios.post<OTPResponse>(
        `${DERIV_REST_BASE}/trading/v1/options/accounts/${accountId}/otp`,
        {},
        { headers: this.headers, timeout: REQUEST_TIMEOUT },
      );

      const url = response.data?.data?.url;
      if (!url || !url.startsWith('wss://')) {
        throw new Error('URL OTP inválida ou em formato inesperado');
      }

      logger.info('[DerivAPI] OTP obtido', { accountId });
      return { url, wsUrl: url };
    } catch (error) {
      if (axios.isAxiosError(error)) {
        logger.error('[DerivAPI] Falha ao obter OTP', {
          accountId,
          status: error.response?.status,
          data:   error.response?.data,
        });
        throw new Error(
          error.response?.data?.message ?? `Falha ao obter OTP (HTTP ${error.response?.status})`,
        );
      }
      throw error;
    }
  }

  // ── Health Check ─────────────────────────────────────────

  async healthCheck(): Promise<{ status: string }> {
    try {
      const response = await axios.get<{ status: string }>(
        `${DERIV_REST_BASE}/v1/health`,
        { headers: this.headers, timeout: 5_000 },
      );
      return response.data;
    } catch {
      return { status: 'error' };
    }
  }

  // ── WebSocket Autenticado (via OTP) ──────────────────────

  /**
   * Cria um WebSocket autenticado usando a URL com OTP.
   *
   * NÃO inclui ping próprio — o server.ts gere o heartbeat
   * da sessão para evitar envios duplicados e rate limit.
   *
   * Callbacks: onMessage, onError, onClose, onOpen
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

    const ws = new WebSocket(wsUrl);

    ws.on('open', () => {
      logger.info('[DerivAPI] WS autenticado conectado');
      onOpen?.();
    });

    ws.on('message', (raw: WebSocket.RawData) => {
      let msg: Record<string, unknown>;
      try {
        msg = JSON.parse(raw.toString('utf8'));
      } catch {
        logger.warn('[DerivAPI] Mensagem WS não é JSON');
        return;
      }

      // Ping/pong: silencioso — não passar ao caller
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

  // ── WebSocket Público (sem autenticação) ─────────────────

  /**
   * Cria um WebSocket público para dados de mercado:
   * ticks, active_symbols, contracts_for, candles, etc.
   *
   * Não requer OTP nem access_token.
   */
  createPublicWebSocket(
    onMessage: (data: Record<string, unknown>) => void,
    onError?:  (error: Error) => void,
    onOpen?:   () => void,
    onClose?:  () => void,
  ): WebSocket {
    const PUBLIC_WS_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';

    logger.info('[DerivAPI] A criar WS público');
    const ws = new WebSocket(PUBLIC_WS_URL);

    ws.on('open', () => {
      logger.info('[DerivAPI] WS público conectado');
      onOpen?.();
    });

    ws.on('message', (raw: WebSocket.RawData) => {
      let msg: Record<string, unknown>;
      try {
        msg = JSON.parse(raw.toString('utf8'));
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
