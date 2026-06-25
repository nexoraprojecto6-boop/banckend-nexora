// ============================================================
// NEXORA FOREX — Auth Service (OAuth2 + PKCE)
// ============================================================
// Correcções vs versão anterior:
//   ✅ Sessões PKCE guardadas em Redis (não global.authSessions)
//      → Sobrevive a reinícios do Railway e funciona com múltiplas
//        instâncias em paralelo
//   ✅ TTL de 10 minutos no Redis para sessões PKCE
//   ✅ exchangeCodeForToken: tratamento de erros melhorado
//   ✅ getOtpUrl: novo método para obter URL WS privada com OTP
// ============================================================

import crypto from 'crypto';
import { v4 as uuidv4 } from 'uuid';
import axios from 'axios';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';
import { AuthenticationError } from '../types/errors.js';
import { redisClient } from '@utils/redis.js';

// ─── Tipos ───────────────────────────────────────────────────

interface PKCEPair {
  codeVerifier: string;
  codeChallenge: string;
}

export interface TokenResponse {
  accessToken: string;
  expiresIn: number;
  tokenType: string;
}

export interface UserSession {
  accessToken: string;
  expiresAt: number;
  userId: string;
  refreshToken?: string;
}

// ─── AuthService ─────────────────────────────────────────────

export class AuthService {

  // ── PKCE ─────────────────────────────────────────────────

  static generatePKCE(): PKCEPair {
    // code_verifier: 64 bytes aleatórios, chars seguros para URL
    const codeVerifier = Array.from(crypto.getRandomValues(new Uint8Array(64)))
      .map((v) => 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~'[v % 66])
      .join('');

    const hash = crypto.createHash('sha256').update(codeVerifier).digest();
    const codeChallenge = Buffer.from(hash)
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');

    return { codeVerifier, codeChallenge };
  }

  static generateState(): string {
    return crypto.randomBytes(16).toString('hex');
  }

  // ── URL de autorização ───────────────────────────────────

  static async buildAuthorizationUrl(options: {
    prompt?: 'login' | 'registration';
    sidc?: string;
    utmCampaign?: string;
    utmMedium?: string;
    utmSource?: string;
  } = {}): Promise<string> {
    const pkce = this.generatePKCE();
    const state = this.generateState();

    // Guardar em Redis com TTL de 10 minutos
    // (era global.authSessions — perde-se ao reiniciar Railway)
    await redisClient.set(
      `pkce:${state}`,
      JSON.stringify({ codeVerifier: pkce.codeVerifier, state }),
      { EX: 600 }, // 10 minutos
    );

    const params = new URLSearchParams({
      response_type:         'code',
      client_id:             config.deriv.clientId,
      redirect_uri:          config.deriv.redirectUri,
      scope:                 'trade',
      state,
      code_challenge:        pkce.codeChallenge,
      code_challenge_method: 'S256',
      ...(options.prompt       && { prompt:        options.prompt }),
      ...(options.sidc         && { sidc:          options.sidc }),
      ...(options.utmCampaign  && { utm_campaign:  options.utmCampaign }),
      ...(options.utmMedium    && { utm_medium:    options.utmMedium }),
      ...(options.utmSource    && { utm_source:    options.utmSource }),
    });

    return `${config.deriv.authBaseUrl}/auth?${params.toString()}`;
  }

  // ── Troca de código por token ────────────────────────────

  static async exchangeCodeForToken(
    code: string,
    state: string,
  ): Promise<TokenResponse> {
    // Recuperar codeVerifier do Redis
    const raw = await redisClient.get(`pkce:${state}`);
    if (!raw) {
      throw new AuthenticationError('State inválido ou expirado');
    }

    const { codeVerifier } = JSON.parse(raw) as { codeVerifier: string };

    // Apagar imediatamente (uso único)
    await redisClient.del(`pkce:${state}`);

    try {
      const tokenResponse = await axios.post(
        `${config.deriv.authBaseUrl}/token`,
        new URLSearchParams({
          grant_type:    'authorization_code',
          client_id:     config.deriv.clientId,
          code,
          code_verifier: codeVerifier,
          redirect_uri:  config.deriv.redirectUri,
        }),
        { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } },
      );

      const { access_token, expires_in, token_type } = tokenResponse.data;
      logger.info('[AuthService] Token obtido com sucesso', { expiresIn: expires_in });

      return {
        accessToken: access_token,
        expiresIn:   expires_in,
        tokenType:   token_type,
      };
    } catch (error) {
      if (axios.isAxiosError(error)) {
        const derivError = error.response?.data?.error;
        logger.error('[AuthService] Falha na troca de token', {
          status:  error.response?.status,
          message: derivError?.message,
          code:    derivError?.code,
        });
        throw new AuthenticationError(
          derivError?.message ?? 'Falha ao trocar código por token',
        );
      }
      throw error;
    }
  }

  // ── OTP → URL WS privada ─────────────────────────────────

  /**
   * Obtém a URL WebSocket privada com OTP para uma conta.
   * Deve ser chamado SEMPRE antes de criar/reconectar o WebSocket.
   *
   * POST https://api.derivws.com/trading/v1/options/accounts/{accountId}/otp
   * Authorization: Bearer {accessToken}
   *
   * Resposta: { data: { url: "wss://api.derivws.com/trading/v1/options/ws/...?otp=..." } }
   */
  static async getOtpUrl(params: {
    accessToken: string;
    accountId: string;
  }): Promise<string> {
    const endpoint = `${config.deriv.apiBaseUrl}/trading/v1/options/accounts/${params.accountId}/otp`;

    try {
      const response = await axios.post(
        endpoint,
        {},
        {
          headers: {
            Authorization: `Bearer ${params.accessToken}`,
            'Content-Type': 'application/json',
          },
          timeout: 10_000,
        },
      );

      const url = response.data?.data?.url as string | undefined;
      if (!url || !url.startsWith('wss://')) {
        throw new Error('URL OTP inválida recebida da Deriv');
      }

      logger.debug('[AuthService] OTP URL obtida', {
        accountId: params.accountId,
        // nunca logar a URL completa (contém OTP)
      });

      return url;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        logger.error('[AuthService] Falha ao obter OTP URL', {
          status:    error.response?.status,
          accountId: params.accountId,
          data:      error.response?.data,
        });
        throw new AuthenticationError(
          `Falha ao obter OTP: ${error.response?.data?.message ?? error.message}`,
        );
      }
      throw error;
    }
  }

  // ── Sessão de utilizador ─────────────────────────────────

  static createSession(accessToken: string, expiresIn: number): UserSession {
    const userId = uuidv4();
    const expiresAt = Date.now() + expiresIn * 1_000;
    logger.info('[AuthService] Sessão criada', { userId });
    return { accessToken, expiresAt, userId };
  }

  static isTokenExpired(session: UserSession): boolean {
    // Considera expirado 60s antes para dar margem
    return Date.now() > session.expiresAt - 60_000;
  }

  static extractBearerToken(authHeader?: string): string {
    if (!authHeader?.startsWith('Bearer ')) {
      throw new AuthenticationError('Header de autorização ausente ou inválido');
    }
    return authHeader.slice(7);
  }
}
