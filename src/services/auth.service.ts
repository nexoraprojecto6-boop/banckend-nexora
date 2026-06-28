// ============================================================
// NEXORA FOREX — Auth Service (OAuth2 + PKCE via Cookie)
//
// CORRECÇÃO: SameSite=None (era Lax) para funcionar com
// frontend Vercel + backend Railway (domínios diferentes).
//
// SameSite=Lax bloqueia cookies em redirects cross-origin —
// exactamente o que acontece no callback OAuth:
//   Deriv → Railway /api/auth/callback (cross-origin redirect)
// O browser descartava o cookie antes de o Railway o conseguir
// ler, causando "Cookie PKCE não encontrado".
//
// SameSite=None requer Secure=true (só HTTPS) — o que é sempre
// o caso em Railway + Vercel em produção.
// Em desenvolvimento (HTTP), usamos SameSite=Lax como fallback.
// ============================================================

import crypto from 'crypto';
import { v4 as uuidv4 } from 'uuid';
import axios from 'axios';
import { Request, Response } from 'express';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';
import { AuthenticationError } from '../types/errors.js';

// ─── Tipos ───────────────────────────────────────────────────

interface PKCEPair {
  codeVerifier:  string;
  codeChallenge: string;
}

export interface TokenResponse {
  accessToken: string;
  expiresIn:   number;
  tokenType:   string;
}

export interface UserSession {
  accessToken:   string;
  expiresAt:     number;
  userId:        string;
  refreshToken?: string;
}

// ─── Constantes ───────────────────────────────────────────────

const COOKIE_NAME    = 'nexora_pkce';
const COOKIE_MAX_AGE = 600; // 10 minutos em segundos
const HMAC_SEP       = '.';

// ─── HMAC helpers ─────────────────────────────────────────────

function sign(payload: string): string {
  const sig = crypto
    .createHmac('sha256', config.security.jwtSecret)
    .update(payload)
    .digest('base64url');
  return `${payload}${HMAC_SEP}${sig}`;
}

function verify(signed: string): string | null {
  const idx = signed.lastIndexOf(HMAC_SEP);
  if (idx === -1) return null;
  const payload  = signed.slice(0, idx);
  const expected = crypto
    .createHmac('sha256', config.security.jwtSecret)
    .update(payload)
    .digest('base64url');
  const actual = signed.slice(idx + 1);
  if (actual.length !== expected.length) return null;
  if (!crypto.timingSafeEqual(Buffer.from(actual), Buffer.from(expected))) return null;
  return payload;
}

// ─── Cookie helpers ───────────────────────────────────────────

export function setPkceCookie(
  res: Response,
  codeVerifier: string,
  state: string,
): void {
  const payload = `${codeVerifier}:${state}`;
  const signed  = sign(payload);

  // CORRECÇÃO CRÍTICA: SameSite=None para cross-origin OAuth redirect.
  // Railway (backend) e Vercel (frontend) são domínios diferentes —
  // o redirect da Deriv para o callback é cross-origin, por isso
  // SameSite=Lax/Strict impede o browser de enviar o cookie.
  // SameSite=None requer sempre Secure=true (apenas HTTPS).
  // Em dev (HTTP local), usamos Lax como fallback seguro.
  const isProduction = config.server.isProduction;

  res.cookie(COOKIE_NAME, signed, {
    httpOnly: true,
    secure:   isProduction,           // HTTPS obrigatório em produção
    sameSite: isProduction ? 'none' : 'lax', // none para cross-origin em prod
    maxAge:   COOKIE_MAX_AGE * 1000,
    path:     '/api/auth',
  });
}

export function readAndClearPkceCookie(
  req: Request,
  res: Response,
  expectedState: string,
): { codeVerifier: string } {
  const raw = req.cookies?.[COOKIE_NAME] as string | undefined;

  if (!raw) {
    throw new AuthenticationError(
      'Cookie PKCE não encontrado. O fluxo de login pode ter expirado (>10 min) ou o browser bloqueou cookies.',
    );
  }

  // Apagar imediatamente — uso único
  res.clearCookie(COOKIE_NAME, {
    path:     '/api/auth',
    httpOnly: true,
    secure:   config.server.isProduction,
    sameSite: config.server.isProduction ? 'none' : 'lax',
  });

  const payload = verify(raw);
  if (!payload) {
    throw new AuthenticationError('Cookie PKCE inválido ou adulterado.');
  }

  const separatorIdx = payload.indexOf(':');
  if (separatorIdx === -1) {
    throw new AuthenticationError('Formato de cookie PKCE inválido.');
  }

  const codeVerifier = payload.slice(0, separatorIdx);
  const cookieState  = payload.slice(separatorIdx + 1);

  if (cookieState !== expectedState) {
    throw new AuthenticationError('State PKCE não corresponde. Possível ataque CSRF.');
  }

  return { codeVerifier };
}

// ─── AuthService ─────────────────────────────────────────────

export class AuthService {

  static generatePKCE(): PKCEPair {
    const codeVerifier = Array.from(crypto.getRandomValues(new Uint8Array(64)))
      .map(v => 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~'[v % 66])
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

  // Síncrono — sem Redis, sem await
  static buildAuthorizationUrl(options: {
    prompt?:      'login' | 'registration';
    sidc?:        string;
    utmCampaign?: string;
    utmMedium?:   string;
    utmSource?:   string;
  } = {}): { url: string; codeVerifier: string; state: string } {
    const pkce  = this.generatePKCE();
    const state = this.generateState();

    const params = new URLSearchParams({
      response_type:         'code',
      client_id:             config.deriv.clientId,
      redirect_uri:          config.deriv.redirectUri,
      scope:                 'trade',
      state,
      code_challenge:        pkce.codeChallenge,
      code_challenge_method: 'S256',
      ...(options.prompt      && { prompt:       options.prompt }),
      ...(options.sidc        && { sidc:         options.sidc }),
      ...(options.utmCampaign && { utm_campaign: options.utmCampaign }),
      ...(options.utmMedium   && { utm_medium:   options.utmMedium }),
      ...(options.utmSource   && { utm_source:   options.utmSource }),
    });

    return {
      url:          `${config.deriv.authBaseUrl}/auth?${params.toString()}`,
      codeVerifier: pkce.codeVerifier,
      state,
    };
  }

  static async exchangeCodeForToken(
    code:         string,
    codeVerifier: string,
  ): Promise<TokenResponse> {
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

  static async getOtpUrl(params: {
    accessToken: string;
    accountId:   string;
  }): Promise<string> {
    const endpoint = `${config.deriv.apiBaseUrl}/trading/v1/options/accounts/${params.accountId}/otp`;
    try {
      const response = await axios.post(endpoint, {}, {
        headers: {
          Authorization:  `Bearer ${params.accessToken}`,
          'Content-Type': 'application/json',
        },
        timeout: 10_000,
      });

      const url = response.data?.data?.url as string | undefined;
      if (!url || !url.startsWith('wss://')) {
        throw new Error('URL OTP inválida recebida da Deriv');
      }

      logger.debug('[AuthService] OTP URL obtida', { accountId: params.accountId });
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

  static createSession(accessToken: string, expiresIn: number): UserSession {
    const userId    = uuidv4();
    const expiresAt = Date.now() + expiresIn * 1_000;
    logger.info('[AuthService] Sessão criada', { userId });
    return { accessToken, expiresAt, userId };
  }

  static isTokenExpired(session: UserSession): boolean {
    return Date.now() > session.expiresAt - 60_000;
  }

  static extractBearerToken(authHeader?: string): string {
    if (!authHeader?.startsWith('Bearer ')) {
      throw new AuthenticationError('Header de autorização ausente ou inválido');
    }
    return authHeader.slice(7);
  }
}
