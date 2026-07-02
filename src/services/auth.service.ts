// ============================================================
// NEXORA FOREX — Auth Service (OAuth2 + PKCE via Redis)
//
// CORRECÇÃO: substituição de cookies por Redis para armazenar
// o par (codeVerifier, state) durante o fluxo OAuth.
//
// POR QUÊ:
//   Cookies SameSite=None são bloqueados por browsers modernos
//   (Safari ITP, Chrome 3rd-party cookie phase-out, Firefox ETP)
//   em redirects cross-origin — exactamente o que acontece no
//   callback OAuth: Deriv → Render (domínios diferentes).
//   O browser descartava o cookie antes de o Render o conseguir
//   ler, causando "Cookie PKCE não encontrado".
//
// SOLUÇÃO:
//   O state OAuth (já gerado por segurança CSRF) serve como
//   chave única no Redis. O codeVerifier é guardado em Redis
//   com TTL de 10 minutos. Nenhum cookie necessário.
//
// FLUXO:
//   1. /login  → gera PKCE + state → guarda codeVerifier em
//               Redis com chave "pkce:{state}" → TTL 600s
//   2. /callback → lê e apaga codeVerifier do Redis usando
//                  o state recebido da Deriv como chave
// ============================================================

import crypto   from 'crypto'
import { v4 as uuidv4 } from 'uuid'
import axios    from 'axios'
import { Request } from 'express'
import { config } from '@config/index.js'
import logger   from '@utils/logger.js'
import { getRedisClient, isRedisConnected } from '@utils/redis.js'
import { AuthenticationError } from '../types/errors.js'

// ─── Tipos ───────────────────────────────────────────────────

interface PKCEPair {
  codeVerifier:  string
  codeChallenge: string
}

export interface TokenResponse {
  accessToken: string
  expiresIn:   number
  tokenType:   string
}

export interface UserSession {
  accessToken:   string
  expiresAt:     number
  userId:        string
  refreshToken?: string
}

// ─── Constantes ───────────────────────────────────────────────
const PKCE_TTL_SECONDS = 600  // 10 minutos

function pkceKey(state: string): string {
  return `pkce:${state}`
}

// ─── Fallback em memória (quando Redis não está disponível) ───
// Garante que o fluxo funciona mesmo em dev sem Redis.
const memoryStore = new Map<string, { verifier: string; expiresAt: number }>()

async function storePkce(state: string, codeVerifier: string): Promise<void> {
  const key = pkceKey(state)

  if (isRedisConnected()) {
    try {
      const client = getRedisClient()
      await client.set(key, codeVerifier, { EX: PKCE_TTL_SECONDS })
      logger.debug('[Auth] PKCE guardado no Redis', { state })
      return
    } catch (err) {
      logger.warn('[Auth] Falha ao guardar PKCE no Redis, usando memória', { err })
    }
  }

  // Fallback: memória
  memoryStore.set(key, {
    verifier: codeVerifier,
    expiresAt: Date.now() + PKCE_TTL_SECONDS * 1000,
  })
  logger.debug('[Auth] PKCE guardado em memória (fallback)', { state })
}

async function retrieveAndDeletePkce(state: string): Promise<string> {
  const key = pkceKey(state)

  if (isRedisConnected()) {
    try {
      const client = getRedisClient()
      const verifier = await client.getDel(key)
      if (verifier) {
        logger.debug('[Auth] PKCE lido e apagado do Redis', { state })
        return verifier
      }
    } catch (err) {
      logger.warn('[Auth] Falha ao ler PKCE do Redis, tentando memória', { err })
    }
  }

  // Fallback: memória
  const entry = memoryStore.get(key)
  if (!entry) {
    throw new AuthenticationError(
      'Estado PKCE não encontrado. O fluxo de login pode ter expirado (>10 min) ou foi iniciado noutra sessão de servidor.',
    )
  }
  if (Date.now() > entry.expiresAt) {
    memoryStore.delete(key)
    throw new AuthenticationError('Estado PKCE expirado. Inicia o login novamente.')
  }
  memoryStore.delete(key)
  logger.debug('[Auth] PKCE lido e apagado da memória (fallback)', { state })
  return entry.verifier
}

// ─── AuthService ─────────────────────────────────────────────

export class AuthService {

  static generatePKCE(): PKCEPair {
    const codeVerifier = Array.from(crypto.getRandomValues(new Uint8Array(64)))
      .map(v => 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~'[v % 66])
      .join('')

    const hash = crypto.createHash('sha256').update(codeVerifier).digest()
    const codeChallenge = Buffer.from(hash)
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '')

    return { codeVerifier, codeChallenge }
  }

  static generateState(): string {
    return crypto.randomBytes(16).toString('hex')
  }

  static async buildAuthorizationUrl(options: {
    prompt?:      'login' | 'registration'
    sidc?:        string
    utmCampaign?: string
    utmMedium?:   string
    utmSource?:   string
  } = {}): Promise<{ url: string; state: string }> {
    const pkce  = this.generatePKCE()
    const state = this.generateState()

    // Guardar codeVerifier no Redis (ou memória) — sem cookie
    await storePkce(state, pkce.codeVerifier)

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
    })

    return {
      url:   `${config.deriv.authBaseUrl}/auth?${params.toString()}`,
      state,
    }
  }

  // Recupera e apaga o codeVerifier do Redis (uso único)
  static async retrievePkce(state: string): Promise<string> {
    return retrieveAndDeletePkce(state)
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
      )

      const { access_token, expires_in, token_type } = tokenResponse.data
      logger.info('[AuthService] Token obtido com sucesso', { expiresIn: expires_in })

      return {
        accessToken: access_token,
        expiresIn:   expires_in,
        tokenType:   token_type,
      }
    } catch (error) {
      if (axios.isAxiosError(error)) {
        const derivError = error.response?.data?.error
        logger.error('[AuthService] Falha na troca de token', {
          status:  error.response?.status,
          message: derivError?.message,
          code:    derivError?.code,
        })
        throw new AuthenticationError(
          derivError?.message ?? 'Falha ao trocar código por token',
        )
      }
      throw error
    }
  }

  static async getOtpUrl(params: {
    accessToken: string
    accountId:   string
  }): Promise<string> {
    const endpoint = `${config.deriv.apiBaseUrl}/trading/v1/options/accounts/${params.accountId}/otp`
    try {
      const response = await axios.post(endpoint, {}, {
        headers: {
          Authorization:  `Bearer ${params.accessToken}`,
          'Content-Type': 'application/json',
        },
        timeout: 10_000,
      })

      const url = response.data?.data?.url as string | undefined
      if (!url || !url.startsWith('wss://')) {
        throw new Error('URL OTP inválida recebida da Deriv')
      }

      logger.debug('[AuthService] OTP URL obtida', { accountId: params.accountId })
      return url
    } catch (error) {
      if (axios.isAxiosError(error)) {
        logger.error('[AuthService] Falha ao obter OTP URL', {
          status:    error.response?.status,
          accountId: params.accountId,
          data:      error.response?.data,
        })
        throw new AuthenticationError(
          `Falha ao obter OTP: ${error.response?.data?.message ?? (error as Error).message}`,
        )
      }
      throw error
    }
  }

  static createSession(accessToken: string, expiresIn: number): UserSession {
    const userId    = uuidv4()
    const expiresAt = Date.now() + expiresIn * 1_000
    logger.info('[AuthService] Sessão criada', { userId })
    return { accessToken, expiresAt, userId }
  }

  static isTokenExpired(session: UserSession): boolean {
    return Date.now() > session.expiresAt - 60_000
  }

  static extractBearerToken(authHeader?: string): string {
    if (!authHeader?.startsWith('Bearer ')) {
      throw new AuthenticationError('Header de autorização ausente ou inválido')
    }
    return authHeader.slice(7)
  }
}
