// ============================================================
// NEXORA FOREX — Auth Service v2 (OAuth2 + PKCE via Redis)
// ============================================================
// Correcções vs v1:
//   ✅ code_verifier gerado com randomBytes + base64url (o anterior usava
//      "v % 66" → enviesado, e getRandomValues do módulo crypto)
//   ✅ state com 32 bytes
//   ✅ Erros OAuth lidos no formato padrão (error / error_description)
//      — a v1 só lia error.message e perdia o motivo real
//   ✅ Timeout nos pedidos HTTP (token e OTP); retry no OTP
//   ✅ Fallback em memória com limpeza periódica (crescia sem limite)
//   ✅ refresh_token guardado se a Deriv o devolver
// ============================================================

import crypto   from 'crypto'
import { v4 as uuidv4 } from 'uuid'
import axios    from 'axios'
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
  accessToken:   string
  expiresIn:     number
  tokenType:     string
  refreshToken?: string
}

export interface UserSession {
  accessToken:   string
  expiresAt:     number
  userId:        string
  refreshToken?: string
}

// ─── Constantes ───────────────────────────────────────────────
const PKCE_TTL_SECONDS = 600
const HTTP_TIMEOUT_MS  = 15_000

function pkceKey(state: string): string {
  return `pkce:${state}`
}

// ─── Fallback em memória ──────────────────────────────────────
const memoryStore = new Map<string, { verifier: string; expiresAt: number }>()

const cleanupTimer = setInterval(() => {
  const now = Date.now()
  for (const [k, v] of memoryStore) {
    if (v.expiresAt < now) memoryStore.delete(k)
  }
}, 60_000)
cleanupTimer.unref?.()

async function storePkce(state: string, codeVerifier: string): Promise<void> {
  const key = pkceKey(state)

  if (isRedisConnected()) {
    try {
      await getRedisClient().set(key, codeVerifier, { EX: PKCE_TTL_SECONDS })
      logger.debug('[Auth] PKCE guardado no Redis')
      return
    } catch (err) {
      logger.warn('[Auth] Falha ao guardar PKCE no Redis, usando memória', { err })
    }
  }

  memoryStore.set(key, { verifier: codeVerifier, expiresAt: Date.now() + PKCE_TTL_SECONDS * 1000 })
  logger.debug('[Auth] PKCE guardado em memória (fallback)')
}

async function retrieveAndDeletePkce(state: string): Promise<string> {
  const key = pkceKey(state)

  if (isRedisConnected()) {
    try {
      const verifier = await getRedisClient().getDel(key)
      if (verifier) return verifier
    } catch (err) {
      logger.warn('[Auth] Falha ao ler PKCE do Redis, tentando memória', { err })
    }
  }

  const entry = memoryStore.get(key)
  if (!entry) {
    throw new AuthenticationError(
      'Estado PKCE não encontrado. O fluxo de login pode ter expirado (>10 min) ou foi iniciado noutra instância do servidor.',
    )
  }
  memoryStore.delete(key)
  if (Date.now() > entry.expiresAt) {
    throw new AuthenticationError('Estado PKCE expirado. Inicia o login novamente.')
  }
  return entry.verifier
}

// ─── Erros OAuth (formato padrão RFC 6749 + envelope Deriv) ───
function oauthErrorInfo(error: unknown): { code?: string; message?: string; status?: number } {
  if (!axios.isAxiosError(error)) return {}
  const data = error.response?.data as any
  const status = error.response?.status
  if (typeof data?.error === 'string') {
    return { status, code: data.error, message: data.error_description ?? data.error }
  }
  const first = data?.errors?.[0]
  return {
    status,
    code:    data?.error?.code ?? first?.code,
    message: data?.error?.message ?? first?.message ?? data?.message,
  }
}

// ─── AuthService ─────────────────────────────────────────────

export class AuthService {

  static generatePKCE(): PKCEPair {
    // 64 bytes → 86 caracteres base64url (RFC 7636: 43–128)
    const codeVerifier = crypto.randomBytes(64).toString('base64url')
    const codeChallenge = crypto.createHash('sha256').update(codeVerifier).digest('base64url')
    return { codeVerifier, codeChallenge }
  }

  static generateState(): string {
    return crypto.randomBytes(32).toString('hex')
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

    return { url: `${config.deriv.authBaseUrl}/auth?${params.toString()}`, state }
  }

  static async retrievePkce(state: string): Promise<string> {
    return retrieveAndDeletePkce(state)
  }

  static async exchangeCodeForToken(code: string, codeVerifier: string): Promise<TokenResponse> {
    try {
      const response = await axios.post(
        `${config.deriv.authBaseUrl}/token`,
        new URLSearchParams({
          grant_type:    'authorization_code',
          client_id:     config.deriv.clientId,
          code,
          code_verifier: codeVerifier,
          redirect_uri:  config.deriv.redirectUri,
        }),
        {
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          timeout: HTTP_TIMEOUT_MS,
        },
      )

      const { access_token, expires_in, token_type, refresh_token } = response.data
      if (!access_token) throw new AuthenticationError('Resposta da Deriv sem access_token')

      logger.info('[AuthService] Token obtido com sucesso', { expiresIn: expires_in })
      return {
        accessToken:  access_token,
        expiresIn:    Number(expires_in) || 3600,
        tokenType:    token_type ?? 'Bearer',
        refreshToken: refresh_token,
      }
    } catch (error) {
      if (error instanceof AuthenticationError) throw error
      if (axios.isAxiosError(error)) {
        const info = oauthErrorInfo(error)
        logger.error('[AuthService] Falha na troca de token', info)
        throw new AuthenticationError(info.message ?? 'Falha ao trocar código por token')
      }
      throw error
    }
  }

  static async getOtpUrl(params: { accessToken: string; accountId: string }): Promise<string> {
    const endpoint = `${config.deriv.apiBaseUrl}/trading/v1/options/accounts/${params.accountId}/otp`
    const headers = {
      Authorization:  `Bearer ${params.accessToken}`,
      'Content-Type': 'application/json',
    }

    let lastError: unknown
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const response = await axios.post(endpoint, {}, { headers, timeout: 10_000 })
        const url = response.data?.data?.url as string | undefined
        if (!url || !url.startsWith('wss://')) throw new Error('URL OTP inválida recebida da Deriv')
        logger.debug('[AuthService] OTP URL obtida', { accountId: params.accountId })
        return url
      } catch (error) {
        lastError = error
        const status = axios.isAxiosError(error) ? error.response?.status : undefined
        const retryable = axios.isAxiosError(error) && (!status || [429, 502, 503, 504].includes(status))
        if (!retryable || attempt === 2) break
        await new Promise((r) => setTimeout(r, 300 * 2 ** attempt))
      }
    }

    if (axios.isAxiosError(lastError)) {
      const info = oauthErrorInfo(lastError)
      logger.error('[AuthService] Falha ao obter OTP URL', { accountId: params.accountId, ...info })
      throw new AuthenticationError(`Falha ao obter OTP: ${info.message ?? lastError.message}`)
    }
    throw lastError
  }

  static createSession(accessToken: string, expiresIn: number, refreshToken?: string): UserSession {
    const userId    = uuidv4()
    const expiresAt = Date.now() + expiresIn * 1_000
    logger.info('[AuthService] Sessão criada', { userId })
    return { accessToken, expiresAt, userId, refreshToken }
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
