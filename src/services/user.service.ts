// ============================================================
// NEXORA — User Service (login local, email + senha)
//
// Independente do fluxo OAuth da Deriv (auth.service.ts).
// Guarda utilizadores locais em Redis, com fallback em memória
// seguindo o mesmo padrão já usado no PKCE (auth.service.ts).
//
// Chaves Redis:
//   user:{id}                 -> JSON do utilizador
//   user:email:{email}        -> id  (lookup rápido por email)
//   users:index                -> SET com todos os ids (listagens/stats)
//   email-verify:{token}       -> id  (token de verificação, TTL 24h)
// ============================================================

import bcrypt from 'bcryptjs'
import crypto from 'crypto'
import { v4 as uuidv4 } from 'uuid'
import { getRedisClient, isRedisConnected } from '@utils/redis.js'
import logger from '@utils/logger.js'
import { AuthenticationError } from '../types/errors.js'
import { EmailService } from './email.service.js'

const SALT_ROUNDS = 12
const INACTIVE_AFTER_DAYS = 30
const VERIFY_TOKEN_TTL_SECONDS = 24 * 60 * 60 // 24h

export interface LocalUser {
  id: string
  fullName: string
  whatsapp: string
  email: string
  passwordHash: string
  role: 'user' | 'admin'
  status: 'active' | 'blocked'
  emailVerified: boolean
  createdAt: number
  lastLoginAt: number | null
}

export type PublicUser = Omit<LocalUser, 'passwordHash'>

function toPublic(user: LocalUser): PublicUser {
  const { passwordHash, ...rest } = user
  return rest
}

function userKey(id: string): string {
  return `user:${id}`
}
function emailKey(email: string): string {
  return `user:email:${email.toLowerCase().trim()}`
}
function verifyKey(token: string): string {
  return `email-verify:${token}`
}
const INDEX_KEY = 'users:index'

// ─── Fallback em memória ──────────────────────────────────────
const memUsers = new Map<string, LocalUser>()
const memEmailIndex = new Map<string, string>()
const memIndex = new Set<string>()
const memVerifyTokens = new Map<string, { userId: string; expiresAt: number }>()

export class UserService {

  static async register(input: {
    fullName: string
    whatsapp: string
    email: string
    password: string
  }): Promise<PublicUser> {
    const normalizedEmail = input.email.toLowerCase().trim()

    const existing = await this.findByEmail(normalizedEmail)
    if (existing) {
      throw new AuthenticationError('Este email já está registado')
    }

    const isFirstUser = (await this.countAll()) === 0

    const user: LocalUser = {
      id: uuidv4(),
      fullName: input.fullName.trim(),
      whatsapp: input.whatsapp.trim(),
      email: normalizedEmail,
      passwordHash: await bcrypt.hash(input.password, SALT_ROUNDS),
      role: isFirstUser ? 'admin' : 'user',
      status: 'active',
      emailVerified: false,
      createdAt: Date.now(),
      lastLoginAt: null,
    }

    await this.persist(user)
    logger.info('[UserService] Novo utilizador registado', { id: user.id, email: user.email, role: user.role })

    // Dispara o email de verificação (não bloqueia o registo se falhar)
    const token = await this.generateEmailVerificationToken(user.id)
    await EmailService.sendVerificationEmail(user.email, user.fullName, token)

    return toPublic(user)
  }

  static async login(email: string, password: string): Promise<PublicUser> {
    const user = await this.findByEmail(email)
    if (!user) {
      throw new AuthenticationError('Email ou senha inválidos')
    }
    if (user.status === 'blocked') {
      throw new AuthenticationError('Conta bloqueada. Contacta o suporte.')
    }

    const valid = await bcrypt.compare(password, user.passwordHash)
    if (!valid) {
      throw new AuthenticationError('Email ou senha inválidos')
    }

    if (!user.emailVerified) {
      throw new AuthenticationError('Confirma o teu email antes de entrares. Verifica a tua caixa de entrada.')
    }

    user.lastLoginAt = Date.now()
    await this.persist(user)

    logger.info('[UserService] Login efetuado', { id: user.id, email: user.email })
    return toPublic(user)
  }

  // ─── Verificação de email ──────────────────────────────────

  static async generateEmailVerificationToken(userId: string): Promise<string> {
    const token = crypto.randomBytes(32).toString('hex')

    if (isRedisConnected()) {
      try {
        await getRedisClient().set(verifyKey(token), userId, { EX: VERIFY_TOKEN_TTL_SECONDS })
        return token
      } catch (err) {
        logger.warn('[UserService] Falha ao guardar token de verificação no Redis, a usar memória', { err })
      }
    }

    memVerifyTokens.set(token, { userId, expiresAt: Date.now() + VERIFY_TOKEN_TTL_SECONDS * 1000 })
    return token
  }

  static async verifyEmailToken(token: string): Promise<PublicUser> {
    let userId: string | null = null

    if (isRedisConnected()) {
      try {
        const client = getRedisClient()
        userId = await client.getDel(verifyKey(token))
      } catch (err) {
        logger.warn('[UserService] Falha ao ler token de verificação no Redis, a usar memória', { err })
      }
    }

    if (!userId) {
      const entry = memVerifyTokens.get(token)
      if (entry) {
        memVerifyTokens.delete(token)
        if (Date.now() <= entry.expiresAt) userId = entry.userId
      }
    }

    if (!userId) {
      throw new AuthenticationError('Link de verificação inválido ou expirado')
    }

    const user = await this.findById(userId)
    if (!user) {
      throw new AuthenticationError('Utilizador não encontrado')
    }

    user.emailVerified = true
    await this.persist(user)
    logger.info('[UserService] Email verificado', { id: user.id, email: user.email })
    return toPublic(user)
  }

  static async resendVerification(email: string): Promise<void> {
    const user = await this.findByEmail(email)
    if (!user) {
      // Não revela se o email existe ou não (evita enumeração de contas)
      return
    }
    if (user.emailVerified) {
      throw new AuthenticationError('Este email já está verificado')
    }
    const token = await this.generateEmailVerificationToken(user.id)
    await EmailService.sendVerificationEmail(user.email, user.fullName, token)
  }

  // ─── Consultas ──────────────────────────────────────────────

  static async findById(id: string): Promise<LocalUser | null> {
    if (isRedisConnected()) {
      try {
        const raw = await getRedisClient().get(userKey(id))
        return raw ? (JSON.parse(raw) as LocalUser) : null
      } catch (err) {
        logger.warn('[UserService] Falha ao ler utilizador no Redis, a usar memória', { err })
      }
    }
    return memUsers.get(id) ?? null
  }

  static async findByEmail(email: string): Promise<LocalUser | null> {
    const normalizedEmail = email.toLowerCase().trim()

    if (isRedisConnected()) {
      try {
        const client = getRedisClient()
        const id = await client.get(emailKey(normalizedEmail))
        if (!id) return null
        const raw = await client.get(userKey(id))
        return raw ? (JSON.parse(raw) as LocalUser) : null
      } catch (err) {
        logger.warn('[UserService] Falha ao procurar por email no Redis, a usar memória', { err })
      }
    }

    const id = memEmailIndex.get(normalizedEmail)
    if (!id) return null
    return memUsers.get(id) ?? null
  }

  static async listAll(): Promise<LocalUser[]> {
    if (isRedisConnected()) {
      try {
        const client = getRedisClient()
        const ids = await client.sMembers(INDEX_KEY)
        if (ids.length === 0) return []
        const raws = await Promise.all(ids.map((id) => client.get(userKey(id))))
        return raws.filter(Boolean).map((r) => JSON.parse(r as string) as LocalUser)
      } catch (err) {
        logger.warn('[UserService] Falha ao listar utilizadores no Redis, a usar memória', { err })
      }
    }
    return Array.from(memUsers.values())
  }

  static async countAll(): Promise<number> {
    if (isRedisConnected()) {
      try {
        return await getRedisClient().sCard(INDEX_KEY)
      } catch (err) {
        logger.warn('[UserService] Falha ao contar utilizadores no Redis, a usar memória', { err })
      }
    }
    return memIndex.size
  }

  static async getStats() {
    const users = await this.listAll()
    const now = Date.now()
    const inactiveThreshold = INACTIVE_AFTER_DAYS * 24 * 60 * 60 * 1000

    const total = users.length
    const blocked = users.filter((u) => u.status === 'blocked').length
    const verified = users.filter((u) => u.emailVerified).length
    const unverified = total - verified
    const neverLoggedIn = users.filter((u) => u.lastLoginAt === null).length
    const inactive = users.filter((u) => {
      if (u.status === 'blocked') return false
      if (u.lastLoginAt === null) return now - u.createdAt > inactiveThreshold
      return now - u.lastLoginAt > inactiveThreshold
    }).length
    const active = total - inactive - blocked

    return {
      total,
      active,
      inactive,
      blocked,
      verified,
      unverified,
      neverLoggedIn,
      inactiveAfterDays: INACTIVE_AFTER_DAYS,
    }
  }

  static async setStatus(id: string, status: 'active' | 'blocked'): Promise<PublicUser> {
    const user = await this.findById(id)
    if (!user) throw new AuthenticationError('Utilizador não encontrado')
    user.status = status
    await this.persist(user)
    return toPublic(user)
  }

  // ─── Persistência interna ────────────────────────────────
  private static async persist(user: LocalUser): Promise<void> {
    if (isRedisConnected()) {
      try {
        const client = getRedisClient()
        await client.set(userKey(user.id), JSON.stringify(user))
        await client.set(emailKey(user.email), user.id)
        await client.sAdd(INDEX_KEY, user.id)
        return
      } catch (err) {
        logger.warn('[UserService] Falha ao gravar no Redis, a usar memória', { err })
      }
    }
    memUsers.set(user.id, user)
    memEmailIndex.set(user.email, user.id)
    memIndex.add(user.id)
  }
}
