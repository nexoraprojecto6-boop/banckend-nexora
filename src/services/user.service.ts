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
//
// Admin: o role 'admin' é atribuído com base em config.admin.emails
// (env var ADMIN_EMAILS, lista separada por vírgulas). É recalculado
// a cada login para se manter em sincronia caso a lista mude depois
// do utilizador já ter sido criado.
//
// NOTA: a verificação de email foi removida. O registo já cria o
// utilizador com emailVerified: true e o login não faz mais essa
// checagem. O campo emailVerified foi mantido no tipo apenas para
// não quebrar as estatísticas do admin (getStats / AdminUserRow).
// ============================================================

import bcrypt from 'bcryptjs'
import { v4 as uuidv4 } from 'uuid'
import { getRedisClient, isRedisConnected } from '@utils/redis.js'
import logger from '@utils/logger.js'
import { config } from '@config/index.js'
import { AuthenticationError } from '../types/errors.js'
import { ConflictError } from '../types/errors.js'

const SALT_ROUNDS = 12
const INACTIVE_AFTER_DAYS = 30
const ONLINE_THRESHOLD_MS = 5 * 60 * 1000 // 5 min sem atividade = offline

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
  lastActiveAt: number | null
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
const INDEX_KEY = 'users:index'

function isAdminEmail(email: string): boolean {
  return config.admin.emails.includes(email.toLowerCase().trim())
}

// ─── Fallback em memória ──────────────────────────────────────
const memUsers = new Map<string, LocalUser>()
const memEmailIndex = new Map<string, string>()
const memIndex = new Set<string>()

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
      // 409 Conflict — o email já existe, não é um problema de autenticação.
      throw new ConflictError('Este email já está registado')
    }

    const user: LocalUser = {
      id: uuidv4(),
      fullName: input.fullName.trim(),
      whatsapp: input.whatsapp.trim(),
      email: normalizedEmail,
      passwordHash: await bcrypt.hash(input.password, SALT_ROUNDS),
      role: isAdminEmail(normalizedEmail) ? 'admin' : 'user',
      status: 'active',
      emailVerified: true, // verificação de email desativada — conta já entra ativa
      createdAt: Date.now(),
      lastLoginAt: null,
      lastActiveAt: null,
    }

    await this.persist(user)
    logger.info('[UserService] Novo utilizador registado', { id: user.id, email: user.email, role: user.role })

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

    // Recalcula o role a cada login, para refletir alterações na
    // lista ADMIN_EMAILS sem precisar de migração manual de dados.
    const shouldBeAdmin = isAdminEmail(user.email)
    if (shouldBeAdmin && user.role !== 'admin') user.role = 'admin'
    if (!shouldBeAdmin && user.role === 'admin') user.role = 'user'

    user.lastLoginAt = Date.now()
    user.lastActiveAt = Date.now()
    await this.persist(user)

    logger.info('[UserService] Login efetuado', { id: user.id, email: user.email, role: user.role })
    return toPublic(user)
  }

  // Chamado a cada pedido autenticado (ver requireLocalAuth) para
  // sabermos quem está "online agora" e quem usa o sistema com
  // frequência. Feito em fire-and-forget, nunca bloqueia o pedido.
  static async touchActivity(id: string): Promise<void> {
    try {
      const user = await this.findById(id)
      if (!user) return
      user.lastActiveAt = Date.now()
      await this.persist(user)
    } catch (err) {
      logger.warn('[UserService] Falha ao atualizar lastActiveAt', { id, err })
    }
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

    const startOfDay = new Date()
    startOfDay.setHours(0, 0, 0, 0)
    const startOfDayMs = startOfDay.getTime()

    const total = users.length
    const blocked = users.filter((u) => u.status === 'blocked').length
    const verified = users.filter((u) => u.emailVerified).length
    const unverified = total - verified
    const neverLoggedIn = users.filter((u) => u.lastLoginAt === null).length
    const newToday = users.filter((u) => u.createdAt >= startOfDayMs).length
    const onlineNow = users.filter((u) => u.lastActiveAt !== null && now - u.lastActiveAt <= ONLINE_THRESHOLD_MS).length
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
      newToday,
      onlineNow,
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

  // ─── Apagar utilizador (usado no fluxo de dev/testes) ────────
  static async deleteByEmail(email: string): Promise<boolean> {
    const user = await this.findByEmail(email)
    if (!user) return false

    if (isRedisConnected()) {
      try {
        const client = getRedisClient()
        await client.del(userKey(user.id))
        await client.del(emailKey(user.email))
        await client.sRem(INDEX_KEY, user.id)
      } catch (err) {
        logger.warn('[UserService] Falha ao apagar no Redis, a limpar apenas memória', { err })
      }
    }

    memUsers.delete(user.id)
    memEmailIndex.delete(user.email)
    memIndex.delete(user.id)

    logger.info('[UserService] Utilizador apagado', { id: user.id, email: user.email })
    return true
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
