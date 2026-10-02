// ============================================================
// NEXORA — User Service (email + senha, gravado no Redis)
//
// Independente do fluxo OAuth da Deriv (auth.service.ts).
//
// ENTRAR = LOGIN OU CRIAÇÃO
//   enter(email, senha):
//     - email já existe  -> confere a senha e entra
//     - email novo       -> cria a conta, grava no Redis e entra
//   Não há formulário de registo nem verificação de email.
//
// Chaves Redis:
//   user:{id}            -> JSON do utilizador (senha só como hash bcrypt)
//   user:email:{email}   -> id  (lookup rápido por email)
//   users:index          -> SET com todos os ids
//
// Admin: o role 'admin' vem de config.admin.emails (ADMIN_EMAILS) e
// é recalculado a cada entrada.
// ============================================================

import bcrypt from 'bcryptjs'
import { v4 as uuidv4 } from 'uuid'
import { getRedisClient, isRedisConnected } from '@utils/redis.js'
import logger from '@utils/logger.js'
import { config } from '@config/index.js'
import { AuthenticationError, ConflictError } from '../types/errors.js'

// bcryptjs é JavaScript puro e bloqueia o event loop durante o hash.
// 10 rondas é o equilíbrio recomendado (12 ficava pesado em instâncias
// pequenas e atrasava o WebSocket e os bots).
const SALT_ROUNDS = 10
const MIN_PASSWORD_LENGTH = 6
const INACTIVE_AFTER_DAYS = 30
const ONLINE_THRESHOLD_MS = 5 * 60 * 1000       // 5 min sem atividade = offline
const ACTIVITY_WRITE_INTERVAL_MS = 60 * 1000    // grava lastActiveAt no máximo 1x/min por utilizador

// Senha curta demais ao criar uma conta nova (a rota devolve 400).
export class WeakPasswordError extends Error {
  constructor() {
    super(`A senha precisa de pelo menos ${MIN_PASSWORD_LENGTH} caracteres.`)
    this.name = 'WeakPasswordError'
  }
}

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

const userKey = (id: string) => `user:${id}`
const emailKey = (email: string) => `user:email:${email.toLowerCase().trim()}`
const INDEX_KEY = 'users:index'

function isAdminEmail(email: string): boolean {
  return config.admin.emails.includes(email.toLowerCase().trim())
}

// ─── Fallback em memória (quando o Redis não está disponível) ──
const memUsers = new Map<string, LocalUser>()
const memEmailIndex = new Map<string, string>()
const memIndex = new Set<string>()

// Controlo de frequência das gravações de atividade
const lastActivityWrite = new Map<string, number>()

export class UserService {

  // ─── Entrar (ou criar conta) ────────────────────────────────

  static async enter(
    email: string,
    password: string,
  ): Promise<{ user: PublicUser; created: boolean }> {
    const normalizedEmail = email.toLowerCase().trim()
    const existing = await this.findByEmail(normalizedEmail)

    // ── Email já existe: login normal ──
    if (existing) {
      const valid = await bcrypt.compare(password, existing.passwordHash)
      if (!valid) throw new AuthenticationError('Email ou senha incorretos')
      // Só depois de a senha estar certa é que se diz que está bloqueada.
      if (existing.status === 'blocked') {
        throw new AuthenticationError('Conta bloqueada. Contacta o suporte.')
      }

      existing.role = isAdminEmail(existing.email) ? 'admin' : 'user'
      existing.lastLoginAt = Date.now()
      existing.lastActiveAt = Date.now()
      await this.persist(existing)

      logger.info('[UserService] Login efetuado', { id: existing.id, role: existing.role })
      return { user: toPublic(existing), created: false }
    }

    // ── Email novo: cria a conta ──
    if (password.length < MIN_PASSWORD_LENGTH) throw new WeakPasswordError()

    const now = Date.now()
    const user: LocalUser = {
      id: uuidv4(),
      fullName: normalizedEmail.split('@')[0],
      whatsapp: '',
      email: normalizedEmail,
      passwordHash: await bcrypt.hash(password, SALT_ROUNDS),
      role: isAdminEmail(normalizedEmail) ? 'admin' : 'user',
      status: 'active',
      emailVerified: true,
      createdAt: now,
      lastLoginAt: now,
      lastActiveAt: now,
    }
    await this.persist(user)

    logger.info('[UserService] Conta criada automaticamente', { id: user.id, role: user.role })
    return { user: toPublic(user), created: true }
  }

  // Registo com nome e WhatsApp (já não é usado pelo ecrã de entrada;
  // mantido para a rota /register e scripts existentes).
  static async register(input: {
    fullName: string
    whatsapp: string
    email: string
    password: string
  }): Promise<PublicUser> {
    const normalizedEmail = input.email.toLowerCase().trim()

    if (await this.findByEmail(normalizedEmail)) {
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
      emailVerified: true,
      createdAt: Date.now(),
      lastLoginAt: null,
      lastActiveAt: null,
    }

    await this.persist(user)
    logger.info('[UserService] Novo utilizador registado', { id: user.id, role: user.role })
    return toPublic(user)
  }

  // ─── Atividade ──────────────────────────────────────────────

  // Chamado a cada pedido autenticado. Grava no máximo 1x por minuto
  // por utilizador: evita ler+escrever em todos os pedidos e reduz a
  // janela em que uma escrita de atividade poderia sobrepor outra
  // alteração (ex.: um bloqueio feito no painel admin).
  static async touchActivity(id: string): Promise<void> {
    const now = Date.now()
    if (now - (lastActivityWrite.get(id) ?? 0) < ACTIVITY_WRITE_INTERVAL_MS) return
    lastActivityWrite.set(id, now)

    try {
      const user = await this.findById(id)
      if (!user) return
      user.lastActiveAt = now
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
    if (!normalizedEmail) return null

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
    return id ? (memUsers.get(id) ?? null) : null
  }

  static async listAll(): Promise<LocalUser[]> {
    if (isRedisConnected()) {
      try {
        const client = getRedisClient()
        const ids = await client.sMembers(INDEX_KEY)
        if (ids.length === 0) return []
        const raws = await client.mGet(ids.map(userKey))
        return raws.filter((r): r is string => !!r).map((r) => JSON.parse(r) as LocalUser)
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
    const neverLoggedIn = users.filter((u) => u.lastLoginAt === null).length
    const newToday = users.filter((u) => u.createdAt >= startOfDayMs).length
    const onlineNow = users.filter((u) => u.lastActiveAt !== null && now - u.lastActiveAt <= ONLINE_THRESHOLD_MS).length
    const inactive = users.filter((u) => {
      if (u.status === 'blocked') return false
      return now - (u.lastLoginAt ?? u.createdAt) > inactiveThreshold
    }).length

    return {
      total,
      active: total - inactive - blocked,
      inactive,
      blocked,
      verified,
      unverified: total - verified,
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

  // ─── Apagar utilizador (dev/testes) ─────────────────────────
  static async deleteByEmail(email: string): Promise<boolean> {
    const user = await this.findByEmail(email)
    if (!user) return false

    if (isRedisConnected()) {
      try {
        await getRedisClient()
          .multi()
          .del(userKey(user.id))
          .del(emailKey(user.email))
          .sRem(INDEX_KEY, user.id)
          .exec()
      } catch (err) {
        logger.warn('[UserService] Falha ao apagar no Redis, a limpar apenas memória', { err })
      }
    }

    memUsers.delete(user.id)
    memEmailIndex.delete(user.email)
    memIndex.delete(user.id)
    lastActivityWrite.delete(user.id)

    logger.info('[UserService] Utilizador apagado', { id: user.id })
    return true
  }

  // ─── Persistência interna ───────────────────────────────────
  // Uma única transação: utilizador, índice por email e índice geral
  // são gravados juntos (ou nenhum).
  private static async persist(user: LocalUser): Promise<void> {
    if (isRedisConnected()) {
      try {
        await getRedisClient()
          .multi()
          .set(userKey(user.id), JSON.stringify(user))
          .set(emailKey(user.email), user.id)
          .sAdd(INDEX_KEY, user.id)
          .exec()
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
