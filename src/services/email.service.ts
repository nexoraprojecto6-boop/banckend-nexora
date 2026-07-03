// ============================================================
// NEXORA — Email Service (verificação de conta)
//
// Se SMTP_HOST não estiver configurado, faz fallback e apenas
// regista o link de verificação nos logs — não bloqueia o registo
// em desenvolvimento sem SMTP configurado.
// ============================================================

import nodemailer, { Transporter } from 'nodemailer'
import { config } from '@config/index.js'
import logger from '@utils/logger.js'

let transporter: Transporter | null = null

function getTransporter(): Transporter | null {
  if (!config.email.smtpHost) return null
  if (transporter) return transporter

  transporter = nodemailer.createTransport({
    host: config.email.smtpHost,
    port: config.email.smtpPort,
    secure: config.email.smtpSecure,
    auth: config.email.smtpUser
      ? { user: config.email.smtpUser, pass: config.email.smtpPass }
      : undefined,
  })
  return transporter
}

export class EmailService {

  static async sendVerificationEmail(to: string, name: string, token: string): Promise<void> {
    const verifyUrl = `${config.frontend.url}/verify-email?token=${token}`
    const t = getTransporter()

    if (!t) {
      logger.warn('[Email] SMTP não configurado — link de verificação apenas nos logs', {
        to, verifyUrl,
      })
      return
    }

    try {
      await t.sendMail({
        from: config.email.from,
        to,
        subject: 'Confirma o teu email — NEXORA',
        html: `
          <div style="font-family: sans-serif; max-width: 480px; margin: 0 auto;">
            <h2>Olá, ${name}!</h2>
            <p>Obrigado por te registares na NEXORA. Confirma o teu email para ativar a tua conta:</p>
            <p style="margin: 24px 0;">
              <a href="${verifyUrl}" style="background:#0d6efd;color:#fff;padding:12px 24px;border-radius:6px;text-decoration:none;">
                Confirmar email
              </a>
            </p>
            <p>Ou copia este link: <br/>${verifyUrl}</p>
            <p style="color:#888;font-size:12px;">Este link expira em 24 horas.</p>
          </div>
        `,
      })
      logger.info('[Email] Email de verificação enviado', { to })
    } catch (err) {
      logger.error('[Email] Falha ao enviar email de verificação', { to, err })
      // Não lança erro — o registo não deve falhar por causa do SMTP.
      // O utilizador pode pedir reenvio via /resend-verification.
    }
  }
}
