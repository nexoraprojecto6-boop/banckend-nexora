// ============================================================
// NEXORA — Email Service (verificação de conta)
//
// Usa o SDK oficial da Resend (API HTTPS) em vez de SMTP.
// Motivo: o Render bloqueia ligações SMTP de saída no plano
// gratuito — nodemailer + SMTP dá sempre ETIMEDOUT nesse
// ambiente. A Resend usa HTTPS normal (porta 443), que nunca é
// bloqueada.
//
// Se RESEND_API_KEY não estiver configurada, faz fallback e
// apenas regista o link de verificação nos logs — não bloqueia
// o registo em desenvolvimento sem a API key configurada.
//
// NOTA: enquanto o "from" for onboarding@resend.dev (sandbox),
// só consegues enviar para o teu próprio email cadastrado na
// Resend. Para enviar aos utilizadores reais, verifica um
// domínio próprio em Resend → Settings → Domains e atualiza
// EMAIL_FROM para algo como "NEXORA <no-reply@teudominio.com>".
// ============================================================
import { Resend } from 'resend'
import { config } from '@config/index.js'
import logger from '@utils/logger.js'

let resendClient: Resend | null = null

function getResendClient(): Resend | null {
  if (!config.email.resendApiKey) return null
  if (!resendClient) resendClient = new Resend(config.email.resendApiKey)
  return resendClient
}

export class EmailService {
  static async sendVerificationEmail(to: string, name: string, token: string): Promise<void> {
    const verifyUrl = `${config.frontend.url}/verify-email?token=${token}`
    const client = getResendClient()

    if (!client) {
      logger.warn('[Email] RESEND_API_KEY não configurada — link de verificação apenas nos logs', {
        to, verifyUrl,
      })
      return
    }

    const html = `
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
    `

    try {
      const { data, error } = await client.emails.send({
        from: config.email.from,
        to,
        subject: 'Confirma o teu email — NEXORA',
        html,
      })

      if (error) {
        throw new Error(typeof error === 'string' ? error : JSON.stringify(error))
      }

      logger.info('[Email] Email de verificação enviado via Resend', { to, id: data?.id })
    } catch (err) {
      logger.error('[Email] Falha ao enviar email de verificação', { to, err })
      // Não lança erro — o registo não deve falhar por causa do provedor de email.
      // O utilizador pode pedir reenvio via /resend-verification.
    }
  }
}
