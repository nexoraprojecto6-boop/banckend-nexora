// ============================================================
// NEXORA FOREX — Classificação de erros da Deriv
// ============================================================
// Decide o que fazer com cada erro (parar, esperar, repetir) e dá uma
// mensagem clara ao utilizador. A Deriv devolve mensagens em inglês e
// os erros variam por país/conta/moeda/mercado.

export type DerivErrorKind =
  | 'auth'
  | 'insufficient_balance'
  | 'stake_limits'
  | 'config'
  | 'market_closed'
  | 'symbol_unavailable'
  | 'rate_limit'
  | 'transient'
  | 'unknown';

export interface ClassifiedError {
  kind: DerivErrorKind;
  code: string;
  message: string;      // original
  userMessage: string;  // para mostrar
  fatal: boolean;       // repetir não adianta → parar o bot
  cooldownMs: number;   // esperar antes de voltar a tentar
}

const AUTH_CODES = new Set(['InvalidToken', 'AuthorizationRequired', 'Unauthorized', 'PermissionDenied']);
const CONFIG_CODES = new Set([
  'InputValidationFailed', 'ValidationError', 'InvalidContractProposal',
  'OfferingsValidationError', 'NotOffered', 'InvalidCurrency', 'InvalidDuration',
]);

export function classifyDerivError(err: unknown): ClassifiedError {
  const e = (err ?? {}) as { code?: unknown; message?: unknown };
  const code = typeof e.code === 'string' ? e.code : '';
  const message = typeof e.message === 'string' && e.message ? e.message : String(err);

  const mk = (kind: DerivErrorKind, userMessage: string, fatal: boolean, cooldownMs = 0): ClassifiedError =>
    ({ kind, code, message, userMessage, fatal, cooldownMs });

  if (code === 'InsufficientBalance' || /insufficient balance|not enough (funds|balance)/i.test(message)) {
    return mk('insufficient_balance', 'Saldo insuficiente na conta para abrir o contrato.', true);
  }
  if (AUTH_CODES.has(code) || /token.*(invalid|expired)/i.test(message)) {
    return mk('auth', 'A sessão Deriv expirou ou não tem permissão. Faça login novamente.', true);
  }
  if (/(minimum|maximum) stake|stake.*(too (low|high)|exceed)|decimal places|stake.*must/i.test(message)) {
    return mk('stake_limits', `Stake fora dos limites da Deriv para esta moeda/mercado: ${message}`, true);
  }
  if (code === 'InvalidSymbol' || /invalid symbol|symbol.*(not (available|offered|found)|unknown)|market.*not (available|offered)/i.test(message)) {
    return mk('symbol_unavailable', `O mercado não está disponível para esta conta: ${message}`, true);
  }
  if (code === 'MarketIsClosed' || /market (is )?(presently )?closed|trading is suspended|not open for trading/i.test(message)) {
    return mk('market_closed', 'Mercado fechado ou suspenso. O bot espera e tenta de novo.', false, 30_000);
  }
  if (code === 'RateLimit' || /rate ?limit|too many requests/i.test(message)) {
    return mk('rate_limit', 'Muitos pedidos à Deriv. O bot abranda e tenta de novo.', false, 3_000);
  }
  if (/not (offered|available)|restricted|not allowed|jurisdiction|your country|invalid (symbol|currency|duration|barrier)|currency (mismatch|not)/i.test(message)
      || CONFIG_CODES.has(code)) {
    return mk('config', `Este contrato/mercado/moeda não está disponível para a sua conta ou país: ${message}`, true);
  }
  if (/timeout|timed out|ligação|connection|socket|econn|closed before/i.test(message)) {
    return mk('transient', 'Falha temporária de ligação à Deriv.', false);
  }
  return mk('unknown', message, false);
}
