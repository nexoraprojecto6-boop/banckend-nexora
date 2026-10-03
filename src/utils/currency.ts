// ============================================================
// NEXORA FOREX — Moedas (qualquer conta Deriv, qualquer país)
// ============================================================
// Casas decimais aceites pela Deriv por moeda. Usar sempre o código
// da conta ATIVA (session.currency) — nunca "USD" fixo.

const DECIMALS: Record<string, number> = {
  // fiat
  USD: 2, EUR: 2, GBP: 2, AUD: 2, CAD: 2, CHF: 2, JPY: 2, NZD: 2,
  // stablecoins
  USDC: 2, UST: 2, EUSDT: 2, TUSDT: 2, USDT: 2, DAI: 2, TUSD: 2, PAX: 2, BUSD: 2,
  // cripto "clássicas"
  BTC: 8, ETH: 8, LTC: 8,
};

export function currencyDecimals(code?: string): number {
  return DECIMALS[(code ?? 'USD').toUpperCase()] ?? 2;
}

/** Arredonda ao nº de casas da moeda (evita 0.35000000000000003 e stakes que viram 0 em BTC). */
export function roundToCurrency(n: number, code?: string): number {
  return Number(n.toFixed(currencyDecimals(code)));
}

export function formatMoney(n: number | string, code?: string): string {
  const cur = (code ?? 'USD').toUpperCase();
  return `${Number(n).toFixed(currencyDecimals(cur))} ${cur}`;
}

// Proteção: um stake pensado em USD (ex: 1 ou 5) numa conta BTC/ETH/LTC
// seria 1 BTC! Limites de sanidade para o stake INICIAL.
const CRYPTO_STAKE_SANITY: Record<string, number> = { BTC: 0.01, ETH: 0.2, LTC: 5 };

export function stakeSanityError(currency: string, stake: number): string | null {
  if (!Number.isFinite(stake) || stake <= 0) return 'Stake inválido.';
  const cur = currency.toUpperCase();
  const max = CRYPTO_STAKE_SANITY[cur];
  if (max !== undefined && stake >= max && process.env.ALLOW_LARGE_CRYPTO_STAKE !== '1') {
    return (
      `O stake ${stake} ${cur} parece um valor em USD. Em contas ${cur} o stake é em ${cur} ` +
      `(ex: ${cur === 'BTC' ? '0.0001' : cur === 'ETH' ? '0.002' : '0.05'}). Ajuste o stake e tente de novo.`
    );
  }
  return null;
}
