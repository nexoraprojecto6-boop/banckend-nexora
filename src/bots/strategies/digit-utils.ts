// ============================================================
// NEXORA FOREX — utilitários partilhados pelas estratégias
// ============================================================

/**
 * pip_size do tick → nº de casas decimais.
 * A Deriv envia o nº de casas (ex: 2); se vier como passo (0.01), converte.
 */
export function decimalsFromPip(pip: number | undefined, fallback = 2): number {
  if (pip === undefined || !Number.isFinite(pip) || pip <= 0) return fallback;
  return pip >= 1 ? Math.round(pip) : Math.round(-Math.log10(pip));
}

/** Último dígito com as casas decimais REAIS do símbolo (mantém zeros finais). */
export function lastDigit(price: number, decimals: number): number {
  const s = price.toFixed(decimals);
  return s.charCodeAt(s.length - 1) - 48;
}

/** Prefere o dígito do adapter (vem de exit_spot, string original da Deriv). */
export function exitDigitOf(
  result: { exitDigit?: number; exitTick: number },
  decimals: number,
): number | undefined {
  if (typeof result.exitDigit === 'number') return result.exitDigit;
  return result.exitTick ? lastDigit(result.exitTick, decimals) : undefined;
}

/** Defaults + overrides (o `??` antigo ignorava os defaults se houvesse params parciais). */
export function withDefaults<T extends object>(defaults: T, overrides?: Record<string, unknown>): T {
  const clean: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(overrides ?? {})) {
    if (v !== undefined && v !== null && !(typeof v === 'number' && Number.isNaN(v))) clean[k] = v;
  }
  return { ...defaults, ...clean };
}

export const roundMoney = (n: number): number => Math.round(n * 100) / 100;
