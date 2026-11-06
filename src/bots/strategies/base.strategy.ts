// Apenas tipos mantidos por compatibilidade com server.ts.
// (As estratégias antigas foram removidas: o ciclo corre na Deriv via auto_*.)
import { BotConfig, BotStrategyType } from '../bot.types.js';

export interface SymbolResolution {
  symbol: string | null;
  changed: boolean;
  reason?: string;
}
export type SymbolResolver = (
  strategy: BotStrategyType,
  config: BotConfig,
  current: string,
) => Promise<SymbolResolution>;
