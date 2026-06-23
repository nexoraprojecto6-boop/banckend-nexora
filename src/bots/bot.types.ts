// ============================================================
// NEXORA FOREX — Bot Types
// ============================================================

export type BotStatus = 'idle' | 'running' | 'paused' | 'stopped' | 'error';
export type BotStrategyType = 'scalping' | 'martingale' | 'anti_martingale' | 'trend_following';
export type TradeDirection = 'CALL' | 'PUT';

// ─── Configuração base (comum a todas as estratégias) ────────

export interface BotConfig {
  symbol: string;              // ex: "R_100", "1HZ100V"
  contractType: string;        // ex: "CALL", "PUT", "DIGITOVER"
  duration: number;
  durationUnit: 'ticks' | 's' | 'm' | 'h' | 'd';
  initialStake: number;
  currency: string;            // "USD"
  maxTrades?: number;          // 0 = ilimitado
  maxLoss?: number;            // para se perda total > valor
  maxProfit?: number;          // para se lucro total > valor
  strategyParams?: Record<string, unknown>;
}

// ─── Parâmetros específicos por estratégia ───────────────────

export interface ScalpingParams {
  maxConsecutiveLosses: number; // pausa o bot após N perdas seguidas
}

export interface MartingaleParams {
  multiplier: number;          // multiplicador do stake (ex: 2.0)
  maxStake: number;            // teto do stake para evitar explosão
  resetOnWin: boolean;         // true = anti-martingale
}

export interface TrendFollowingParams {
  ticksWindow: number;         // quantos ticks analisar
  trendThreshold: number;      // variação % mínima para confirmar tendência (ex: 0.002)
}

// ─── Estatísticas do bot ─────────────────────────────────────

export interface BotStats {
  totalTrades: number;
  wins: number;
  losses: number;
  totalProfit: number;
  totalLoss: number;
  netPnL: number;
  winRate: number;             // 0–100
  currentStreak: number;       // positivo = wins, negativo = losses
  currentStake: number;
  peakProfit: number;
  maxDrawdown: number;
}

// ─── Estado completo do bot ──────────────────────────────────

export interface BotState {
  id: string;
  name: string;
  strategy: BotStrategyType;
  status: BotStatus;
  config: BotConfig;
  stats: BotStats;
  lastError?: string;
  startedAt?: Date;
  stoppedAt?: Date;
  logs: BotLogEntry[];
}

// ─── Logs ────────────────────────────────────────────────────

export type LogLevel = 'info' | 'warn' | 'error' | 'trade';

export interface BotLogEntry {
  timestamp: Date;
  level: LogLevel;
  message: string;
  data?: Record<string, unknown>;
}

// ─── Resultado de trade ──────────────────────────────────────

export interface TradeResult {
  contractId: string;
  profit: number;
  stake: number;
  won: boolean;
  entryTick?: number;
  exitTick?: number;
  entryTime: Date;
  exitTime?: Date;
}

// ─── Eventos emitidos pelo bot ───────────────────────────────
// Estes eventos são repassados via WebSocket ao frontend
//
// bot:goal_reached — emitido ANTES de bot:stopped quando o bot pára
//   automaticamente por ter atingido uma condição de paragem (take
//   profit, stop de perda, ou limite de trades). payload.reason
//   identifica qual: 'max_profit_reached' | 'max_loss_reached' |
//   'max_trades_reached'. O frontend usa isto para mostrar a
//   mensagem certa ("Meta atingida!", "Stop de perda atingido", etc).
//
// bot:insufficient_balance — emitido quando uma compra falha por
//   saldo insuficiente na conta Deriv (erro InsufficientBalance da
//   API). O bot é parado automaticamente a seguir.

export type BotEventType =
  | 'bot:started'
  | 'bot:stopped'
  | 'bot:paused'
  | 'bot:resumed'
  | 'bot:error'
  | 'bot:trade_opened'
  | 'bot:trade_closed'
  | 'bot:stats_updated'
  | 'bot:log'
  | 'bot:goal_reached'
  | 'bot:insufficient_balance';

export interface BotEvent {
  type: BotEventType;
  botId: string;
  payload: Record<string, unknown>;
}

// ─── Shapes de request/response da API REST ──────────────────

export interface CreateBotRequest {
  name: string;
  strategy: BotStrategyType;
  config: BotConfig;
}

export interface BotSummary {
  id: string;
  name: string;
  strategy: BotStrategyType;
  status: BotStatus;
  stats: BotStats;
  startedAt?: Date;
  stoppedAt?: Date;
}

// ─── Interface do adaptador Deriv injectado nos bots ─────────
// Recebe o derivWs da sessão do cliente (já autenticado)

export interface DerivWsAdapter {
  buyContract(params: {
    symbol: string;
    contractType: string;
    stake: number;
    duration: number;
    durationUnit: string;
    currency: string;
  }): Promise<{ contractId: string }>;

  waitForContract(contractId: string): Promise<{
    profit: number;
    won: boolean;
    entryTick: number;
    exitTick: number;
  }>;

  subscribeToTicks(
    symbol: string,
    onTick: (price: number) => void,
  ): { unsubscribe: () => void };
}
