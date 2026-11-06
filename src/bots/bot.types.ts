// ============================================================
// NEXORA FOREX — Bot Types v3 (modelo auto_* da Deriv)
// ============================================================
// A Deriv corre o ciclo de compras nos servidores dela (auto_start).
// Estratégias disponíveis: 'martingale' e 'dalembert'.
// ============================================================

export type BotStatus = 'idle' | 'running' | 'paused' | 'stopped' | 'error';
export type BotStrategyType = 'martingale' | 'dalembert';

// Contratos aceites pelas duas estratégias (auto_list_strategies)
export const AUTO_CONTRACT_TYPES = [
  'ACCU', 'CALL', 'CALLE', 'PUT', 'PUTE', 'ASIAND', 'ASIANU',
  'DIGITEVEN', 'DIGITODD', 'DIGITMATCH', 'DIGITDIFF', 'DIGITOVER', 'DIGITUNDER',
] as const;

// Contratos de dígitos que exigem barrier
export const BARRIER_CONTRACTS = ['DIGITMATCH', 'DIGITDIFF', 'DIGITOVER', 'DIGITUNDER'] as const;

// ─── Configuração ────────────────────────────────────────────
// A Deriv EXIGE take_profit e stop_loss (> 0):
//   maxProfit → take_profit | maxLoss → stop_loss | maxTrades → max_contracts
export interface BotConfig {
  symbol: string;               // ex: "R_100", "1HZ100V"
  contractType: string;         // um de AUTO_CONTRACT_TYPES
  duration: number;
  durationUnit: 'ticks' | 's' | 'm' | 'h' | 'd';
  initialStake: number;
  currency: string;
  maxTrades?: number;           // 0 = ilimitado
  maxLoss: number;              // obrigatório (> 0)
  maxProfit: number;            // obrigatório (> 0)
  strategyParams?: AutoStrategyParams;
}

export interface AutoStrategyParams {
  barrier?: string | number;    // obrigatório em DIGITOVER/UNDER/MATCH/DIFF
  multiplier?: number;          // martingale: (1, 10], padrão 2
  maxStake?: number;            // teto do stake (>= initialStake). RECOMENDADO
  unit?: number;                // dalembert: obrigatório (> 0)
}

// ─── Estatísticas ────────────────────────────────────────────

export interface BotStats {
  totalTrades: number;
  wins: number;
  losses: number;
  totalProfit: number;
  totalLoss: number;
  netPnL: number;
  winRate: number;
  currentStreak: number;
  currentStake: number;
  peakProfit: number;
  maxDrawdown: number;
}

export interface BotState {
  id: string;
  name: string;
  strategy: BotStrategyType;
  status: BotStatus;
  config: BotConfig;
  stats: BotStats;
  runId?: string;               // run_id da Deriv
  lastError?: string;
  startedAt?: Date;
  stoppedAt?: Date;
  logs: BotLogEntry[];
}

export type LogLevel = 'info' | 'warn' | 'error' | 'trade';

export interface BotLogEntry {
  timestamp: Date;
  level: LogLevel;
  message: string;
  data?: Record<string, unknown>;
}

export interface TradeResult {
  contractId: string;
  profit: number;
  stake: number;
  won: boolean;
  entryTick?: number;
  exitTick?: number;
  exitDigit?: number;
  entryTime: Date;
  exitTime?: Date;
}

// ─── Eventos (iguais aos anteriores → frontend não muda) ─────

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

// ─── Adaptador Deriv (auto_*) ────────────────────────────────

export interface AutoRunInfo {
  runId: string;
  status: 'running' | 'paused' | 'stopped';
  stopReason?: 'error' | 'user_stopped' | 'condition_triggered';
  stopReasonCode?: string;
  totalStake: number;
  totalPayout: number;
  contractIds: string[];
}

export interface ContractOutcome {
  profit: number;
  won: boolean;
  buyPrice: number;
  entryTick: number;
  exitTick: number;
  entrySpot?: string;
  exitSpot?: string;
  exitDigit?: number;
}

export interface AutoStartParams {
  tag: string;                                   // identifica o stream (usar o id do bot)
  strategyId: string;
  contractTemplate: Record<string, unknown>;
  strategyParameters: Record<string, unknown>;
}

export interface AutoHandlers {
  onUpdate: (run: AutoRunInfo) => void;
  onError: (err: Error) => void;
  onDisconnect: () => void;
}

export interface ManualBuyParams {
  symbol: string;
  contractType: string;
  stake: number;
  duration: number;
  durationUnit: string;
  currency: string;
  barrier?: string;
}

export interface DerivWsAdapter {
  /** Compra única (Operar Manual): buy directo, 1 viagem de rede. */
  buyContract(params: ManualBuyParams): Promise<{
    contractId: string;
    payout?: number;
    buyPrice?: number;
    balanceAfter?: number;
  }>;
  /** auto_start com subscribe:1. Resolve com o estado inicial do run. */
  autoStart(params: AutoStartParams, handlers: AutoHandlers): Promise<{ run: AutoRunInfo; detach: () => void }>;
  /** Volta a subscrever um run existente (auto_get + subscribe) após reconexão. */
  autoAttach(tag: string, runId: string, handlers: AutoHandlers): { detach: () => void };
  /** auto_pause | auto_resume | auto_stop */
  autoControl(action: 'pause' | 'resume' | 'stop', runId: string): Promise<AutoRunInfo>;
  /** Resultado real de um contrato (proposal_open_contract). */
  watchContract(contractId: string, maxWaitMs?: number): Promise<ContractOutcome>;
}
