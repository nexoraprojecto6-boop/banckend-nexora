// ============================================================
// NEXORA FOREX — Bot Types
// ============================================================

export type BotStatus = 'idle' | 'running' | 'paused' | 'stopped' | 'error';
export type BotStrategyType = 'scalping' | 'martingale' | 'anti_martingale' | 'trend_following' | 'scalper' | 'alfha' | 'over1_recovery';
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

// Parâmetros do bot SCALPER — apenas o necessário para configurar.
// STOP LOSS / TAKE PROFIT ficam em config.maxLoss / config.maxProfit.
export interface ScalperParams {
  predictDigit: number;        // barreira do DIGITOVER (ex: 2 = "over 2", gatilho cobre dígitos 0,1,2)
  gale: number;                // multiplicador do stake por perda
  lossesToStartGale: number;   // nº de perdas seguidas antes do Gale começar
  maxMartingaleSteps: number;  // nº de níveis de Gale antes de resetar o stake
  pauseLossLimit: number;      // perda acumulada que pausa o bot temporariamente (0 = desactivado)
  pauseSeconds: number;        // duração da pausa automática, em segundos
  decimalPlaces?: number;      // nº de casas decimais do símbolo, usado para extrair
                                //   o último dígito corretamente (padrão: 2 se omitido)
}

// Parâmetros do bot ALFHA — opera Over e Under conforme o último dígito.
// STOP LOSS / TAKE PROFIT ficam em config.maxLoss / config.maxProfit.
export interface AlfhaParams {
  digitOver: number;            // barreira para entrar em DIGITOVER
  digitUnder: number;           // barreira para entrar em DIGITUNDER
  gale: number;                 // multiplicador do stake por perda
  lossesToStartGale: number;    // nº de perdas seguidas antes do Gale começar
  maxMartingaleSteps: number;   // nº de níveis de Gale antes de resetar o stake
  pauseLossLimit: number;       // perda acumulada que pausa o bot temporariamente (0 = desactivado)
  pauseSeconds: number;         // duração da pausa automática, em segundos
}

// Parâmetros do bot OVER1_RECOVERY (nome de exibição: "ETHEREUM") —
// Over 1 fixo, com gale de recuperação (recupera a perda acumulada
// do ciclo + uma margem extra, calculado sobre o payout real da Deriv).
// STOP LOSS / TAKE PROFIT ficam em config.maxLoss / config.maxProfit.
export interface OverOneRecoveryParams {
  digitBarrier: number;           // barreira do DIGITOVER (fixo em 1 para "Over 1")
  recoveryMarginPercent: number;  // margem extra sobre a perda acumulada (ex: 10 = +10%)
  protectiveStakePercent: number; // % do stake inicial usado a partir da 2ª perda SEGUIDA
                                    //   (protege a banca em vez de continuar escalando)
  maxMartingaleSteps: number;     // teto de perdas seguidas (mesmo em modo protetor) antes de cortar e resetar
  maxStakeCap: number;            // teto absoluto de stake — NUNCA ultrapassado,
                                    //   mesmo que a recuperação fique incompleta
  minStake?: number;              // piso de stake (evita stake residual ~0 por arredondamento)
  pauseLossLimit: number;         // perda acumulada que pausa o bot temporariamente (0 = desativado)
  pauseSeconds: number;           // duração da pausa automática, em segundos
  decimalPlaces?: number;         // casas decimais do símbolo (padrão 2)
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
  exitDigit?: number;   // último algarismo do exitTick, já calculado pela
                          // estratégia (evita recalcular no frontend com
                          // casas decimais desconhecidas do símbolo)
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
    barrier?: string;       // necessário para contratos de dígitos (DIGITOVER/DIGITUNDER)
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

  // Pedido único de proposta (payout) sem comprar — usado pelas
  // estratégias para calcular o stake certo ANTES de decidir quanto
  // apostar (ex: gale de recuperação do Over1 Recovery / ETHEREUM).
  getProposal(params: {
    symbol: string;
    contractType: string;
    duration: number;
    durationUnit: string;
    currency: string;
    barrier?: string;
    stake: number;
  }): Promise<{ payout: number }>;
}
