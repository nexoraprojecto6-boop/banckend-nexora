import { WebSocketManager } from '@websocket/manager.js';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';
import { DerivAPIService } from './deriv-api.service.js';
import { ProposalRequest, BuyRequest, SellRequest, ContractUpdate } from '../types/schemas.js';

// ─── Correcções vs v1 ────────────────────────────────────────
//   ✅ getBalance: já não envia subscribe:1 (cada chamada criava uma subscrição
//      que nunca era cancelada → esgotava o limite de 100). Para stream use
//      subscribeBalance().
//   ✅ getProposal: já não envia `subscribe: 0` (o schema só aceita 1) e é pedido
//      ÚNICO. Para stream use subscribeProposal(). Aceita `barrier`.
//   ✅ getTicksHistory: a resposta da Deriv traz `history` (ticks) ou `candles`,
//      não `ticks_history` (devolvia sempre undefined).
//   ✅ Erros da Deriv chegam como DerivWsError (com .code) — ver websocket/manager.ts

export class TradingService {
  // público para permitir acesso direto em trading.routes.ts
  public wsManager: WebSocketManager;

  /**
   * Duas formas de instanciar:
   *
   *   new TradingService(otpUrl)
   *     → mantido por retrocompatibilidade. Usa uma URL fixa; se a
   *       ligação cair, o reconnect tenta reutilizar a MESMA url, o
   *       que falha sempre que a Deriv emitiu essa URL com um OTP de
   *       uso único (o sintoma é "autentica e cai logo a seguir",
   *       em ciclo, até esgotar as tentativas).
   *
   *   new TradingService(otpUrl, { token, accountId })
   *     → RECOMENDADO. Com token+accountId, cada reconexão pede um
   *       OTP novo à Deriv antes de tentar ligar — resolve o ciclo
   *       de desconexão na origem.
   */
  constructor(otpUrl: string, refresh?: { token: string; accountId: string }) {
    this.wsManager = new WebSocketManager({
      ...(refresh
        ? { getUrl: () => TradingService.fetchFreshUrl(refresh.token, refresh.accountId) }
        : { url: otpUrl }),
      autoConnect: true,
      heartbeatInterval: config.websocket.heartbeatInterval,
      reconnectMaxAttempts: config.websocket.reconnectMaxAttempts,
      reconnectDelay: config.websocket.reconnectDelay,
    });

    if (!refresh) {
      logger.warn(
        '[TradingService] Instanciado sem token/accountId — reconexões reutilizarão a mesma URL/OTP ' +
        'e podem falhar em ciclo. Prefira new TradingService(otpUrl, { token, accountId }).',
      );
    }
  }

  private static async fetchFreshUrl(token: string, accountId: string): Promise<string> {
    const derivAPI = new DerivAPIService(token);
    const otpData = await derivAPI.getOTP(accountId);
    const wsUrl: string = otpData?.url || otpData?.wsUrl;
    if (!wsUrl) {
      throw new Error('Failed to get WebSocket URL (OTP)');
    }
    return wsUrl;
  }

  async getActiveSymbols(detailed = false) {
    try {
      const response = await this.wsManager.send({
        active_symbols: detailed ? 'full' : 'brief',
      });
      return response.active_symbols;
    } catch (error) {
      logger.error('Failed to get active symbols', { error });
      throw error;
    }
  }

  async getContractsFor(symbol: string) {
    try {
      const response = await this.wsManager.send({ contracts_for: symbol });
      return response.contracts_for;
    } catch (error) {
      logger.error('Failed to get contracts for symbol', { error, symbol });
      throw error;
    }
  }

  async getContractsList() {
    try {
      const response = await this.wsManager.send({ contracts_list: 1 });
      return response.contracts_list;
    } catch (error) {
      logger.error('Failed to get contracts list', { error });
      throw error;
    }
  }

  subscribeTicks(symbols: string | string[], onTick: (tick: any) => void): string {
    const symbolArray = Array.isArray(symbols) ? symbols : [symbols];
    return this.wsManager.subscribe('ticks', {
      ticks: symbolArray.length === 1 ? symbolArray[0] : symbolArray,
    }, onTick);
  }

  /**
   * Histórico de ticks ou velas.
   * Devolve { history, candles, pip_size }:
   *   style 'ticks'   → history: { prices[], times[] }
   *   style 'candles' → candles: [{ open, high, low, close, epoch }]
   */
  async getTicksHistory(symbol: string, options: {
    count?: number;
    start?: number;
    end?: string;
    style?: 'ticks' | 'candles';
    granularity?: number;
  } = {}) {
    try {
      const response = await this.wsManager.send({
        ticks_history: symbol,
        end: options.end || 'latest',
        count: options.count || 100,
        style: options.style || 'ticks',
        adjust_start_time: 1,
        ...(options.start ? { start: options.start } : {}),
        ...(options.granularity && { granularity: options.granularity }),
      });
      return {
        history: response.history,
        candles: response.candles,
        pip_size: response.pip_size,
      };
    } catch (error) {
      logger.error('Failed to get ticks history', { error, symbol });
      throw error;
    }
  }

  // Pedido ÚNICO de saldo. Para acompanhar o saldo use subscribeBalance().
  async getBalance() {
    try {
      const response = await this.wsManager.send({ balance: 1 });
      return response.balance;
    } catch (error) {
      logger.error('Failed to get balance', { error });
      throw error;
    }
  }

  subscribeBalance(onBalance: (balance: any) => void): string {
    return this.wsManager.subscribe('balance', { balance: 1 }, onBalance);
  }

  async getPortfolio() {
    try {
      const response = await this.wsManager.send({ portfolio: 1 });
      return response.portfolio;
    } catch (error) {
      logger.error('Failed to get portfolio', { error });
      throw error;
    }
  }

  async getProfitTable(options: { limit?: number; offset?: number; description?: boolean } = {}) {
    try {
      const response = await this.wsManager.send({
        profit_table: 1,
        limit: options.limit || 25,
        offset: options.offset || 0,
        description: options.description ? 1 : 0,
      });
      return response.profit_table;
    } catch (error) {
      logger.error('Failed to get profit table', { error });
      throw error;
    }
  }

  async getStatement(options: { limit?: number; offset?: number; description?: boolean } = {}) {
    try {
      const response = await this.wsManager.send({
        statement: 1,
        limit: options.limit || 100,
        offset: options.offset || 0,
        description: options.description ? 1 : 0,
      });
      return response.statement;
    } catch (error) {
      logger.error('Failed to get statement', { error });
      throw error;
    }
  }

  subscribeTransactions(onTransaction: (transaction: any) => void): string {
    return this.wsManager.subscribe('transaction', { transaction: 1 }, onTransaction);
  }

  // Pedido ÚNICO de proposta. Para acompanhar o preço use subscribeProposal().
  async getProposal(request: ProposalRequest) {
    try {
      const r = request as ProposalRequest & { barrier?: string };
      const response = await this.wsManager.send({
        proposal: 1,
        amount: r.amount,
        basis: r.basis,
        contract_type: r.contractType,
        currency: r.currency,
        duration: r.duration,
        duration_unit: r.durationUnit,
        underlying_symbol: r.underlyingSymbol,
        ...(r.barrier !== undefined ? { barrier: r.barrier } : {}),
      });

      return {
        id: response.proposal.id,
        askPrice: response.proposal.ask_price,
        payout: response.proposal.payout,
        spot: response.proposal.spot,
        spotTime: response.proposal.spot_time,
        dateStart: response.proposal.date_start,
        dateExpiry: response.proposal.date_expiry,
        longcode: response.proposal.longcode,
      };
    } catch (error) {
      logger.error('Failed to get proposal', { error });
      throw error;
    }
  }

  subscribeProposal(request: ProposalRequest, onProposal: (proposal: any) => void): string {
    const r = request as ProposalRequest & { barrier?: string };
    return this.wsManager.subscribe('proposal', {
      proposal: 1,
      amount: r.amount,
      basis: r.basis,
      contract_type: r.contractType,
      currency: r.currency,
      duration: r.duration,
      duration_unit: r.durationUnit,
      underlying_symbol: r.underlyingSymbol,
      ...(r.barrier !== undefined ? { barrier: r.barrier } : {}),
      subscribe: 1,
    }, onProposal);
  }

  async buy(request: BuyRequest) {
    try {
      const response = await this.wsManager.send({
        buy: request.proposalId,
        price: request.maxPrice,
      });

      return {
        contractId: response.buy.contract_id,
        buyPrice: response.buy.buy_price,
        payout: response.buy.payout,
        purchaseTime: response.buy.purchase_time,
        balanceAfter: response.buy.balance_after,
        transactionId: response.buy.transaction_id,
        longcode: response.buy.longcode,
      };
    } catch (error) {
      logger.error('Failed to buy contract', { error });
      throw error;
    }
  }

  async sell(request: SellRequest) {
    try {
      const response = await this.wsManager.send({
        sell: request.contractId,
        price: request.minPrice,
      });

      return {
        contractId: response.sell.contract_id,
        soldFor: response.sell.sold_for,
        balanceAfter: response.sell.balance_after,
        transactionId: response.sell.transaction_id,
      };
    } catch (error) {
      logger.error('Failed to sell contract', { error });
      throw error;
    }
  }

  async getOpenContract(contractId: number) {
    try {
      const response = await this.wsManager.send({
        proposal_open_contract: 1,
        contract_id: contractId,
      });
      return response.proposal_open_contract;
    } catch (error) {
      logger.error('Failed to get open contract', { error, contractId });
      throw error;
    }
  }

  subscribeOpenContract(contractId: number, onUpdate: (contract: any) => void): string {
    return this.wsManager.subscribe(`contract_${contractId}`, {
      proposal_open_contract: 1,
      contract_id: contractId,
    }, onUpdate);
  }

  async updateContract(request: ContractUpdate) {
    try {
      const response = await this.wsManager.send({
        contract_update: 1,
        contract_id: request.contractId,
        limit_order: {
          stop_loss: request.limitOrder?.stopLoss,
          take_profit: request.limitOrder?.takeProfit,
        },
      });
      return response.contract_update;
    } catch (error) {
      logger.error('Failed to update contract', { error, contractId: request.contractId });
      throw error;
    }
  }

  async cancelContract(contractId: number) {
    try {
      const response = await this.wsManager.send({ cancel: contractId });
      return response.cancel;
    } catch (error) {
      logger.error('Failed to cancel contract', { error, contractId });
      throw error;
    }
  }

  getStatus() {
    return this.wsManager.getStatus();
  }

  disconnect() {
    this.wsManager.disconnect();
  }

  cleanupExpired() {
    this.wsManager.cleanupExpired();
  }
}
