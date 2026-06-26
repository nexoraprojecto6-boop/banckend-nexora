import { WebSocketManager } from '@websocket/manager.js';
import { config } from '@config/index.js';
import logger from '@utils/logger.js';
import { DerivAPIService } from './deriv-api.service.js';
import { ProposalRequest, BuyRequest, SellRequest, ContractUpdate } from '../types/schemas.js';

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
        start: options.start || 1,
        count: options.count || 100,
        style: options.style || 'ticks',
        ...(options.granularity && { granularity: options.granularity }),
      });
      return response.ticks_history;
    } catch (error) {
      logger.error('Failed to get ticks history', { error, symbol });
      throw error;
    }
  }

  async getBalance() {
    try {
      const response = await this.wsManager.send({ balance: 1, subscribe: 1 });
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

  async getProposal(request: ProposalRequest) {
    try {
      const response = await this.wsManager.send({
        proposal: 1,
        amount: request.amount,
        basis: request.basis,
        contract_type: request.contractType,
        currency: request.currency,
        duration: request.duration,
        duration_unit: request.durationUnit,
        underlying_symbol: request.underlyingSymbol,
        subscribe: request.subscribe ? 1 : 0,
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
    return this.wsManager.subscribe('proposal', {
      proposal: 1,
      amount: request.amount,
      basis: request.basis,
      contract_type: request.contractType,
      currency: request.currency,
      duration: request.duration,
      duration_unit: request.durationUnit,
      underlying_symbol: request.underlyingSymbol,
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
