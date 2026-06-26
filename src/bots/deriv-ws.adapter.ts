// ============================================================
// NEXORA FOREX — Deriv WS Adapter
// ============================================================
// Fluxo correcto da API Deriv para comprar um contrato:
//   1. Enviar "proposal" com os parâmetros do contrato
//   2. Aguardar resposta "proposal" com o id da proposta
//   3. Enviar "buy" com o proposalId + ask_price
//   4. Aguardar resposta "buy" com o contract_id
//   5. Subscrever "proposal_open_contract" para acompanhar
//   6. Resolver quando contract fecha (is_sold / status=sold)
//
// ─── Versão migrada para o WebSocketManager ───────────────────
// Esta versão NÃO regista listeners 'message' diretamente no
// socket bruto. Em vez disso usa wsManager.send() (que já devolve
// a resposta correspondente ao req_id, via pendingRequests) e
// wsManager.subscribe()/unsubscribe() (que já tratam o
// subscription.id real da Deriv).
//
// Isto resolve de raiz o problema de listeners acumulados:
// antes, cada chamada a buyContract/waitForContract/subscribeToTicks
// registava o seu próprio handler 'message' no WebSocket — com
// muitos bots simultâneos, o mesmo evento 'message' do socket era
// testado contra dezenas de listeners (custo O(n) por mensagem).
// Agora, há um único ponto de entrada de mensagens por sessão (o
// WebSocketManager), e o roteamento para o pedido/subscrição certa
// é feito internamente por req_id / subscription.id — O(1) por
// mensagem, independentemente de quantas operações estão em curso.
// ============================================================

import { WebSocketManager } from '@websocket/manager.js';
import { DerivWsAdapter } from './bot.types.js';
import logger from '@utils/logger.js';

// Preserva o código de erro da Deriv (ex: "InsufficientBalance",
// "InputValidationFailed") para que as estratégias possam reagir de
// forma específica, em vez de fazer parsing frágil da mensagem.
export class DerivApiError extends Error {
  code: string;
  constructor(message: string, code: string) {
    super(message);
    this.name = 'DerivApiError';
    this.code = code;
  }
}

// ─── Mapeamento durationUnit → duration_unit da Deriv ────────
const DURATION_UNIT_MAP: Record<string, string> = {
  ticks: 't',
  s:     's',
  m:     'm',
  h:     'h',
  d:     'd',
  t:     't',
};

export function createDerivWsAdapter(getWsManager: () => WebSocketManager | null): DerivWsAdapter {

  function requireManager(): WebSocketManager {
    const manager = getWsManager();
    if (!manager) {
      throw new Error('Deriv WebSocket não está disponível');
    }
    return manager;
  }

  // Lê o código de erro Deriv de uma resposta, se existir, e lança
  // DerivApiError em vez de devolver a mensagem crua — mantém o
  // mesmo comportamento que as estratégias já esperam.
  function throwIfError(msg: any, fallbackMessage: string): void {
    if (msg?.error) {
      throw new DerivApiError(
        msg.error.message ?? fallbackMessage,
        msg.error.code ?? 'UNKNOWN',
      );
    }
  }

  // ─── buyContract ──────────────────────────────────────────
  // Fluxo: proposal → buy → contractId
  // Cada send() é isolado por req_id dentro do WebSocketManager —
  // múltiplos bots a comprar em simultâneo na mesma sessão não
  // podem cruzar respostas entre si, porque não há estado global
  // partilhado: o manager devolve exatamente a resposta do req_id
  // que esta chamada enviou.

  async function buyContract(params: {
    symbol: string;
    contractType: string;
    stake: number;
    duration: number;
    durationUnit: string;
    currency: string;
  }): Promise<{ contractId: string }> {
    const manager = requireManager();
    const durationUnit = DURATION_UNIT_MAP[params.durationUnit] ?? params.durationUnit;

    // ── Passo 1: pedir proposal ─────────────────────────────
    const proposalResponse = await manager.send({
      proposal:          1,
      amount:            params.stake,
      basis:             'stake',
      contract_type:     params.contractType,
      currency:          params.currency,
      duration:          params.duration,
      duration_unit:     durationUnit,
      underlying_symbol: params.symbol,
    }, 15_000);

    throwIfError(proposalResponse, 'Erro na proposta');

    const proposalId = proposalResponse?.proposal?.id;
    if (!proposalId) {
      throw new Error('Resposta de proposal inválida');
    }

    logger.debug('[DerivAdapter] proposal recebida', {
      proposalId, symbol: params.symbol, stake: params.stake,
      contractType: params.contractType, duration: params.duration,
      durationUnit,
    });

    // ── Passo 2: comprar com o proposalId ──────────────────
    // Reler requireManager() — se a sessão trocou de conta entre o
    // passo 1 e aqui, o manager antigo pode já não ser o ativo.
    const manager2 = requireManager();

    const buyResponse = await manager2.send({
      buy:   proposalId,
      price: params.stake,
    }, 15_000);

    throwIfError(buyResponse, 'Erro ao comprar contrato');

    const contractId = buyResponse?.buy?.contract_id;
    if (!contractId) {
      throw new Error('Resposta buy inválida');
    }

    logger.debug('[DerivAdapter] buy concluído', { proposalId, contractId });
    return { contractId: String(contractId) };
  }

  // ─── waitForContract ──────────────────────────────────────
  // Subscreve proposal_open_contract e resolve quando fechar.
  // Usa wsManager.subscribe(), que já trata o subscription.id real
  // da Deriv internamente — aqui só filtramos pelo contract_id certo
  // dentro do handler, e cancelamos a subscrição com unsubscribe()
  // assim que tivermos o resultado (equivalente ao "forget" antigo,
  // mas sem qualquer listener registado manualmente no socket).
  //
  // Timeout de 35s — contratos de duração curta (ticks/segundos, o
  // caso comum dos bots do catálogo) resolvem bem dentro disto.

  async function waitForContract(contractId: string): Promise<{
    profit: number;
    won: boolean;
    entryTick: number;
    exitTick: number;
  }> {
    const manager = requireManager();

    return new Promise((resolve, reject) => {
      let settled = false;
      let subscriptionId: string | null = null;
      let timer: ReturnType<typeof setTimeout>;

      function cleanup() {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (subscriptionId) {
          manager.unsubscribe(subscriptionId).catch((err: any) => {
            logger.warn('[DerivAdapter] Falha ao cancelar subscrição de contrato', {
              contractId, error: err?.message,
            });
          });
        }
      }

      function handler(msg: any) {
        if (settled) return;
        try {
          if (msg.error) {
            cleanup();
            reject(new DerivApiError(
              msg.error.message ?? 'Erro ao acompanhar contrato',
              msg.error.code ?? 'UNKNOWN',
            ));
            return;
          }

          const poc = msg.proposal_open_contract;
          if (!poc || String(poc.contract_id) !== contractId) return;

          if (poc.status === 'sold' || poc.is_sold || poc.is_expired || poc.is_settleable) {
            cleanup();
            const profit = parseFloat(poc.profit ?? '0');
            resolve({
              profit,
              won:       profit > 0,
              entryTick: poc.entry_tick ?? 0,
              exitTick:  poc.exit_tick  ?? 0,
            });
          }
        } catch (err) {
          logger.warn('[DerivAdapter] Erro ao processar update de contrato', { contractId, err });
        }
      }

      timer = setTimeout(() => {
        cleanup();
        reject(new Error('Timeout aguardando resultado do contrato'));
      }, 35_000);

      subscriptionId = manager.subscribe(
        'proposal_open_contract',
        { proposal_open_contract: 1, contract_id: parseInt(contractId, 10) },
        handler,
      );
    });
  }

  // ─── subscribeToTicks ─────────────────────────────────────
  // Mesma lógica: wsManager.subscribe() trata o subscription.id
  // real, e unsubscribe() faz o "forget" correto.

  function subscribeToTicks(
    symbol: string,
    onTick: (price: number) => void,
  ): { unsubscribe: () => void } {
    const manager = getWsManager();
    if (!manager) {
      logger.warn('[DerivAdapter] WebSocket indisponível para ticks');
      return { unsubscribe: () => {} };
    }

    let unsubscribed = false;

    function handler(msg: any) {
      if (unsubscribed) return;
      if (msg.msg_type === 'tick' && msg.tick?.symbol === symbol) {
        onTick(parseFloat(msg.tick.quote));
      }
    }

    const subscriptionId = manager.subscribe('tick', { ticks: symbol }, handler);
    logger.debug('[DerivAdapter] Subscrito a ticks', { symbol });

    return {
      unsubscribe: () => {
        if (unsubscribed) return;
        unsubscribed = true;
        manager.unsubscribe(subscriptionId).catch((err: any) => {
          logger.warn('[DerivAdapter] Falha ao cancelar subscrição de ticks', { symbol, error: err?.message });
        });
        logger.debug('[DerivAdapter] Tick subscription cancelada', { symbol });
      },
    };
  }

  return { buyContract, waitForContract, subscribeToTicks };
}
