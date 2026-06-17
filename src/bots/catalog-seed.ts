
// ============================================================
// NEXORA FOREX — Bot Catalog Seed
// ============================================================
// Popula o catálogo com os bots oficiais no arranque do servidor.
//
// COMO GERIR BOTS:
//   → Adicionar: cria uma nova entrada neste array e reinicia
//   → Desactivar: isActive: false  (continua no catálogo mas não aparece aos utilizadores)
//   → Remover: apaga a entrada e reinicia
//
// ESTRATÉGIAS DISPONÍVEIS (bot.manager.ts):
//   'scalping'         → ScalpingStrategy  (CALL/PUT rápidos, para após N perdas seguidas)
//   'martingale'       → MartingaleStrategy (dobra stake após perda)
//   'anti_martingale'  → MartingaleStrategy com resetOnWin: true
//   'trend_following'  → TrendFollowingStrategy
//
// ACESSO:
//   Apenas o administrador gere este ficheiro.
//   Os utilizadores listam e executam os bots via WS (list_bots / start_catalog_bot).
// ============================================================

import { BotCatalog, CreateCatalogBotDto } from './bot-catalog.js';
import logger from '@utils/logger.js';

// ─── Definição dos bots oficiais ─────────────────────────────
// Edita apenas este array para adicionar / remover bots.

const CATALOG_BOTS: CreateCatalogBotDto[] = [

  // ──────────────────────────────────────────────────────────
  // BOT 1 — Simple CALL/PUT
  // ──────────────────────────────────────────────────────────
  // Abre contratos CALL/PUT alternados com stake fixo.
  // Não ajusta stake. Para quando atinge maxLoss ou maxProfit.
  // Usa ScalpingStrategy com maxConsecutiveLosses: 0 (sem pausa
  // automática por perdas consecutivas).
  {
    name:        'GPT5.6',
    description: 'Abre contratos alternando CALL e PUT com stake fixo de $1. Sem ajuste de stake. Para automaticamente ao atingir $20 de perda ou $50 de lucro. Ideal para testar a ligação e ver o fluxo de trades em tempo real.',
    strategy:    'scalping',
    tags:        ['simples', 'iniciante', 'stake-fixo', 'teste'],
    isActive:    true,
    createdBy:   'admin',
    defaultConfig: {
      symbol:       '1HZ100V',       // Volatility 100 Index (1s) — disponível 24/7
      contractType: 'CALL',
      duration:     5,
      durationUnit: 'ticks',
      initialStake: 1,               // $1 por trade
      currency:     'USD',
      maxTrades:    0,               // 0 = sem limite de trades
      maxLoss:      20,              // para se perda acumulada >= $20
      maxProfit:    50,              // para se lucro acumulado >= $50
      strategyParams: {
        maxConsecutiveLosses: 0,     // 0 = nunca pausa por perdas consecutivas
      },
    },
  },

  // ──────────────────────────────────────────────────────────
  // BOT 2 — Martingale Classic
  // ──────────────────────────────────────────────────────────
  // Após cada perda, duplica o stake.
  // Após um ganho, reseta para o stake inicial.
  // Progressão: $1 → $2 → $4 → $8 → $16 → $32 → $64 (tecto)
  // ATENÇÃO: risco elevado — usar apenas em conta demo.
  {
    name:        'MORGAM',
    description: 'Duplica o stake após cada perda e reseta no ganho. Recupera perdas rapidamente, mas o risco cresce exponencialmente. Progressão máxima: $1→$2→$4→$8→$16→$32→$64. Recomendado apenas em conta demo.',
    strategy:    'martingale',
    tags:        ['martingale', 'risco-alto', 'recuperação', 'demo'],
    isActive:    true,
    createdBy:   'admin',
    defaultConfig: {
      symbol:       '1HZ100V',
      contractType: 'CALL',
      duration:     5,
      durationUnit: 'ticks',
      initialStake: 1,
      currency:     'USD',
      maxTrades:    0,
      maxLoss:      127,             // cobre 7 perdas seguidas (1+2+4+8+16+32+64)
      maxProfit:    100,
      strategyParams: {
        multiplier:  2,              // dobra após perda
        maxStake:    64,             // tecto do stake
        resetOnWin:  false,          // false = Martingale | true = Anti-Martingale
      },
    },
  },

];

// ─── Seed ────────────────────────────────────────────────────

export async function seedBotCatalog(): Promise<void> {
  const existing = await BotCatalog.listAll();

  if (existing.length > 0) {
    logger.info(`[BotCatalog Seed] Catálogo já tem ${existing.length} bot(s) — seed ignorado.`);
    return;
  }

  logger.info('[BotCatalog Seed] A popular o catálogo de bots...');

  for (const dto of CATALOG_BOTS) {
    await BotCatalog.create(dto);
  }

  const all = await BotCatalog.listAll();
  logger.info(`[BotCatalog Seed] ✅ ${all.length} bot(s) registados:`);
  all.forEach(b =>
    logger.info(`  • [${b.strategy.padEnd(16)}] ${b.name} — ${b.isActive ? 'activo' : 'inactivo'}`),
  );
}
