// ============================================================
// NEXORA FOREX — Bot Catalog Seed v3 (auto_* da Deriv)
// ============================================================
// Estratégias disponíveis (a Deriv corre o ciclo):
//   'martingale' → multiplica o stake após cada perda, reseta no ganho
//   'dalembert'  → +unit após perda, −unit após ganho (nunca abaixo do inicial)
//
// A Deriv EXIGE take_profit (maxProfit) e stop_loss (maxLoss) > 0.
// Não há filtro de entrada: a Deriv compra em ciclo contínuo.
// Dígitos exigem strategyParams.barrier.
//
// ATENÇÃO: nenhuma destas estratégias cria vantagem estatística.
// Recomendado apenas em conta demo.
// ============================================================

import { BotCatalog, CreateCatalogBotDto } from './bot-catalog.js';
import logger from '@utils/logger.js';

const CATALOG_BOTS: CreateCatalogBotDto[] = [
  {
    name: 'GPT5.6',
    description: 'CALL de 5 ticks com D\'Alembert: +$1 após perda, −$1 após ganho, teto de $10. Para ao atingir $20 de perda ou $50 de lucro. Recomendado apenas em conta demo.',
    strategy: 'dalembert',
    tags: ['dalembert', 'risco-medio', 'demo'],
    isActive: true,
    createdBy: 'admin',
    defaultConfig: {
      symbol: '1HZ100V', contractType: 'CALL', duration: 5, durationUnit: 'ticks',
      initialStake: 1, currency: 'USD', maxTrades: 0, maxLoss: 20, maxProfit: 50,
      strategyParams: { unit: 1, maxStake: 10 },
    },
  },
  {
    name: 'MORGAM',
    description: 'CALL de 5 ticks com Martingale: duplica o stake após cada perda e reseta no ganho. Progressão máxima $1→$64. Risco elevado. Recomendado apenas em conta demo.',
    strategy: 'martingale',
    tags: ['martingale', 'risco-alto', 'demo'],
    isActive: true,
    createdBy: 'admin',
    defaultConfig: {
      symbol: '1HZ100V', contractType: 'CALL', duration: 5, durationUnit: 'ticks',
      initialStake: 1, currency: 'USD', maxTrades: 0, maxLoss: 127, maxProfit: 100,
      strategyParams: { multiplier: 2, maxStake: 64 },
    },
  },
  {
    name: 'SCALPER',
    description: 'DIGITOVER 2 (1 tick) no Volatility 100 (1s) com Martingale x2. Risco elevado. Recomendado apenas em conta demo.',
    strategy: 'martingale',
    tags: ['digitos', 'martingale', 'risco-alto', 'demo'],
    isActive: true,
    createdBy: 'admin',
    defaultConfig: {
      symbol: '1HZ100V', contractType: 'DIGITOVER', duration: 1, durationUnit: 'ticks',
      initialStake: 0.35, currency: 'USD', maxTrades: 0, maxLoss: 10, maxProfit: 5,
      strategyParams: { barrier: 2, multiplier: 2, maxStake: 12 },
    },
  },
  {
    name: 'ALFHA',
    description: 'DIGITUNDER 7 (1 tick) no Volatility 100 Index com Martingale x2. Risco elevado. Recomendado apenas em conta demo.',
    strategy: 'martingale',
    tags: ['digitos', 'martingale', 'risco-alto', 'demo'],
    isActive: true,
    createdBy: 'admin',
    defaultConfig: {
      symbol: 'R_100', contractType: 'DIGITUNDER', duration: 1, durationUnit: 'ticks',
      initialStake: 0.4, currency: 'USD', maxTrades: 0, maxLoss: 10, maxProfit: 5,
      strategyParams: { barrier: 7, multiplier: 2, maxStake: 12 },
    },
  },
  {
    name: 'ETHEREUM',
    description: 'DIGITOVER 1 (1 tick) com D\'Alembert: +$0.50 após perda, −$0.50 após ganho, teto de $10. Recomendado apenas em conta demo.',
    strategy: 'dalembert',
    tags: ['digitos', 'dalembert', 'demo'],
    isActive: true,
    createdBy: 'admin',
    defaultConfig: {
      symbol: '1HZ100V', contractType: 'DIGITOVER', duration: 1, durationUnit: 'ticks',
      initialStake: 0.5, currency: 'USD', maxTrades: 0, maxLoss: 20, maxProfit: 10,
      strategyParams: { barrier: 1, unit: 0.5, maxStake: 10 },
    },
  },
  {
    name: 'LITECOIN',
    description: 'DIGITEVEN (1 tick) com Martingale x2, teto de $16. Recomendado apenas em conta demo.',
    strategy: 'martingale',
    tags: ['digitos', 'par-impar', 'martingale', 'risco-alto', 'demo'],
    isActive: true,
    createdBy: 'admin',
    defaultConfig: {
      symbol: '1HZ100V', contractType: 'DIGITEVEN', duration: 1, durationUnit: 'ticks',
      initialStake: 0.5, currency: 'USD', maxTrades: 0, maxLoss: 20, maxProfit: 10,
      strategyParams: { multiplier: 2, maxStake: 16 },
    },
  },
  {
    name: 'BITCOIN',
    description: 'DIGITODD (1 tick) com D\'Alembert: +$0.50 após perda, −$0.50 após ganho, teto de $10. Recomendado apenas em conta demo.',
    strategy: 'dalembert',
    tags: ['digitos', 'par-impar', 'dalembert', 'demo'],
    isActive: true,
    createdBy: 'admin',
    defaultConfig: {
      symbol: '1HZ100V', contractType: 'DIGITODD', duration: 1, durationUnit: 'ticks',
      initialStake: 0.5, currency: 'USD', maxTrades: 0, maxLoss: 20, maxProfit: 10,
      strategyParams: { unit: 0.5, maxStake: 10 },
    },
  },
];

export async function seedBotCatalog(): Promise<void> {
  const existing = await BotCatalog.listAll();
  if (existing.length > 0) {
    logger.info(`[BotCatalog Seed] Catálogo já tem ${existing.length} bot(s) — seed ignorado.`);
    return;
  }

  logger.info('[BotCatalog Seed] A popular o catálogo de bots...');
  for (const dto of CATALOG_BOTS) await BotCatalog.create(dto);

  const all = await BotCatalog.listAll();
  logger.info(`[BotCatalog Seed] ✅ ${all.length} bot(s) registados:`);
  all.forEach((b) =>
    logger.info(`  • [${b.strategy.padEnd(10)}] ${b.name} — ${b.isActive ? 'activo' : 'inactivo'}`),
  );
}
