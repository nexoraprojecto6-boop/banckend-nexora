# Migração para auto_* da Deriv

## 1. Copiar (substituir) em src/bots/
bot.types.ts, deriv-ws.adapter.ts, auto-run.bot.ts (novo), bot.manager.ts,
catalog-seed.ts, strategies/base.strategy.ts (agora só tipos)

## 2. APAGAR de src/bots/strategies/
alfha, even-odd-recovery, even-odd-smart, martingale, over1-recovery,
scalper, scalping, trend-following (*.strategy.ts) e digit-utils.ts

## 3. Não mexer
bot-catalog.ts e bots.routes.ts (compatíveis tal como estão)

## 4. Editar src/server.ts (5 alterações)

A) Imports (linha do bot.types):
   import { BotEvent, BotConfig, BotStrategyType, DerivWsAdapter, AutoStrategyParams } from './bots/bot.types.js';

B) Substituir TODA a função contractTypesFor por:
   function contractTypesFor(_strategy: BotStrategyType, config: BotConfig): string[] {
     return [config.contractType];
   }

C) Em start_catalog_bot, trocar "as Record<string, unknown>" por "as AutoStrategyParams"
   no bloco strategyParams do finalConfig:
   ...((configOverride.strategyParams as AutoStrategyParams) ?? {}),

D) Em start_catalog_bot, substituir
       await manager.startBot(created.id);
   por
       try {
         await manager.startBot(created.id);
       } catch (e) {
         await manager.deleteBot(created.id);
         throw e;
       }

E) Em pause_bot: "manager.pauseBot(botId);" → "await manager.pauseBot(botId);"
   Em handleManualBuy: "adapter.waitForContract(bought.contractId)" → "adapter.watchContract(bought.contractId)"
