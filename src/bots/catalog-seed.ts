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
//   'scalping'               → ScalpingStrategy  (CALL/PUT rápidos, para após N perdas seguidas)
//   'martingale'             → MartingaleStrategy (dobra stake após perda)
//   'anti_martingale'        → MartingaleStrategy com resetOnWin: true
//   'trend_following'        → TrendFollowingStrategy
//   'scalper'                → ScalperStrategy (DIGITOVER + Gale)
//   'alfha'                  → AlfhaStrategy (DIGITOVER/DIGITUNDER conforme o dígito + Gale)
//   'over1_recovery'         → Over1RecoveryStrategy (DIGITOVER 1 + Gale de recuperação por payout real) — nome de exibição: "ETHEREUM"
//   'even_odd_recovery'      → EvenOddRecoveryStrategy (DIGITEVEN/DIGITODD + Gale de recuperação por payout real) — nome de exibição: "LITECOIN"
//   'even_odd_smart'         → EvenOddSmartStrategy (DIGITEVEN/DIGITODD 1 tick + martingale de recuperação com tetos de risco) — nome de exibição: "BITCOIN"
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

  // ──────────────────────────────────────────────────────────
  // BOT 3 — SCALPER (Digit Over + Gale)
  // ──────────────────────────────────────────────────────────
  // Compra DIGITOVER 2 (1 tick) sempre que o último dígito observado
  // for baixo (<= 2). Gale (martingale) desde a 1ª perda, com teto
  // de níveis, e pausa automática temporária ao atingir uma perda
  // acumulada.
  // ATENÇÃO: alto risco — usar apenas em conta demo. Dígitos em
  // índices sintéticos são praticamente aleatórios e independentes
  // entre ticks; esta análise não constitui vantagem estatística
  // comprovada.
  {
    name:        'SCALPER',
    description: 'Compra DIGITOVER 2 (1 tick) no Volatility 100 (1s) Index sempre que o último dígito é baixo. Usa Gale (martingale) desde a 1ª perda, com pausa automática temporária ao atingir uma perda acumulada. Recomendado apenas em conta demo.',
    strategy:    'scalper',
    tags:        ['digitos', 'gale', 'risco-alto', 'demo'],
    isActive:    true,
    createdBy:   'admin',
    defaultConfig: {
      symbol:       '1HZ100V',       // Volatility 100 (1s) Index
      contractType: 'DIGITOVER',
      duration:     1,
      durationUnit: 'ticks',
      initialStake: 0.35,
      currency:     'USD',
      maxTrades:    0,
      maxLoss:      10,
      maxProfit:    5,
      strategyParams: {
        predictDigit:        2,      // barreira: dígito > 2 (gatilho cobre 0,1,2)
        gale:                2,      // multiplicador do stake por perda
        lossesToStartGale:   0,      // Gale começa já na 1ª perda
        maxMartingaleSteps:  10,     // teto de níveis antes de resetar
        pauseLossLimit:      20,     // pausa temporária ao acumular $20 de perda
        pauseSeconds:        10,     // duração da pausa automática
        decimalPlaces:       2,      // casas decimais do símbolo, para extrair o dígito certo
      },
    },
  },

  // ──────────────────────────────────────────────────────────
  // BOT 4 — ALFHA (Digit Over/Under + Gale)
  // ──────────────────────────────────────────────────────────
  // Compra DIGITOVER 2 quando o último dígito é baixo (<= 2), ou
  // DIGITUNDER 7 quando é alto (>= 7). Gale desde a 1ª perda, com
  // teto de níveis, e pausa automática temporária ao atingir uma
  // perda acumulada.
  // ATENÇÃO: alto risco — usar apenas em conta demo.
  {
    name:        'ALFHA',
    description: 'Compra DIGITOVER 2 ou DIGITUNDER 7 (1 tick) no Volatility 100 Index conforme o último dígito observado. Usa Gale (martingale) desde a 1ª perda, com pausa automática temporária ao atingir uma perda acumulada. Recomendado apenas em conta demo.',
    strategy:    'alfha',
    tags:        ['digitos', 'gale', 'risco-alto', 'demo'],
    isActive:    true,
    createdBy:   'admin',
    defaultConfig: {
      symbol:       'R_100',         // Volatility 100 Index
      contractType: 'DIGITOVER',     // trocado dinamicamente para DIGITUNDER conforme o sinal
      duration:     1,
      durationUnit: 'ticks',
      initialStake: 0.4,
      currency:     'USD',
      maxTrades:    0,
      maxLoss:      10,
      maxProfit:    5,
      strategyParams: {
        digitOver:           2,      // entra em Over quando o dígito é <= 2
        digitUnder:          7,      // entra em Under quando o dígito é >= 7
        gale:                2,      // multiplicador do stake por perda
        lossesToStartGale:   0,      // Gale começa já na 1ª perda
        maxMartingaleSteps:  10,     // teto de níveis antes de resetar
        pauseLossLimit:      20,     // pausa temporária ao acumular $20 de perda
        pauseSeconds:        10,     // duração da pausa automática
      },
    },
  },

  // ──────────────────────────────────────────────────────────
  // BOT 5 — ETHEREUM (Digit Over 1 + Gale de Recuperação)
  // ──────────────────────────────────────────────────────────
  // Nome de exibição "ETHEREUM"; estratégia interna continua
  // 'over1_recovery' (não confundir com o ativo cripto Ethereum —
  // é só o nome comercial escolhido para este bot).
  //
  // Compra DIGITOVER 1 (1 tick) sempre que o último dígito observado
  // for baixo (<= 1).
  //   • 1ª perda do ciclo: consulta o payout real da Deriv e calcula
  //     o stake exato para recuperar 100% do valor perdido + uma
  //     margem extra (recoveryMarginPercent).
  //   • 2ª perda SEGUIDA em diante: em vez de continuar escalando,
  //     derruba o nível — entra com um stake pequeno e protegido
  //     (protectiveStakePercent, % do stake inicial) até ganhar
  //     (reseta o ciclo) ou até a pausa automática entrar.
  // Sempre respeitando um teto absoluto de stake (maxStakeCap) e um
  // teto de perdas seguidas (maxMartingaleSteps).
  // ATENÇÃO: alto risco — usar apenas em conta demo. Dígitos em
  // índices sintéticos são praticamente aleatórios e independentes
  // entre ticks; recuperar a perda + margem não muda o valor
  // esperado do jogo, só muda o formato do risco.
  {
    name:        'ETHEREUM',
    description: 'Compra DIGITOVER 1 (1 tick) no Volatility 100 (1s) Index sempre que o último dígito é baixo. Na 1ª perda, tenta recuperar 100% do valor + 10% consultando o payout real. Em perdas seguidas, protege a banca entrando com stake reduzido em vez de continuar escalando. Recomendado apenas em conta demo.',
    strategy:    'over1_recovery',
    tags:        ['digitos', 'gale', 'recuperação', 'risco-alto', 'demo'],
    isActive:    true,
    createdBy:   'admin',
    defaultConfig: {
      symbol:       '1HZ100V',       // Volatility 100 (1s) Index
      contractType: 'DIGITOVER',
      duration:     1,
      durationUnit: 'ticks',
      initialStake: 0.5,
      currency:     'USD',
      maxTrades:    0,
      maxLoss:      20,
      maxProfit:    10,
      strategyParams: {
        digitBarrier:           1,    // Over 1 — perde se o dígito for 0 ou 1
        recoveryMarginPercent:  10,   // margem extra de +10% sobre o stake inicial (só na 1ª perda)
        protectiveStakePercent: 25,   // % do stake inicial usado a partir da 2ª perda seguida
        maxMartingaleSteps:     6,    // teto de perdas seguidas antes de cortar e resetar
        maxStakeCap:            50,   // teto absoluto de stake — nunca ultrapassado
        minStake:               0.35, // piso de stake (mínimo aceito pela Deriv)
        pauseLossLimit:         20,   // pausa temporária ao acumular $20 de perda
        pauseSeconds:           10,   // duração da pausa automática
        decimalPlaces:          2,    // casas decimais do símbolo
      },
    },
  },

  // ──────────────────────────────────────────────────────────
  // BOT 6 — LITECOIN (Digit Even/Odd + Gale de Recuperação)
  // ──────────────────────────────────────────────────────────
  // Nome de exibição "LITECOIN"; estratégia interna
  // 'even_odd_recovery' (não confundir com o ativo cripto Litecoin —
  // é só o nome comercial escolhido para este bot, no mesmo padrão
  // do ETHEREUM).
  //
  // Compra DIGITEVEN (1 tick) sempre que os últimos `streakTrigger`
  // dígitos observados forem todos ímpares em sequência.
  //   • 1ª perda do ciclo: consulta o payout real da Deriv e calcula
  //     o stake exato para recuperar 100% do valor perdido + uma
  //     margem extra (recoveryMarginPercent).
  //   • 2ª perda SEGUIDA em diante: em vez de continuar escalando,
  //     derruba o nível — entra com um stake pequeno e protegido
  //     (protectiveStakePercent, % do stake inicial) até ganhar
  //     (reseta o ciclo) ou até a pausa automática entrar.
  // Sempre respeitando um teto absoluto de stake (maxStakeCap) e um
  // teto de perdas seguidas (maxMartingaleSteps).
  // ATENÇÃO: alto risco — usar apenas em conta demo. Cada tick em
  // índices sintéticos é estatisticamente independente do anterior;
  // uma sequência de dígitos ímpares NÃO aumenta a probabilidade do
  // próximo ser par (falácia do apostador). O streakTrigger é só um
  // filtro de entrada, não uma previsão real, e recuperar a perda +
  // margem não muda o valor esperado do jogo, só muda o formato do
  // risco.
  {
    name:        'LITECOIN',
    description: 'Compra DIGITEVEN (1 tick) no Volatility 100 (1s) Index após uma sequência de dígitos ímpares seguidos. Na 1ª perda, tenta recuperar 100% do valor + 10% consultando o payout real. Em perdas seguidas, protege a banca entrando com stake reduzido em vez de continuar escalando. Recomendado apenas em conta demo.',
    strategy:    'even_odd_recovery',
    tags:        ['digitos', 'gale', 'recuperação', 'risco-alto', 'demo'],
    isActive:    true,
    createdBy:   'admin',
    defaultConfig: {
      symbol:       '1HZ100V',       // Volatility 100 (1s) Index
      contractType: 'DIGITEVEN',
      duration:     1,
      durationUnit: 'ticks',
      initialStake: 0.5,
      currency:     'USD',
      maxTrades:    0,
      maxLoss:      20,
      maxProfit:    10,
      strategyParams: {
        targetParity:           'even', // 'even' -> DIGITEVEN | 'odd' -> DIGITODD
        streakTrigger:          4,     // entra após 4 ticks seguidos com paridade ímpar
        recoveryMarginPercent:  10,   // margem extra de +10% sobre o stake inicial (só na 1ª perda)
        protectiveStakePercent: 25,   // % do stake inicial usado a partir da 2ª perda seguida
        maxMartingaleSteps:     6,    // teto de perdas seguidas antes de cortar e resetar
        maxStakeCap:            50,   // teto absoluto de stake — nunca ultrapassado
        minStake:               0.35, // piso de stake (mínimo aceito pela Deriv)
        pauseLossLimit:         20,   // pausa temporária ao acumular $20 de perda
        pauseSeconds:           10,   // duração da pausa automática
        decimalPlaces:          2,    // casas decimais do símbolo
      },
    },
  },

  // ──────────────────────────────────────────────────────────
  // BOT 7 — BITCOIN (Digit Even/Odd 1 tick + Martingale Inteligente)
  // ──────────────────────────────────────────────────────────
  // Nome de exibição "BITCOIN"; estratégia interna 'even_odd_smart'
  // (só o nome comercial, no mesmo padrão do ETHEREUM/LITECOIN).
  //
  // Compra DIGITEVEN ou DIGITODD (1 tick) depois de `streakLength`
  // ticks seguidos com a mesma paridade ('follow' = aposta na mesma
  // paridade; 'reverse' = na oposta).
  //   • Cada perda: stake = (perda acumulada + lucro alvo) / retorno
  //     líquido real do payout (lido uma vez da Deriv, sem atrasar
  //     as entradas).
  //   • Vitória parcial só reduz a dívida; o ciclo reseta quando ela
  //     é totalmente recuperada.
  //   • Tetos: maxMartingaleSteps, maxStakeMultiplier e o que resta
  //     do maxLoss — ao atingir qualquer um, corta a perda e volta à
  //     stake inicial. Pausa curta após pauseAfterLosses perdas seguidas.
  // ATENÇÃO: alto risco — usar apenas em conta demo. Os dígitos são
  // independentes entre ticks; a sequência é só um filtro de entrada,
  // não uma previsão, e a recuperação não muda o valor esperado do
  // jogo, só o formato do risco.
  {
    name:        'BITCOIN',
    description: 'Compra DIGITEVEN ou DIGITODD (1 tick) no Volatility 100 (1s) Index após uma sequência de dígitos com a mesma paridade. Martingale de recuperação calculado sobre o payout real, com tetos de risco ligados ao stop de perda e pausa automática após perdas seguidas. Recomendado apenas em conta demo.',
    strategy:    'even_odd_smart',
    tags:        ['digitos', 'par-impar', 'martingale', 'recuperação', 'risco-alto', 'demo'],
    isActive:    true,
    createdBy:   'admin',
    defaultConfig: {
      symbol:       '1HZ100V',       // Volatility 100 (1s) Index
      contractType: 'DIGITEVEN',     // só referência; a estratégia escolhe EVEN/ODD a cada sinal
      duration:     1,
      durationUnit: 'ticks',
      initialStake: 0.5,
      currency:     'USD',
      maxTrades:    0,
      maxLoss:      20,
      maxProfit:    10,
      strategyParams: {
        mode:                 'follow', // 'follow' = mesma paridade da sequência | 'reverse' = oposta
        streakLength:         3,        // ticks seguidos com a mesma paridade (0 = todo tick)
        payoutRatio:          0.95,     // reserva; o retorno líquido real vem de getProposal
        recoveryProfitFactor: 0.5,      // lucro alvo extra na recuperação (× stake inicial)
        maxMartingaleSteps:   6,        // teto de níveis antes de cortar e resetar
        maxStakeMultiplier:   40,       // teto da stake = stake inicial × 40
        pauseAfterLosses:     4,        // pausa temporária após 4 perdas seguidas
        pauseSeconds:         8,        // duração da pausa automática
        decimalPlaces:        2,        // casas decimais do símbolo
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
