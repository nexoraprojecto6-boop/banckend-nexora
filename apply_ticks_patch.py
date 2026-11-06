#!/usr/bin/env python3
"""
Aplica o patch de ticks (subscribe_ticks / ticks_snapshot) ao src/server.ts.

Uso (na raiz do backend):
    python3 apply_ticks_patch.py src/server.ts

- Faz backup em server.ts.bak
- Idempotente: se um passo já estiver aplicado, salta-o
- Aborta SEM escrever nada se algum ponto de ancoragem não for encontrado
"""
import sys, shutil, re

path = sys.argv[1] if len(sys.argv) > 1 else "src/server.ts"
src = open(path, encoding="utf-8").read()
orig = src
log = []

def once(s, anchor, name):
    n = s.count(anchor)
    if n != 1:
        sys.exit(f"ERRO [{name}]: âncora encontrada {n}x (esperado 1). Nada foi escrito.\n  âncora: {anchor[:80]!r}")

# ── S1: interface ClientSession ──────────────────────────────
if "tickHandle:" not in src.split("interface ActiveSymbol")[0]:
    a = "msgCount:             number;\n"
    once(src, a, "S1")
    src = src.replace(a, a + "  tickHandle:           { close: () => void } | null;\n"
                             "  tickSymbol:           string | null;\n"
                             "  tickGen:              number;\n")
    log.append("S1 interface")
else:
    # normaliza indentação da linha tickHandle (estava com 4 espaços)
    src = re.sub(r"\n {4}tickHandle:( +\{ close)", r"\n  tickHandle:\1", src)
    log.append("S1 já existia (indentação normalizada)")

# ── S2: inicialização da sessão ──────────────────────────────
if "tickHandle: null" not in src:
    a = "msgWindowStart: 0, msgCount: 0,\n  };"
    once(src, a, "S2")
    src = src.replace(a, "msgWindowStart: 0, msgCount: 0,\n    tickHandle: null, tickSymbol: null, tickGen: 0,\n  };")
    log.append("S2 init da sessão")

# ── S3: startTicks / stopTicks ───────────────────────────────
S3 = '''// ─── Ticks do gráfico (ligação autenticada da sessão) ─────────
// ticks_history + subscribe:1 → 1.ª resposta = histórico ('ticks_snapshot'),
// seguintes = ticks (passam pelo case 'tick' de buildDerivMessageHandler).
const TICKS_DEFAULT_COUNT = 1000;

function stopTicks(session: ClientSession): void {
  session.tickGen++;
  try { session.tickHandle?.close(); } catch { /* ligação já fechada */ }
  session.tickHandle = null;
}

async function startTicks(
  clientWs: WebSocket, session: ClientSession, symbol: string, count = TICKS_DEFAULT_COUNT,
): Promise<void> {
  stopTicks(session);
  session.tickSymbol = symbol;
  const gen = session.tickGen;

  let ws: WebSocket;
  try { ws = await waitForDerivWs(session, 5_000); } catch { return; } // a reconexão volta a chamar
  if (gen !== session.tickGen) return;

  session.tickHandle = getMessageRouter(ws).openStream(
    `ui:ticks:${symbol}`,
    { ticks_history: symbol, end: 'latest', count, style: 'ticks' },
    {
      onMessage: (msg) => {
        const h = msg.history as { prices?: number[]; times?: number[] } | undefined;
        if (!h?.prices) return; // ticks seguintes seguem pelo case 'tick'
        sendToClient(clientWs, 'ticks_snapshot', {
          payload: { symbol, prices: h.prices, times: h.times, pip_size: msg.pip_size },
        });
      },
      onError: (err) => sendError(clientWs, `Ticks de ${symbol}: ${err.message}`, 'TICKS_ERROR'),
      onDisconnect: () => { /* scheduleDerivReconnect volta a subscrever */ },
    },
  );
}

'''
if "function startTicks" not in src:
    a = "function getAdapter(session: ClientSession)"
    once(src, a, "S3")
    src = src.replace(a, S3 + a)
    log.append("S3 startTicks/stopTicks")

# ── S4: case 'tick' ──────────────────────────────────────────
old_tick = """      case 'tick':
        sendToClient(clientWs, 'tick', {
          quote:    (msg.tick as any)?.quote,
          epoch:    (msg.tick as any)?.epoch,
          symbol:   (msg.tick as any)?.symbol,
          pip_size: (msg.tick as any)?.pip_size,
        });
        break;
"""
new_tick = """      case 'tick': {
        const t = msg.tick as any;
        sendToClient(clientWs, 'tick', {
          quote:    t?.quote,
          epoch:    t?.epoch,
          symbol:   t?.symbol ?? t?.underlying_symbol ?? session.tickSymbol,
          pip_size: t?.pip_size,
        });
        break;
      }
"""
if old_tick in src:
    src = src.replace(old_tick, new_tick); log.append("S4 case 'tick'")
elif "t?.underlying_symbol" not in src:
    sys.exit("ERRO [S4]: bloco case 'tick' não encontrado. Nada foi escrito.")

# ── S5: re-subscrever após reconexão ─────────────────────────
if "void startTicks(clientWs, session, session.tickSymbol)" not in src:
    a = "sendToClient(clientWs, 'deriv_reconnected', { message: 'Conexão Deriv restabelecida' });\n"
    once(src, a, "S5")
    src = src.replace(a, a + "        if (session.tickSymbol) void startTicks(clientWs, session, session.tickSymbol);\n")
    log.append("S5 reconexão")

# ── S6: authenticateClient ───────────────────────────────────
if "stopTicks(session);\n    session.tickSymbol = null;" not in src:
    a = "    session.symbolsByTypes.clear();\n"
    once(src, a, "S6")
    src = src.replace(a, a + "    stopTicks(session);\n    session.tickSymbol = null; // o frontend volta a subscrever após 'authenticated'\n")
    log.append("S6 authenticateClient")

# ── S7: cleanupSession ───────────────────────────────────────
if "  stopTicks(session);\n  await closeDerivWsGracefully(session);\n  session.adapter = null;" not in src:
    a = "  await closeDerivWsGracefully(session);\n  session.adapter = null;"
    once(src, a, "S7")
    src = src.replace(a, "  stopTicks(session);\n" + a)
    log.append("S7 cleanupSession")

# ── S8: handlers subscribe/unsubscribe ───────────────────────
S8 = """      if (type === 'subscribe_ticks') {
        if (!session.authenticated) { sendError(ws, 'Não autenticado', 'NOT_AUTH'); return; }
        const symbol = String(payload.symbol ?? '');
        if (!/^\\w{2,30}$/.test(symbol)) { sendError(ws, 'Símbolo inválido', 'INVALID_SYMBOL'); return; }
        const count = Math.min(Math.max(Number(payload.count) || TICKS_DEFAULT_COUNT, 1), 5000);
        await startTicks(ws, session, symbol, count);
        return;
      }

      if (type === 'unsubscribe_ticks') {
        stopTicks(session);
        session.tickSymbol = null;
        return;
      }

"""
if "type === 'subscribe_ticks'" not in src:
    a = "      if (type && BOT_MESSAGE_TYPES.has(type)) {"
    once(src, a, "S8")
    src = src.replace(a, S8 + a)
    log.append("S8 handlers")

if src == orig:
    print("Nada a fazer: tudo já aplicado.")
else:
    shutil.copyfile(path, path + ".bak")
    open(path, "w", encoding="utf-8").write(src)
    print("OK. Passos aplicados:", ", ".join(log))
    print("Backup:", path + ".bak")
