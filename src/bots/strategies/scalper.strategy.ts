// ── 1) Campos novos na classe ─────────────────────────────
private lastPrice?: number;

// ── 2) Params: acrescenta no withDefaults ─────────────────
const params = withDefaults<ScalperParams>({
  predictDigit: 2,
  gale: 2,
  lossesToStartGale: 0,
  maxMartingaleSteps: 10,
  pauseLossLimit: 20,
  pauseSeconds: 10,
  decimalPlaces: 2,
  entryFilter: true,   // false = entra em todos os ticks (mais rápido)
}, config.strategyParams);
// (adiciona `entryFilter?: boolean` ao ScalperParams em bot.types.ts)

// ── 3) onSymbolChanged: limpa o cache ─────────────────────
protected async onSymbolChanged(): Promise<void> {
  this.pipDecimals = undefined;
  this.lastPrice = undefined;
  // ... resto igual
}

// ── 4) onTick: guarda o preço e delega para tryTrade ──────
private onTick(price: number, pipSize?: number): void {
  if (pipSize !== undefined && this.pipDecimals === undefined) {
    this.pipDecimals = decimalsFromPip(pipSize, this.params.decimalPlaces ?? 2);
  }
  this.lastPrice = price;
  this.tryTrade();
}

private tryTrade(): void {
  if (!this.active || this.pending || this.paused || this.coolingDown) return;
  if (this.lastPrice === undefined) return;

  if (this.params.entryFilter === false ||
      lastDigit(this.lastPrice, this.decimals) <= this.params.predictDigit) {
    void this.placeTrade();
  }
}

// ── 5) placeTrade: 1 tick fixo ────────────────────────────
private async placeTrade(): Promise<void> {
  this.pending = true;
  const { symbol, currency } = this.state.config;   // sem duration/durationUnit
  const { predictDigit } = this.params;
  const stake = this.state.stats.currentStake;

  try {
    const entryTime = new Date();
    const { contractId } = await this.derivWs.buyContract({
      symbol,
      contractType: 'DIGITOVER',
      stake,
      duration: 1,
      durationUnit: 't',
      currency,
      barrier: String(predictDigit),
    });
    // ... resto igual (emit, waitForContract, recordTradeResult, etc.)
  } catch (err) {
    // ... igual
  } finally {
    this.pending = false;
    // reentrada imediata com o último tick, sem esperar outro
    if (this.active && !this.paused) setImmediate(() => this.tryTrade());
  }
}
