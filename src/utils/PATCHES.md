# Patches manuais

## src/services/trading.service.ts

### 1. getBalance — vazava uma subscrição por chamada (limite: 100)
```ts
async getBalance() {
  const response = await this.wsManager.send({ balance: 1 });   // SEM subscribe
  return response.balance;
}
```
(para stream use `subscribeBalance`, que já existe e é gerido)

### 2. getProposal — `subscribe: 0` é inválido (o schema só aceita 1)
```ts
async getProposal(request: ProposalRequest) {
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
    // pedido único: NÃO enviar subscribe (para stream use subscribeProposal)
  });
  const p = response.proposal;
  return {
    id: p.id, askPrice: p.ask_price, payout: p.payout, spot: p.spot,
    spotTime: p.spot_time, dateStart: p.date_start, dateExpiry: p.date_expiry,
    longcode: p.longcode,
  };
}
```

### 3. getTicksHistory — a resposta NÃO tem campo `ticks_history`
Segundo ticks-history.md a resposta traz `history` (ticks) ou `candles`.
O código atual devolvia sempre `undefined`.
```ts
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
```
(se algum controlador ler `.prices/.times` diretamente, passe a ler `.history.prices/.history.times`)

## src/routes/auth.routes.ts

### 1. /validate — só testava se a API estava viva, não o token
```ts
router.get('/validate', async (req, res, next) => {
  try {
    const token = AuthService.extractBearerToken(req.headers.authorization)
    try {
      await new DerivAPIService(token).getAccounts()
      return res.json({ success: true, valid: true })
    } catch (e) {
      if (e instanceof DerivTokenExpiredError) return res.json({ success: true, valid: false })
      throw e
    }
  } catch (error) { next(error) }
})
```
Import: `import { DerivAPIService, DerivTokenExpiredError } from '@services/deriv-api.service.js'`

### 2. /login — validar parâmetros vindos da query
```ts
const prompt: 'login' | 'registration' = req.query.prompt === 'registration' ? 'registration' : 'login'
const clean = (v: unknown) => (typeof v === 'string' && v.length <= 100 ? v : undefined)
const sidc = clean(req.query.sidc) ?? (isSignup ? config.affiliate.sidc : undefined)
// idem utm_campaign / utm_medium / utm_source
```

### 3. /callback — token no URL
`?token=...` fica no histórico do browser, logs de proxy e Referer. Recomendado
usar fragmento (nunca enviado a servidores):
```ts
return res.redirect(`${FRONTEND_URL}/callback#token=${encodeURIComponent(session.accessToken)}`)
```
Frontend: `new URLSearchParams(location.hash.slice(1)).get('token')` e depois
`history.replaceState(null, '', location.pathname)`. Só altere os dois lados juntos.

## src/bots/bot.types.ts

Os dados novos do adapter (dígito final correto, pip_size, payout) não chegam às
estratégias porque a interface não os declara. Substitua o bloco `DerivWsAdapter`
(os campos extra são opcionais, por isso as estratégias atuais continuam a compilar):

```ts
export interface DerivWsAdapter {
  buyContract(params: {
    symbol: string;
    contractType: string;
    stake: number;
    duration: number;
    durationUnit: string;
    currency: string;
    barrier?: string;
  }): Promise<{
    contractId: string;
    payout?: number;
    buyPrice?: number;
    balanceAfter?: number;
  }>;

  waitForContract(contractId: string): Promise<{
    profit: number;
    won: boolean;
    entryTick: number;
    exitTick: number;
    entrySpot?: string;   // preço tal como a Deriv o envia (mantém zeros finais)
    exitSpot?: string;
    exitDigit?: number;   // último dígito de exitSpot — USAR nas estratégias de dígitos
  }>;

  subscribeToTicks(
    symbol: string,
    onTick: (price: number, pipSize?: number) => void,
  ): { unsubscribe: () => void };

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
```

E no `BotConfig`, corrija o comentário (agora é perda LÍQUIDA):
`maxLoss?: number;  // para quando a perda líquida (−netPnL) ≥ valor`

### O que ainda tem de rever nas estratégias (não vi o código delas)
- Trocar o cálculo do último dígito por `result.exitDigit` (vem de `exit_spot`)
  e, nos ticks, usar `price.toFixed(pipSize)` em vez de `String(price)`
  (1.10 vira "1.1" e perde o zero final).
- Garantir que `stake` calculado por gale não gera dízimas (o adapter já arredonda,
  mas o `currentStake` mostrado no UI sai da estratégia).
- Em `even-odd-recovery` confirmar que a classe se chama `EvenOddRecoveryStrategy`.
