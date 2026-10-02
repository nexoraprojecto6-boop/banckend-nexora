// ============================================================
// NEXORA FOREX — Alocador único de req_id
// ============================================================
// A Deriv exige req_id inteiro positivo. Antes existiam 3 contadores
// independentes (manager.ts a partir de 1, server.ts a partir de 1000,
// adapter a partir de 100000). Quando os intervalos se cruzavam, uma
// resposta podia resolver a Promise errada. Agora há UM contador.

const MIN_REQ_ID = 1_000_000; // ids < 1.000.000 ficam reservados ao frontend/server.ts
const MAX_REQ_ID = 2_000_000_000;
let counter = MIN_REQ_ID - 1;

export function nextReqId(): number {
  counter = counter >= MAX_REQ_ID ? MIN_REQ_ID : counter + 1;
  return counter;
}
