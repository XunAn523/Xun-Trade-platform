import http from 'k6/http';
import exec from 'k6/execution';

export const baseUrls = (__ENV.BASE_URLS || __ENV.BASE_URL || 'http://127.0.0.1:18080')
  .split(',')
  .map((item) => item.trim().replace(/\/$/, ''))
  .filter((item) => item.length > 0);
export const baseUrl = baseUrls[0];
export const keyDistribution = __ENV.KEY_DISTRIBUTION || 'uniform';
const hotRatioPercent = Number(__ENV.HOT_KEY_RATIO_PERCENT || 80);
if (!['uniform', 'hot'].includes(keyDistribution)) {
  throw new Error('KEY_DISTRIBUTION must be uniform or hot');
}
if (!Number.isFinite(hotRatioPercent) || hotRatioPercent < 1 || hotRatioPercent > 100) {
  throw new Error('HOT_KEY_RATIO_PERCENT must be in 1..100');
}

export function distributedIndex(cardinality) {
  const ordinal = exec.vu.idInTest + exec.vu.iterationInScenario;
  if (keyDistribution === 'uniform' || cardinality === 1) return ordinal % cardinality;
  // Fixed 80/20-style pressure: one declared key/scope receives the hot share;
  // the residual remains deterministic and distributed across the other keys.
  return ordinal % 100 < hotRatioPercent ? 0 : 1 + (ordinal % (cardinality - 1));
}
export const jsonParams = (workload, responseType = undefined, headers = {}) => {
  const params = { headers: { 'Content-Type': 'application/json', ...headers }, tags: { workload } };
  if (responseType !== undefined) {
    params.responseType = responseType;
  }
  return params;
};

export function factPayload(scopeCount = 4) {
  const vu = exec.vu.idInTest;
  const iteration = exec.vu.iterationInScenario;
  const serial = `${exec.scenario.name}-${vu}-${iteration}`;
  const scope = distributedIndex(scopeCount);
  return JSON.stringify({
    scope_id: `bench-scope-${scope}`,
    execution_id: `k6-execution-${serial}`,
    canonical_client_order_ref: `k6-client-ref-${serial}`,
    kind: 'PRE_NATIVE_COMMITTED',
    order_state: 'PRE_NATIVE_COMMITTED',
    state_version: 1,
  });
}

export function endpointBase() {
  const index = (exec.vu.idInTest + exec.vu.iterationInScenario) % baseUrls.length;
  return baseUrls[index];
}

export function postFact(workload = 'fact') {
  return http.post(
    `${endpointBase()}/perf/v1/oms/facts`,
    factPayload(Number(__ENV.SCOPES || 4)),
    jsonParams(workload, 'none', { 'X-TradeCore-Perf-Ack': 'minimal' }),
  );
}

export function seedOrderPath() {
  const index = distributedIndex(Number(__ENV.SEED_ORDERS || 1024));
  return `${endpointBase()}/perf/v1/oms/orders/seed-${String(index).padStart(6, '0')}`;
}
