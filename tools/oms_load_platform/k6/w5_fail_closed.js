import http from 'k6/http';
import { check } from 'k6';
import { baseUrl, jsonParams, postFact } from './common.js';

const faultKey = __ENV.FAULT_KEY;
export const options = {
  scenarios: {
    fail_closed: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.W5_RATE || 200),
      timeUnit: '1s',
      duration: __ENV.DURATION || '10s',
      preAllocatedVUs: 32,
      maxVUs: 256,
    },
  },
  // 503 is the expected fail-closed result while the fault is active.
  thresholds: { checks: ['rate==1'] },
};

export function setup() {
  if (!faultKey) {
    throw new Error('W5 requires FAULT_KEY and a server started with faults enabled');
  }
  const response = http.post(
    `${baseUrl}/perf/v1/admin/fault/pause-projector`,
    JSON.stringify({ scope_id: 'bench-scope-0' }),
    { headers: { 'Content-Type': 'application/json', 'X-TradeCore-Perf-Fault-Key': faultKey } },
  );
  if (response.status !== 200) {
    throw new Error(`cannot pause projector: HTTP ${response.status}`);
  }
}

export default function () {
  const fact = postFact('w5-fail-closed');
  const health = http.get(`${baseUrl}/perf/v1/oms/health`, jsonParams('w5-health'));
  const snapshot = http.get(`${baseUrl}/perf/v1/oms/snapshot?require_fresh=true`, jsonParams('w5-snapshot'));
  check(fact, { 'W5 paused fact rejected': (item) => item.status === 503 });
  check(health, { 'W5 health reports stale': (item) => item.status === 200 && item.json('status') === 'STALE' });
  check(snapshot, { 'W5 fresh snapshot rejected': (item) => item.status === 503 });
}

export function teardown() {
  http.post(
    `${baseUrl}/perf/v1/admin/fault/resume-projector`,
    JSON.stringify({ scope_id: 'bench-scope-0' }),
    { headers: { 'Content-Type': 'application/json', 'X-TradeCore-Perf-Fault-Key': faultKey } },
  );
}
