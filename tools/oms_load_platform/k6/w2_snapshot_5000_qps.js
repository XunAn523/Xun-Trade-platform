import http from 'k6/http';
import { check } from 'k6';
import { jsonParams, seedOrderPath } from './common.js';

const snapshotQps = Number(__ENV.SNAPSHOT_QPS || 5000);
export const options = {
  scenarios: {
    snapshot_query: {
      executor: 'constant-arrival-rate',
      rate: snapshotQps,
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: Number(__ENV.SNAPSHOT_PREALLOCATED_VUS || 500),
      maxVUs: Number(__ENV.SNAPSHOT_MAX_VUS || 3000),
    },
  },
  thresholds: {
    checks: ['rate==1'],
    dropped_iterations: ['count==0'],
    http_req_failed: ['rate==0'],
    'http_req_duration{workload:snapshot}': ['p(95)<25', 'p(99)<50'],
  },
};

export default function () {
  const response = http.get(seedOrderPath(), jsonParams('snapshot'));
  check(response, { 'W2 snapshot order found': (item) => item.status === 200 && item.json('state') === 'PRE_NATIVE_COMMITTED' });
}
