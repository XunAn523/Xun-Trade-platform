import http from 'k6/http';
import { check } from 'k6';
import { endpointBase, jsonParams } from './common.js';

export const options = {
  scenarios: {
    watermark_health: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.HEALTH_QPS || 250),
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: 32,
      maxVUs: 256,
    },
  },
  thresholds: {
    checks: ['rate==1'],
    dropped_iterations: ['count==0'],
    http_req_failed: ['rate==0'],
    'http_req_duration{workload:health}': ['p(95)<25', 'p(99)<50'],
  },
};

export default function () {
  const response = http.get(`${endpointBase()}/perf/v1/oms/health`, jsonParams('health'));
  check(response, {
    'W4 health ready': (item) => item.status === 200 && item.json('status') === 'READY',
    'W4 four scope watermarks': (item) => Object.keys(item.json('watermarks')).length === Number(__ENV.SCOPES || 4),
  });
}
