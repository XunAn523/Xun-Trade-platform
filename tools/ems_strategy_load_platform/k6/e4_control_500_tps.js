import http from 'k6/http';
import { check } from 'k6';
import { endpointBase, jsonParams } from './common.js';

export const options = {
  scenarios: {
    ems_control: {
      executor: 'constant-arrival-rate', rate: Number(__ENV.CONTROL_TPS || 500), timeUnit: '1s',
      duration: __ENV.DURATION || '30s', preAllocatedVUs: Number(__ENV.CONTROL_PREALLOCATED_VUS || 150),
      maxVUs: Number(__ENV.CONTROL_MAX_VUS || 1000),
    },
  },
  thresholds: {
    checks: ['rate==1'], dropped_iterations: ['count==0'], http_req_failed: ['rate==0'],
    'http_req_duration{workload:e4-control}': ['p(95)<20', 'p(99)<60'],
  },
};

export default function () {
  const response = http.post(
    `${endpointBase()}/perf/v1/admin/ems/reconcile`,
    '{}',
    {
      ...jsonParams('e4-control', 'none'),
      headers: { 'Content-Type': 'application/json', 'X-TradeCore-Perf-Fault-Key': __ENV.FAULT_KEY },
    },
  );
  check(response, { 'E4 reconcile control applied': (item) => item.status === 200 });
}
