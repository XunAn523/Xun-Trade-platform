import http from 'k6/http';
import { check } from 'k6';
import { jsonParams, seedPlanPath } from './common.js';

export const options = {
  scenarios: {
    plan_query: {
      executor: 'constant-arrival-rate', rate: Number(__ENV.PLAN_QUERY_QPS || 5000), timeUnit: '1s',
      duration: __ENV.DURATION || '30s', preAllocatedVUs: Number(__ENV.QUERY_PREALLOCATED_VUS || 600),
      maxVUs: Number(__ENV.QUERY_MAX_VUS || 3000),
    },
  },
  thresholds: {
    checks: ['rate==1'], dropped_iterations: ['count==0'], http_req_failed: ['rate==0'],
    'http_req_duration{workload:e2-query}': ['p(95)<10', 'p(99)<40'],
  },
};

export default function () {
  const response = http.get(seedPlanPath(), jsonParams('e2-query'));
  check(response, { 'E2 immutable plan query': (item) => item.status === 200 });
}
