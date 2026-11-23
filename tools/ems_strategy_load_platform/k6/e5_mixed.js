import http from 'k6/http';
import { check } from 'k6';
import { jsonParams, postPlan, seedPlanPath } from './common.js';

export const options = {
  scenarios: {
    plan_admission: {
      executor: 'constant-arrival-rate', exec: 'write', rate: Number(__ENV.PLAN_TPS || 2000), timeUnit: '1s',
      duration: __ENV.DURATION || '30s', preAllocatedVUs: 400, maxVUs: 2000,
    },
    plan_query: {
      executor: 'constant-arrival-rate', exec: 'read', rate: Number(__ENV.PLAN_QUERY_QPS || 5000), timeUnit: '1s',
      duration: __ENV.DURATION || '30s', preAllocatedVUs: 600, maxVUs: 3000,
    },
  },
  thresholds: {
    checks: ['rate==1'], dropped_iterations: ['count==0'], http_req_failed: ['rate==0'],
    'http_req_duration{workload:e5-plan}': ['p(95)<20', 'p(99)<60'],
    'http_req_duration{workload:e5-query}': ['p(95)<10', 'p(99)<40'],
  },
};

export function write() {
  const response = postPlan('e5', false, 'e5-plan');
  check(response, { 'E5 plan admitted': (item) => item.status === 202 });
}

export function read() {
  const response = http.get(seedPlanPath(), jsonParams('e5-query'));
  check(response, { 'E5 plan query': (item) => item.status === 200 });
}
