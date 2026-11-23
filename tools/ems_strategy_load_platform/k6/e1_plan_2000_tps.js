import { check } from 'k6';
import { postPlan } from './common.js';

export const options = {
  scenarios: {
    plan_admission: {
      executor: 'constant-arrival-rate', rate: Number(__ENV.PLAN_TPS || 2000), timeUnit: '1s',
      duration: __ENV.DURATION || '30s', preAllocatedVUs: Number(__ENV.PLAN_PREALLOCATED_VUS || 400),
      maxVUs: Number(__ENV.PLAN_MAX_VUS || 2000),
    },
  },
  thresholds: {
    checks: ['rate==1'], dropped_iterations: ['count==0'], http_req_failed: ['rate==0'],
    'http_req_duration{workload:e1-plan}': ['p(95)<20', 'p(99)<60'],
  },
};

export default function () {
  const response = postPlan('e1', false, 'e1-plan');
  check(response, { 'E1 plan admitted': (item) => item.status === 202 });
}
