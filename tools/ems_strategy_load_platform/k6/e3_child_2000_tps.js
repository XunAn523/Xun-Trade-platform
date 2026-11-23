import { check } from 'k6';
import { postPlan } from './common.js';

export const options = {
  scenarios: {
    child_preparation: {
      executor: 'constant-arrival-rate', rate: Number(__ENV.CHILD_TPS || 2000), timeUnit: '1s',
      duration: __ENV.DURATION || '30s', preAllocatedVUs: Number(__ENV.PLAN_PREALLOCATED_VUS || 400),
      maxVUs: Number(__ENV.PLAN_MAX_VUS || 2000),
    },
  },
  thresholds: {
    checks: ['rate==1'], dropped_iterations: ['count==0'], http_req_failed: ['rate==0'],
    'http_req_duration{workload:e3-child}': ['p(95)<20', 'p(99)<60'],
  },
};

export default function () {
  const response = postPlan('e3', true, 'e3-child');
  check(response, { 'E3 child instruction ready': (item) => item.status === 201 });
}
