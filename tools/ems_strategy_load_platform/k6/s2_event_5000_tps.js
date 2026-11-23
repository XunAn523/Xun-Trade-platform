import { check } from 'k6';
import { postConsumerEvent } from './common.js';

export const options = {
  scenarios: {
    event_consume: {
      executor: 'constant-arrival-rate', rate: Number(__ENV.EVENT_TPS || 5000), timeUnit: '1s',
      duration: __ENV.DURATION || '30s', preAllocatedVUs: Number(__ENV.EVENT_PREALLOCATED_VUS || 800),
      maxVUs: Number(__ENV.EVENT_MAX_VUS || 4000),
    },
  },
  thresholds: {
    checks: ['rate==1'], dropped_iterations: ['count==0'], http_req_failed: ['rate==0'],
    'http_req_duration{workload:s2-event}': ['p(95)<10', 'p(99)<40'],
  },
};

export default function () {
  const response = postConsumerEvent('s2-event');
  check(response, { 'S2 event checkpointed': (item) => item.status === 201 });
}
