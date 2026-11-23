import http from 'k6/http';
import { check } from 'k6';
import { endpointBase, jsonParams, postConsumerEvent, postSignal } from './common.js';

export const options = {
  scenarios: {
    signal_admission: {
      executor: 'constant-arrival-rate', exec: 'signal', rate: Number(__ENV.SIGNAL_TPS || 2000), timeUnit: '1s',
      duration: __ENV.DURATION || '30s', preAllocatedVUs: 400, maxVUs: 2000,
    },
    event_consume: {
      executor: 'constant-arrival-rate', exec: 'event', rate: Number(__ENV.EVENT_TPS || 5000), timeUnit: '1s',
      duration: __ENV.DURATION || '30s', preAllocatedVUs: 800, maxVUs: 4000,
    },
    snapshot_query: {
      executor: 'constant-arrival-rate', exec: 'snapshot', rate: Number(__ENV.SNAPSHOT_QPS || 5000), timeUnit: '1s',
      duration: __ENV.DURATION || '30s', preAllocatedVUs: 600, maxVUs: 3000,
    },
  },
  thresholds: {
    checks: ['rate==1'], dropped_iterations: ['count==0'], http_req_failed: ['rate==0'],
    'http_req_duration{workload:s5-signal}': ['p(95)<20', 'p(99)<60'],
    'http_req_duration{workload:s5-event}': ['p(95)<10', 'p(99)<40'],
    'http_req_duration{workload:s5-snapshot}': ['p(95)<10', 'p(99)<40'],
  },
};

export function signal() {
  const response = postSignal('s5', false, 's5-signal');
  check(response, { 'S5 signal admitted': (item) => item.status === 202 });
}

export function event() {
  const response = postConsumerEvent('s5-event');
  check(response, { 'S5 event checkpointed': (item) => item.status === 201 });
}

export function snapshot() {
  const response = http.get(`${endpointBase()}/perf/v1/strategy/snapshot`, jsonParams('s5-snapshot'));
  check(response, { 'S5 snapshot ready': (item) => item.status === 200 });
}
