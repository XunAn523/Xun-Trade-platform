import http from 'k6/http';
import { check } from 'k6';
import { endpointBase, jsonParams, postConsumerEvent, postPlan, postSignal } from './common.js';

// OMS-W7-EMS-STR: S1/E1 share the 2,000 TPS strategy signal stream.  E3 is
// deliberately separate child preparation; S2 is a deterministic contract
// feed and is never represented as an EMT callback.
export const options = {
  scenarios: {
    strategy_signal: {
      executor: 'constant-arrival-rate', exec: 'signal', rate: Number(__ENV.SIGNAL_TPS || 2000), timeUnit: '1s',
      duration: __ENV.DURATION || '60s', preAllocatedVUs: 400, maxVUs: 2000,
    },
    child_preparation: {
      executor: 'constant-arrival-rate', exec: 'child', rate: Number(__ENV.CHILD_TPS || 2000), timeUnit: '1s',
      duration: __ENV.DURATION || '60s', preAllocatedVUs: 400, maxVUs: 2000,
    },
    event_consume: {
      executor: 'constant-arrival-rate', exec: 'event', rate: Number(__ENV.EVENT_TPS || 5000), timeUnit: '1s',
      duration: __ENV.DURATION || '60s', preAllocatedVUs: 800, maxVUs: 4000,
    },
    immutable_query: {
      executor: 'constant-arrival-rate', exec: 'query', rate: Number(__ENV.SNAPSHOT_QPS || 5000), timeUnit: '1s',
      duration: __ENV.DURATION || '60s', preAllocatedVUs: 600, maxVUs: 3000,
    },
  },
  thresholds: {
    checks: ['rate==1'], dropped_iterations: ['count==0'], http_req_failed: ['rate==0'],
    'http_req_duration{workload:w7-signal}': ['p(95)<20', 'p(99)<60'],
    'http_req_duration{workload:w7-child}': ['p(95)<20', 'p(99)<60'],
    'http_req_duration{workload:w7-event}': ['p(95)<10', 'p(99)<40'],
    'http_req_duration{workload:w7-query}': ['p(95)<10', 'p(99)<40'],
  },
};

export function signal() {
  const response = postSignal('w7-signal', false, 'w7-signal');
  check(response, { 'W7 S1/E1 admitted': (item) => item.status === 202 });
}

export function child() {
  const response = postPlan('w7-child', true, 'w7-child');
  check(response, { 'W7 E3 child ready': (item) => item.status === 201 });
}

export function event() {
  const response = postConsumerEvent('w7-event');
  check(response, { 'W7 S2 checkpointed': (item) => item.status === 201 });
}

export function query() {
  const response = http.get(`${endpointBase()}/perf/v1/strategy/snapshot`, jsonParams('w7-query'));
  check(response, { 'W7 E2/S3 immutable query': (item) => item.status === 200 });
}
