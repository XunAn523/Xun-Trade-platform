import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { jsonParams, postFact, seedOrderPath } from './common.js';

// A failed aggregate check alone cannot distinguish a projection admission
// rejection from a missing immutable-query generation.  Keep the response
// status split in the exported k6 summary so W3 failures are diagnosable
// without enabling expensive HTTP body logging.
const responseStatus = new Counter('oms_w3_response_status_total');

export const options = {
  scenarios: {
    fact_ingest: {
      executor: 'constant-arrival-rate',
      exec: 'write',
      rate: Number(__ENV.FACT_TPS || 2000),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: Number(__ENV.FACT_PREALLOCATED_VUS || 400),
      maxVUs: Number(__ENV.FACT_MAX_VUS || 2000),
    },
    snapshot_query: {
      executor: 'constant-arrival-rate',
      exec: 'read',
      rate: Number(__ENV.SNAPSHOT_QPS || 5000),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: Number(__ENV.SNAPSHOT_PREALLOCATED_VUS || 600),
      maxVUs: Number(__ENV.SNAPSHOT_MAX_VUS || 3000),
    },
  },
  thresholds: {
    checks: ['rate==1'],
    dropped_iterations: ['count==0'],
    http_req_failed: ['rate==0'],
    'http_req_duration{workload:mixed-fact}': ['p(95)<75', 'p(99)<150'],
    'http_req_duration{workload:mixed-snapshot}': ['p(95)<40', 'p(99)<75'],
  },
};

export function write() {
  const response = postFact('mixed-fact');
  responseStatus.add(1, { operation: 'fact', status: String(response.status) });
  check(response, { 'W3 fact projected': (item) => item.status === 201 });
}

export function read() {
  const response = http.get(seedOrderPath(), jsonParams('mixed-snapshot'));
  responseStatus.add(1, { operation: 'snapshot', status: String(response.status) });
  check(response, { 'W3 snapshot found': (item) => item.status === 200 });
}
