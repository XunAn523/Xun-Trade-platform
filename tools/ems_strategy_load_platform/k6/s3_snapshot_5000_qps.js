import http from 'k6/http';
import { check } from 'k6';
import { endpointBase, jsonParams } from './common.js';

export const options = {
  scenarios: {
    snapshot_query: {
      executor: 'constant-arrival-rate', rate: Number(__ENV.SNAPSHOT_QPS || 5000), timeUnit: '1s',
      duration: __ENV.DURATION || '30s', preAllocatedVUs: Number(__ENV.QUERY_PREALLOCATED_VUS || 600),
      maxVUs: Number(__ENV.QUERY_MAX_VUS || 3000),
    },
  },
  thresholds: {
    checks: ['rate==1'], dropped_iterations: ['count==0'], http_req_failed: ['rate==0'],
    'http_req_duration{workload:s3-snapshot}': ['p(95)<10', 'p(99)<40'],
  },
};

export default function () {
  const response = http.get(`${endpointBase()}/perf/v1/strategy/snapshot`, jsonParams('s3-snapshot'));
  check(response, { 'S3 immutable strategy snapshot': (item) => item.status === 200 });
}
