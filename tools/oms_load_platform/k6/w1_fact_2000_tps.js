import { check } from 'k6';
import { postFact } from './common.js';

const factTps = Number(__ENV.FACT_TPS || 2000);
export const options = {
  scenarios: {
    fact_ingest: {
      executor: 'constant-arrival-rate',
      rate: factTps,
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: Number(__ENV.FACT_PREALLOCATED_VUS || 400),
      maxVUs: Number(__ENV.FACT_MAX_VUS || 2000),
    },
  },
  thresholds: {
    checks: ['rate==1'],
    dropped_iterations: ['count==0'],
    http_req_failed: ['rate==0'],
    'http_req_duration{workload:fact}': ['p(95)<50', 'p(99)<100'],
  },
};

export default function () {
  const response = postFact('fact');
  check(response, { 'W1 fact projected': (item) => item.status === 201 });
}
