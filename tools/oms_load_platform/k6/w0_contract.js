import http from 'k6/http';
import { check } from 'k6';
import { baseUrl, jsonParams } from './common.js';

export const options = {
  vus: 1,
  iterations: 1,
  thresholds: { checks: ['rate==1'], http_req_failed: ['rate==0'] },
};

export default function () {
  const reset = http.post(`${baseUrl}/perf/v1/metrics/reset`, null, jsonParams('w0-reset'));
  const health = http.get(`${baseUrl}/perf/v1/oms/health`, jsonParams('w0-health'));
  const snapshot = http.get(`${baseUrl}/perf/v1/oms/snapshot?require_fresh=true`, jsonParams('w0-snapshot'));
  const hdr = http.get(`${baseUrl}/perf/v1/metrics/hdr`, jsonParams('w0-hdr'));
  check(reset, { 'W0 metric reset': (response) => response.status === 204 });
  check(health, { 'W0 health READY': (response) => response.status === 200 && response.json('status') === 'READY' });
  check(snapshot, { 'W0 snapshot fresh': (response) => response.status === 200 && response.json('authoritative') === true });
  check(hdr, { 'W0 HDR schema': (response) => response.status === 200 && response.json('format') === 'HDRHistogramV1' });
}
