import http from 'k6/http';
import exec from 'k6/execution';

export const baseUrls = (__ENV.BASE_URLS || __ENV.BASE_URL || 'http://127.0.0.1:18180')
  .split(',')
  .map((item) => item.trim().replace(/\/$/, ''))
  .filter((item) => item.length > 0);
export const keyDistribution = __ENV.KEY_DISTRIBUTION || 'uniform';
const hotRatioPercent = Number(__ENV.HOT_KEY_RATIO_PERCENT || 80);
if (!['uniform', 'hot'].includes(keyDistribution)) {
  throw new Error('KEY_DISTRIBUTION must be uniform or hot');
}
if (!Number.isFinite(hotRatioPercent) || hotRatioPercent < 1 || hotRatioPercent > 100) {
  throw new Error('HOT_KEY_RATIO_PERCENT must be in 1..100');
}

export function distributedIndex(cardinality) {
  const ordinal = exec.vu.idInTest + exec.vu.iterationInScenario;
  if (keyDistribution === 'uniform' || cardinality === 1) return ordinal % cardinality;
  return ordinal % 100 < hotRatioPercent ? 0 : 1 + (ordinal % (cardinality - 1));
}

export function endpointBase() {
  return baseUrls[(exec.vu.idInTest + exec.vu.iterationInScenario) % baseUrls.length];
}

export function consumerEndpointBase() {
  return baseUrls[exec.vu.idInTest % baseUrls.length];
}

export function jsonParams(workload, responseType = undefined) {
  const params = { headers: { 'Content-Type': 'application/json' }, tags: { workload } };
  if (responseType !== undefined) params.responseType = responseType;
  return params;
}

export function planPayload(prefix, waitForChild = false) {
  const serial = `${prefix}-${exec.scenario.name}-${exec.vu.idInTest}-${exec.vu.iterationInScenario}`;
  return JSON.stringify({
    request_id: serial,
    scope_id: `perf-scope-${distributedIndex(Number(__ENV.SCOPES || 1))}`,
    wait_for_child: waitForChild,
  });
}

export function postPlan(prefix, waitForChild = false, workload = 'ems-plan') {
  return http.post(
    `${endpointBase()}/perf/v1/ems/plans`,
    planPayload(prefix, waitForChild),
    jsonParams(workload, 'none'),
  );
}

export function postSignal(prefix, waitForChild = false, workload = 'strategy-signal') {
  return http.post(
    `${endpointBase()}/perf/v1/strategy/signals`,
    planPayload(prefix, waitForChild),
    jsonParams(workload, 'none'),
  );
}

export function seedPlanPath() {
  const index = distributedIndex(Number(__ENV.SEED_PLANS || 2048));
  return `${endpointBase()}/perf/v1/ems/plans/ems-seed-${String(index).padStart(6, '0')}`;
}

export function postConsumerEvent(workload = 'strategy-event') {
  // A consumer has an independent, strictly contiguous checkpoint stream.
  // ``iterationInScenario`` is global to the scenario and may skip values
  // observed by one VU when k6 redistributes arrival-rate work.  The VU-local
  // counter is the only valid sequence source for this stable VU consumer.
  const sequence = exec.vu.iterationInInstance + 1;
  const consumerId = `perf-consumer-${exec.vu.idInTest}`;
  const body = JSON.stringify({
    consumer_id: consumerId,
    scope_id: `perf-scope-${distributedIndex(Number(__ENV.SCOPES || 1))}`,
    sequence,
    event_id: `synthetic-event-${consumerId}-${sequence}`,
  });
  return http.post(
    `${consumerEndpointBase()}/perf/v1/strategy/events`,
    body,
    jsonParams(workload, 'none'),
  );
}
