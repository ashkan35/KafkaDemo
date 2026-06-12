import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter, Rate } from 'k6/metrics';

const baseUrl = __ENV.BASE_URL || 'https://localhost:7189';
const startRps = Number(__ENV.START_RPS || 1000);
const targetRps = Number(__ENV.TARGET_RPS || 50000);
const preAllocatedVus = Number(__ENV.PRE_ALLOCATED_VUS || 1000);
const maxVus = Number(__ENV.MAX_VUS || 20000);

const savedEvents = new Counter('saved_events');
const saveFailures = new Rate('save_failures');

export const options = {
    insecureSkipTLSVerify: true,
    discardResponseBodies: true,
    scenarios: {
        findBreakingPoint: {
            executor: 'ramping-arrival-rate',
            startRate: startRps,
            timeUnit: '1s',
            preAllocatedVUs: preAllocatedVus,
            maxVUs: maxVus,
            stages: [
                { duration: '1m', target: startRps },
                { duration: '10m', target: targetRps },
                { duration: '2m', target: targetRps },
                { duration: '1m', target: 0 },
            ],
        },
    },
    thresholds: {
        saved_events: ['count>0'],
        save_failures: ['rate<0.20'],
        http_req_failed: ['rate<0.20'],
    },
};

export default function () {
    const iteration = exec.scenario.iterationInTest + 1;
    const userId = (iteration % 100000) + 1;

    const payload = JSON.stringify({
        userId: userId.toString(),
        userName: `Ashkan${iteration}`,
        loggedInAt: currentTimestampWithoutTimeZone(),
    });

    const response = http.post(`${baseUrl}/UserLoggedInDirectSave`, payload, {
        headers: {
            'Content-Type': 'application/json',
        },
        timeout: __ENV.REQUEST_TIMEOUT || '10s',
    });

    const saved = check(response, {
        'saved successfully': (res) => res.status === 204,
    });

    savedEvents.add(saved ? 1 : 0);
    saveFailures.add(!saved);
}

function currentTimestampWithoutTimeZone() {
    return new Date().toISOString().replace('Z', '');
}
