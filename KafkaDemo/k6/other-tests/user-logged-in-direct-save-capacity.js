import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter, Rate } from 'k6/metrics';

const baseUrl = __ENV.BASE_URL || 'https://localhost:7189';
const startRps = Number(__ENV.START_RPS || 100);
const targetRps = Number(__ENV.TARGET_RPS || 5000);
const preAllocatedVus = Number(__ENV.PRE_ALLOCATED_VUS || 200);
const maxVus = Number(__ENV.MAX_VUS || 2000);

const savedEvents = new Counter('saved_events');
const saveFailures = new Rate('save_failures');

export const options = {
    insecureSkipTLSVerify: true,
    scenarios: {
        findThroughputLimit: {
            executor: 'ramping-arrival-rate',
            startRate: startRps,
            timeUnit: '1s',
            preAllocatedVUs: preAllocatedVus,
            maxVUs: maxVus,
            stages: [
                { duration: '1m', target: startRps },
                { duration: '4m', target: targetRps },
                { duration: '1m', target: targetRps },
                { duration: '30s', target: 0 },
            ],
        },
    },
    thresholds: {
        saved_events: ['count>0'],
        save_failures: ['rate<0.01'],
        http_req_failed: ['rate<0.01'],
        http_req_duration: ['p(95)<1000'],
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
