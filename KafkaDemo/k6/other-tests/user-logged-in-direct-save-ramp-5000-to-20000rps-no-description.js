import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter, Rate } from 'k6/metrics';

const baseUrl = __ENV.BASE_URL || 'http://localhost:5192';
const preAllocatedVus = Number(__ENV.PRE_ALLOCATED_VUS || 3000);
const maxVus = Number(__ENV.MAX_VUS || 20000);
const savedEvents = new Counter('saved_events');
const saveFailures = new Rate('save_failures');

export const options = {
    insecureSkipTLSVerify: true,
    scenarios: {
        rampDirectSaveNoDescription: {
            executor: 'ramping-arrival-rate',
            startRate: 5000,
            timeUnit: '1s',
            preAllocatedVUs: preAllocatedVus,
            maxVUs: maxVus,
            stages: [
                { duration: '30s', target: 5000 },
                { duration: '30s', target: 10000 },
                { duration: '30s', target: 15000 },
                { duration: '30s', target: 20000 },
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
        timeout: __ENV.REQUEST_TIMEOUT || '10s',
    });

    const saved = check(response, {
        'saved successfully': (res) => res.status === 200 && Boolean(res.json('id')),
    });

    if (!saved) {
        console.error(JSON.stringify({
            status: response.status,
            body: response.body,
        }));
    }

    savedEvents.add(saved ? 1 : 0);
    saveFailures.add(!saved);
}

function currentTimestampWithoutTimeZone() {
    return new Date().toISOString().replace('Z', '');
}
