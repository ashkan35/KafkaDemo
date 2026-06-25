import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter, Rate } from 'k6/metrics';

const baseUrl = __ENV.BASE_URL || 'http://localhost:5192';
const warmupRps = Number(__ENV.WARMUP_RPS || 50);
const warmupDuration = __ENV.WARMUP_DURATION || '10s';
const rps = Number(__ENV.RPS || 2500);
const duration = __ENV.DURATION || '120s';
const preAllocatedVus = Number(__ENV.PRE_ALLOCATED_VUS || 500);
const maxVus = Number(__ENV.MAX_VUS || 10000);
const description10kb = 'A'.repeat(10 * 1024);
const savedEvents = new Counter('saved_events');
const saveFailures = new Rate('save_failures');

export const options = {
    insecureSkipTLSVerify: true,
    scenarios: {
        warmup50Rps: {
            executor: 'constant-arrival-rate',
            rate: warmupRps,
            timeUnit: '1s',
            duration: warmupDuration,
            preAllocatedVUs: preAllocatedVus,
            maxVUs: maxVus,
        },
        steady2500Rps: {
            executor: 'constant-arrival-rate',
            rate: rps,
            timeUnit: '1s',
            duration,
            startTime: warmupDuration,
            preAllocatedVUs: preAllocatedVus,
            maxVUs: maxVus,
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
        description: description10kb,
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
