import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter, Rate } from 'k6/metrics';

const baseUrl = __ENV.BASE_URL || 'http://localhost:5192';
const warmupRps = Number(__ENV.WARMUP_RPS || 50);
const warmupDuration = __ENV.WARMUP_DURATION || '10s';
const bridgeRps = Number(__ENV.BRIDGE_RPS || 7000);
const bridgeDuration = __ENV.BRIDGE_DURATION || '10s';
const rps = Number(__ENV.RPS || 10000);
const duration = __ENV.DURATION || '120s';
const preAllocatedVus = Number(__ENV.PRE_ALLOCATED_VUS || 2000);
const maxVus = Number(__ENV.MAX_VUS || 10000);
const description10kb = 'A'.repeat(10 * 1024);
const publishedEvents = new Counter('published_events');
const publishFailures = new Rate('publish_failures');

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
        bridge7000Rps: {
            executor: 'constant-arrival-rate',
            rate: bridgeRps,
            timeUnit: '1s',
            duration: bridgeDuration,
            startTime: warmupDuration,
            preAllocatedVUs: preAllocatedVus,
            maxVUs: maxVus,
        },
        steady10000Rps: {
            executor: 'constant-arrival-rate',
            rate: rps,
            timeUnit: '1s',
            duration,
            startTime: `${parseInt(warmupDuration) + parseInt(bridgeDuration)}s`,
            preAllocatedVUs: preAllocatedVus,
            maxVUs: maxVus,
        },
    },
    thresholds: {
        published_events: ['count>0'],
        publish_failures: ['rate<0.01'],
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

    const response = http.post(`${baseUrl}/UserLoggedIn`, payload, {
        headers: {
            'Content-Type': 'application/json',
        },
        timeout: __ENV.REQUEST_TIMEOUT || '10s',
    });

    const published = check(response, {
        'published successfully': (res) => res.status === 202,
    });

    if (!published) {
        console.error(JSON.stringify({
            status: response.status,
            body: response.body,
        }));
    }

    publishedEvents.add(published ? 1 : 0);
    publishFailures.add(!published);
}

function currentTimestampWithoutTimeZone() {
    return new Date().toISOString().replace('Z', '');
}
