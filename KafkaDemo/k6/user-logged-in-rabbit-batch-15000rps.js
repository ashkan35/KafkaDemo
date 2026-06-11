import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter, Rate } from 'k6/metrics';

const baseUrl = __ENV.BASE_URL || 'https://localhost:7189';
const rps = Number(__ENV.RPS || 15000);
const duration = __ENV.DURATION || '2m';
const preAllocatedVus = Number(__ENV.PRE_ALLOCATED_VUS || 3000);
const maxVus = Number(__ENV.MAX_VUS || 15000);
const description10kb = 'A'.repeat(10 * 1024);
const publishedEvents = new Counter('published_events');
const publishFailures = new Rate('publish_failures');

export const options = {
    insecureSkipTLSVerify: true,
    scenarios: {
        steadyRabbitBatch15000Rps: {
            executor: 'constant-arrival-rate',
            rate: rps,
            timeUnit: '1s',
            duration,
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

    const response = http.post(`${baseUrl}/UserLoggedInRabbitBatch`, payload, {
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
