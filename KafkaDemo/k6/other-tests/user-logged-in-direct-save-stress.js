import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const baseUrl = __ENV.BASE_URL || 'http://localhost:5192';

export const options = {
    scenarios: {
        userLoggedInDirectSave: {
            executor: 'shared-iterations',
            vus: Number(__ENV.VUS || 50),
            iterations: 1000000,
            maxDuration: __ENV.MAX_DURATION || '5m',
        },
    },
    thresholds: {
        http_req_failed: ['rate<0.01'],
    },
};

export default function () {
    const iteration = exec.scenario.iterationInTest + 1;
    const userId = Math.floor(Math.random() * 100000) + 1;

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

    check(response, {
        'saved successfully': (res) => res.status === 204,
    });
}

function currentTimestampWithoutTimeZone() {
    return new Date().toISOString().replace('Z', '');
}
