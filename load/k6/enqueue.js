import http from "k6/http";
import { check, sleep, group } from "k6";
import { Rate, Trend } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://localhost:8080";
const errorRate = new Rate("errors");
const enqueueDuration = new Trend("enqueue_duration", true);

export const options = {
  scenarios: {
    smoke: {
      executor: "constant-vus",
      vus: 2,
      duration: "30s",
      tags: { scenario: "smoke" },
    },
    sustained: {
      executor: "ramping-vus",
      startTime: "30s",
      startVUs: 0,
      stages: [
        { duration: "30s", target: 10 },
        { duration: "1m", target: 20 },
        { duration: "30s", target: 0 },
      ],
      tags: { scenario: "sustained" },
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.05"],
    http_req_duration: ["p(95)<500"],
    errors: ["rate<0.05"],
    enqueue_duration: ["p(95)<400"],
  },
};

const PRIORITIES = ["high", "default", "low"];

export function setup() {
  const ready = http.get(`${BASE}/ready`);
  if (ready.status !== 200) {
    throw new Error(`service not ready: ${ready.status} ${ready.body}`);
  }
  return { base: BASE };
}

export default function (data) {
  const priority = PRIORITIES[Math.floor(Math.random() * PRIORITIES.length)];
  const payload = JSON.stringify({
    type: "print",
    priority: priority,
    payload: { name: `k6-${__VU}-${__ITER}`, ts: Date.now() },
    idempotency_key: `k6-${__VU}-${__ITER}-${priority}`,
  });

  group("enqueue", () => {
    const res = http.post(`${data.base}/job`, payload, {
      headers: {
        "Content-Type": "application/json",
        "X-Request-ID": `k6-${__VU}-${__ITER}`,
      },
      tags: { name: "POST /job" },
    });

    enqueueDuration.add(res.timings.duration);

    const ok = check(res, {
      "status is 2xx": (r) => r.status >= 200 && r.status < 300,
      "has job_id": (r) => {
        try {
          return JSON.parse(r.body).job_id !== undefined;
        } catch {
          return false;
        }
      },
    });
    errorRate.add(!ok);

    if (ok) {
      const jobId = JSON.parse(res.body).job_id;
      const st = http.get(`${data.base}/jobs/${jobId}`, {
        tags: { name: "GET /jobs/{id}" },
      });
      check(st, { "get job 200": (r) => r.status === 200 });
    }
  });

  if (__ITER % 20 === 0) {
    const stats = http.get(`${data.base}/stats`, { tags: { name: "GET /stats" } });
    check(stats, { "stats 200": (r) => r.status === 200 });
  }

  sleep(0.1);
}