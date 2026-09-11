import http from "k6/http";
import { check, sleep, group } from "k6";
import { Rate, Trend } from "k6/metrics";

const BASE = (__ENV.BASE_URL || "http://127.0.0.1:8080").replace(/\/$/, "");
const errorRate = new Rate("errors");
const enqueueDuration = new Trend("enqueue_duration", true);

/**
 * Profiles:
 *   PROFILE=smoke      → CI / quick check
 *   PROFILE=sustained  → moderate load (default)
 *   PROFILE=stress     → find the cliff on this host
 *
 * Example:
 *   k6 run -e BASE_URL=http://127.0.0.1:8080 -e PROFILE=stress load/k6/enqueue.js
 */
const PROFILE = (__ENV.PROFILE || "sustained").toLowerCase();

function buildOptions() {
  const thresholds = {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<100", "p(99)<250"],
    errors: ["rate<0.01"],
    enqueue_duration: ["p(95)<80"],
    "http_req_failed{name:POST /jobs}": ["rate<0.01"],
  };

  if (PROFILE === "smoke") {
    return {
      scenarios: {
        smoke: {
          executor: "constant-vus",
          vus: 5,
          duration: "30s",
          gracefulStop: "10s",
          tags: { scenario: "smoke" },
        },
      },
      thresholds: {
        http_req_failed: ["rate<0.05"],
        http_req_duration: ["p(95)<500"],
        errors: ["rate<0.05"],
        enqueue_duration: ["p(95)<400"],
      },
    };
  }

  if (PROFILE === "stress") {
    return {
      scenarios: {
        stress: {
          executor: "ramping-arrival-rate",
          startRate: 50,
          timeUnit: "1s",
          preAllocatedVUs: 50,
          maxVUs: 300,
          stages: [
            { duration: "30s", target: 100 },
            { duration: "1m", target: 300 },
            { duration: "1m", target: 500 },
            { duration: "1m", target: 1000 }, // push until SLO breaks
            { duration: "30s", target: 0 },
          ],
          gracefulStop: "30s",
          tags: { scenario: "stress" },
        },
      },
      thresholds,
    };
  }

  // sustained (default): target RPS, not vanity VUs
  return {
    scenarios: {
      smoke: {
        executor: "constant-vus",
        vus: 5,
        duration: "20s",
        gracefulStop: "10s",
        tags: { scenario: "smoke" },
      },
      sustained: {
        executor: "ramping-arrival-rate",
        startTime: "20s",
        startRate: 20,
        timeUnit: "1s",
        preAllocatedVUs: 30,
        maxVUs: 150,
        stages: [
          { duration: "30s", target: 50 },
          { duration: "1m", target: 150 },
          { duration: "1m", target: 300 },
          { duration: "1m", target: 300 },
          { duration: "30s", target: 0 },
        ],
        gracefulStop: "30s",
        tags: { scenario: "sustained" },
      },
    },
    thresholds,
  };
}

export const options = buildOptions();

const PRIORITIES = ["high", "default", "low"];

export function setup() {
  const ready = http.get(`${BASE}/ready`);
  console.log(`setup /ready status=${ready.status}`);
  if (ready.status !== 200) {
    throw new Error(
      `service not ready at ${BASE}/ready status=${ready.status} body=${ready.body}`
    );
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
    // Prefer /jobs; /job kept as fallback if you still only register legacy path
    const res = http.post(`${data.base}/jobs`, payload, {
      headers: {
        "Content-Type": "application/json",
        "X-Request-ID": `k6-${__VU}-${__ITER}`,
      },
      tags: { name: "POST /jobs" },
      timeout: "10s",
    });

    enqueueDuration.add(res.timings.duration);

    if (res.status === 0) {
      console.error(`connection failed: ${res.error}`);
    } else if (res.status >= 400) {
      console.error(`POST /jobs status=${res.status} body=${String(res.body).slice(0, 200)}`);
    }

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
        timeout: "10s",
      });
      check(st, { "get job 200": (r) => r.status === 200 });
    }
  });

  if (__ITER % 25 === 0) {
    const stats = http.get(`${data.base}/stats`, {
      tags: { name: "GET /stats" },
      timeout: "10s",
    });
    check(stats, { "stats 200": (r) => r.status === 200 });
  }

  // Small think-time so arrival-rate scenarios stay realistic; stress still ramps RPS via executor
  sleep(Number(__ENV.THINK || 0.05));
}