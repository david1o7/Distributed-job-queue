import http from "k6/http";
import { check, sleep } from "k6";

const BASE = __ENV.BASE_URL || "http://localhost:8080";

export const options = {
  vus: 5,
  duration: "20s",
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<800"],
  },
};

export default function () {
  const res = http.post(
    `${BASE}/job`,
    JSON.stringify({
      type: "print",
      priority: "default",
      payload: { name: "smoke" },
    }),
    { headers: { "Content-Type": "application/json" } }
  );
  check(res, { "accepted": (r) => r.status === 202 || r.status === 200 });
  sleep(0.2);
}