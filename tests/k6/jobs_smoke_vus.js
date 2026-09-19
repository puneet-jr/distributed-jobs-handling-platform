import http from "k6/http"; 
import { check, sleep } from "k6";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";

export default function () {
  const idempotencyKey = `smoke-vus-${__VU}-${__ITER}-${Date.now()}`;

  const body = JSON.stringify({
    type: "email",
    payload: {
      recipient: `smoke-${__VU}-${__ITER}@example.com`,
      subject: "smoke test",
      body: "tiny payload",
    },
    priority: 0,
  });

  const res = http.post(`${BASE_URL}/jobs`, body, {
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey,
    },
  });

  check(res, {
    "status is 202": (r) => r.status === 202,
    "has jobId": (r) => {
      try {
        return Boolean(JSON.parse(r.body).jobId);
      } catch (_) {
        return false;
      }
    },
  });

  sleep(1);
}


