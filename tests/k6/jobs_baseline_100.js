import http from "k6/http";
import { check } from "k6";
import { Rate, Counter } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";

const acceptedRate = new Rate("jobs_accepted_rate");
const droppedCheck = new Counter("manual_drop_watch");

export const options = {
  scenarios: {
    baseline_100: {
      executor: "const-arrival-rate",
      rate: 100,
      time: "1s",
      duration: "1m",
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },

  thresholds: {
    http_req_failed: ["rate<0.01"],
    jobs_accepted_rate: ["rate>0.99"],
  },
};

export default function () {
  const idempotencyKey = `baseline100-${__VU}-${__ITER}-${Date.now()}`;

  const body = JSON.stringify({
    type: "email",
    payload: {
      recipient: `load-${__VU}-${__ITER}@example.com`,
      subject: "baseline 100rps",
      body: "small",
    },
    priority: 0,
  });

  const res = http.post(`${BASE_URL}/jobs`, body, {
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey,
    },
  });

  const accepted = check(res, {
    "status is 202": (r) => r.status === 202,
  });

  acceptedRate.add(accepted);
}
