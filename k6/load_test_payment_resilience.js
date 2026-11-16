import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const gatewayUrl = __ENV.GATEWAY_URL || 'http://api-gateway:8000';

const confirmedOrders = new Counter('orders_confirmed_total');
const paymentFallbacks = new Counter('orders_fallback_503_total');
const rateLimitedOrders = new Counter('orders_rate_limited_429_total');
const expectedOrderResponses = new Rate('expected_order_responses');

export const options = {
  scenarios: {
    orders: {
      executor: 'constant-arrival-rate',
      rate: 50,
      timeUnit: '1s',
      duration: '5m',
      preAllocatedVUs: 75,
      maxVUs: 500,
    },
  },
  thresholds: {
    expected_order_responses: ['rate>0.95'],
  },
};

function randomForwardedFor() {
  return `198.51.100.${Math.floor(Math.random() * 254) + 1}`;
}

export default function () {
  const payload = JSON.stringify({
    customer_id: 100000 + __VU,
    event_id: `fase1-${__VU}-${__ITER}`,
    quantity: Math.floor(Math.random() * 4) + 1,
    total_amount: 50.0,
    customer_email: `fase1-${__VU}@ticketmei.pt`,
  });

  const response = http.post(`${gatewayUrl}/orders`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'X-Forwarded-For': randomForwardedFor(),
    },
    tags: { endpoint: 'POST /orders' },
  });

  const isConfirmed = response.status === 201;
  const isFallback = response.status === 503;
  const isRateLimited = response.status === 429;
  const isExpectedResponse = isConfirmed || isFallback || isRateLimited;

  expectedOrderResponses.add(isExpectedResponse);
  if (isConfirmed) {
    confirmedOrders.add(1);
  }
  if (isFallback) {
    paymentFallbacks.add(1);
  }
  if (isRateLimited) {
    rateLimitedOrders.add(1);
  }

  check(response, {
    'order response is expected (201, 503 or 429)': () => isExpectedResponse,
    'order confirmed (HTTP 201)': (r) => r.status === 201,
    'circuit breaker fallback (HTTP 503)': (r) => r.status === 503,
    'not rate limited (HTTP != 429)': (r) => r.status !== 429,
  });
}
