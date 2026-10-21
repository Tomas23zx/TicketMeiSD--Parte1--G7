import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '30s', target: 50 },  // Ramp-up
    { duration: '1m', target: 100 },   // Steady traffic
    { duration: '15s', target: 0 },    // Ramp-down
  ],
  thresholds: {
    http_req_duration: ['p(95)<300'], // 95% of requests below 300ms
    http_req_failed: ['rate<0.02'],   // Less than 2% failure rate
  },
};

export default function () {
  const gatewayUrl = 'http://api-gateway:8000';

  // 1. Query catalog events
  const catalogRes = http.get(`${gatewayUrl}/events`);
  check(catalogRes, {
    'catalog status is 200': (r) => r.status === 200,
  });

  // 2. Initiate ticket order (asynchronous checkout)
  const orderPayload = JSON.stringify({
    customer_id: Math.floor(Math.random() * 9000) + 1000,
    event_id: 'evt-101',
    quantity: Math.floor(Math.random() * 4) + 1, // 1 to 4 tickets
    total_amount: 85.0 * 2,
    customer_email: 'buyer@ticketmei.pt',
  });

  const orderParams = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const orderRes = http.post(`${gatewayUrl}/orders`, orderPayload, orderParams);
  check(orderRes, {
    'order checkout status is 202': (r) => r.status === 202,
  });

  sleep(0.1);
}
