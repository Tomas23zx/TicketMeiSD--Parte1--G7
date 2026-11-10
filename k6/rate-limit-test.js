import http from 'k6/http';
import { check, sleep } from 'k6';

// Teste desenhado para exceder o limite do Rate Limiter e validar respostas HTTP 429
export const options = {
  scenarios: {
    rate_limit_burst: {
      executor: 'constant-arrival-rate',
      rate: 30, // 30 pedidos por segundo para forçar ultrapassagem do limite configurado
      timeUnit: '1s',
      duration: '20s',
      preAllocatedVUs: 10,
      maxVUs: 50,
    },
  },
  thresholds: {
    // Pelo menos algumas requisições devem receber HTTP 429 (Too Many Requests)
    'http_reqs{status:429}': ['count>0'],
  },
};

export default function () {
  const baseUrl = __ENV.BASE_URL || 'http://api-gateway:8000';

  const payload = JSON.stringify({
    customer_id: 1001,
    event_id: 'evt-101',
    quantity: 2,
    total_amount: 170.0,
    customer_email: 'test-ratelimit@ticketmei.pt',
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post(`${baseUrl}/orders`, payload, params);

  check(res, {
    'status is 202 or 429': (r) => r.status === 202 || r.status === 429,
    'rate limit headers present': (r) =>
      r.headers['X-Ratelimit-Limit'] !== undefined ||
      r.headers['X-RateLimit-Limit'] !== undefined,
  });

  sleep(0.05);
}

