import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const inventoryUrl = __ENV.INVENTORY_URL || 'http://inventory:8005';
const orderUrl = __ENV.ORDER_URL || 'http://order:8002';
const eventId = __ENV.EVENT_ID || `evt-saga-loadtest-${Date.now()}`;

const confirmedOrders = new Counter('saga_load_confirmed_orders');
const stockRejectedOrders = new Counter('saga_load_stock_rejected_orders');
const otherResponses = new Counter('saga_load_other_responses');
const confirmedLatency = new Trend('saga_load_confirmed_latency_ms');
const rejectedLatency = new Trend('saga_load_stock_rejected_latency_ms');

http.setResponseCallback(http.expectedStatuses(201, 409));

export const options = {
  scenarios: {
    simultaneous_checkout: {
      executor: 'per-vu-iterations',
      vus: 1000,
      iterations: 1,
      maxDuration: '2m',
      gracefulStop: '30s',
    },
  },
  thresholds: {
    saga_load_confirmed_orders: ['count==20'],
    saga_load_stock_rejected_orders: ['count==980'],
    saga_load_other_responses: ['count==0'],
  },
};

export function setup() {
  const response = http.post(
    `${inventoryUrl}/inventory/stock`,
    JSON.stringify({ event_id: eventId, total_tickets: 20 }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  if (response.status !== 200) {
    throw new Error(`Could not configure test stock: HTTP ${response.status} ${response.body}`);
  }
  return { eventId };
}

export default function (data) {
  const response = http.post(
    `${orderUrl}/orders`,
    JSON.stringify({
      customer_id: 100000 + __VU,
      event_id: data.eventId,
      quantity: 1,
      total_amount: 85,
      customer_email: `loadtest-${__VU}@ticketmei.pt`,
    }),
    { headers: { 'Content-Type': 'application/json' } },
  );

  if (response.status === 201) {
    confirmedOrders.add(1);
    confirmedLatency.add(response.timings.duration);
  } else if (response.status === 409) {
    stockRejectedOrders.add(1);
    rejectedLatency.add(response.timings.duration);
  } else {
    otherResponses.add(1);
  }

  check(response, {
    'checkout confirmed or rejected for insufficient stock': (r) =>
      r.status === 201 || r.status === 409,
  });
}
