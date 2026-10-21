# TicketMEI - Plataforma de Venda de Bilhetes para Eventos

Projeto desenvolvido no âmbito da unidade curricular de **Sistemas Distribuídos (2026/2027)** - Mestrado em Engenharia Informática, Universidade da Madeira.

---

## 📖 Visão Geral e Arquitetura

O **TicketMEI** é uma plataforma baseada numa arquitetura de microsserviços:
- **Fase 1**: Disponibilização e consulta síncrona do catálogo de eventos através de um **API Gateway**, acompanhado por uma pilha completa de observabilidade (**Prometheus**, **Grafana**, **cAdvisor**) e testes de carga (**k6**).
- **Fase 2**: Integração dos serviços **Order** (checkout provisório no estado PENDING) e **Notification** (consumo assíncrono e envio simulado de emails em 100ms), comunicando via **RabbitMQ**.

```
                      +-------------------+
                      |   Cliente / k6    |
                      +---------+---------+
                                |
             GET /events        |  POST /orders
             GET /events/{id}   |  GET /orders/{id}
                                v
                      +-------------------+
                      |    API Gateway    | :8000
                      +----+---------+----+
                           |         |
                  (HTTP)   |         |  (HTTP)
                           v         v
     +-------------------+             +-------------------+
     |      Catalog      | :8001       |       Order       | :8002
     +---------+---------+             +---------+---------+
               |                                 |  (AMQP)
               v                                 v
      [ data/events.json ]             +-------------------+
                                       |     RabbitMQ      | :5672 / :15672
                                       +---------+---------+
                                                 |  (AMQP)
                                                 v
                                       +-------------------+
                                       |   Notification    | :8003
                                       +---------+---------+
                                                 |
                                                 v
                                     [ notification_logs.json ]
```

---

## 🧩 Explicação de Cada Componente

| Componente | Pasta / Ficheiro | Descrição |
| :--- | :--- | :--- |
| **API Gateway** | [`api-gateway/`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/api-gateway/) | Ponto único de entrada. Encaminha pedidos para Catalog (`/events`) e Order (`POST /orders`, `GET /orders/{id}`). Mede latência, códigos de estado e pedidos para o Prometheus. |
| **Catalog** | [`catalog/`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/catalog/) | Microsserviço de gestão de eventos. Lê dados de `data/events.json` e serve a lista ou detalhes de um evento específico. Expõe `/metrics`. |
| **Order** | [`order/`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/order/) | Recebe pedidos de compra, valida limites (máx 4 bilhetes), regista no SQLite local (`order.db`) com estado `PENDING`, publica mensagem `OrderCreated` no RabbitMQ e responde `HTTP 202 Accepted`. |
| **Notification** | [`notification/`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/notification/) | Consome mensagens do RabbitMQ, simula envio de email com atraso de 100ms e regista nos logs (`notification_logs.json`) e DB (`notification.db`). |
| **RabbitMQ** | Imagem `rabbitmq:3.13-management` | Message broker de mensageria assíncrona entre Order e Notification. Fornece Web UI (:15672) e métricas nativas do Prometheus (:15692). |
| **Fonte de Dados** | [`data/`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/data/) | Ficheiro `events.json`, bases de dados SQLite (`order.db`, `notification.db`) e ficheiro de auditoria `notification_logs.json`. |
| **Prometheus** | [`prometheus/`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/prometheus/) | Sistema de time-series. Faz scraping a cada 5s de Gateway, Catalog, Order, Notification, RabbitMQ e cAdvisor. |
| **Grafana** | [`grafana/`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/grafana/) | Dashboards provisionados: `dashboard1.json` (Fase 1: Gateway e Catalog) e `dashboard2.json` (Fase 2: Orders, Notifications e RabbitMQ). |
| **cAdvisor** | Imagem `cadvisor` | Coletor de métricas ao nível de contentores Docker (CPU, memória, rede). |
| **k6** | [`k6/`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/k6/) | Testes de carga: `load-test.js` (catálogo) e `load-test-phase2.js` (checkout concorrente de orders). |

---

## 🚀 Como Executar o Projeto

### Pré-requisitos
- Ter o **[Docker Desktop](https://www.docker.com/products/docker-desktop/)** instalado e em execução.

---

### Execução com Docker Compose

O ficheiro [`docker-compose.yml`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/docker-compose.yml) orquestra toda a pilha de serviços.

#### 1. Iniciar os serviços
Na raiz do projeto (no PowerShell ou terminal):

```powershell
docker compose up -d --build
```

#### 2. Verificar o estado dos contentores
```powershell
docker compose ps
```

#### 3. Executar os Testes de Carga (k6)
Para executar o teste de carga da Fase 1:
```powershell
docker compose --profile testing up k6
```

Para executar o teste de carga da Fase 2 (Orders concorrentes):
```powershell
docker compose run --rm k6 run load-test-phase2.js
```

#### 4. Parar todos os serviços
```powershell
docker compose down
```

---

## 🌐 Endpoints e Acessos Rápidos

Com os contentores ativos, clique nos links abaixo para aceder diretamente a cada serviço:

| Serviço | URL | Descrição |
| :--- | :--- | :--- |
| **API Gateway - Eventos** | [http://localhost:8000/events](http://localhost:8000/events) | Lista todos os eventos via gateway |
| **API Gateway - Evento Individual** | [http://localhost:8000/events/evt-101](http://localhost:8000/events/evt-101) | Detalhe do evento `evt-101` via gateway |
| **API Gateway - Ver Orders** | [POST http://localhost:8000/orders](http://localhost:8000/orders) | Inicia checkout de bilhetes (`HTTP 202`) |
| **API Gateway - Detalhes da Order** | [http://localhost:8000/orders/1](http://localhost:8000/orders/1) | Consulta detalhes da encomenda `#1` |
| **API Gateway - Swagger UI** | [http://localhost:8000/docs](http://localhost:8000/docs) | Documentação OpenAPI interativa |
| **API Gateway - Métricas** | [http://localhost:8000/metrics](http://localhost:8000/metrics) | Métricas expostas para o Prometheus |
| **Catalog - Swagger UI** | [http://localhost:8001/docs](http://localhost:8001/docs) | OpenAPI do serviço de Catálogo |
| **Catalog - Métricas** | [http://localhost:8001/metrics](http://localhost:8001/metrics) | Métricas do catálogo |
| **Order - Swagger UI** | [http://localhost:8002/docs](http://localhost:8002/docs) | OpenAPI do serviço de Orders |
| **Order - Métricas** | [http://localhost:8002/metrics](http://localhost:8002/metrics) | Métricas do serviço de Orders |
| **Notification - Notificações** | [http://localhost:8003/notifications](http://localhost:8003/notifications) | Lista de notificações processadas |
| **Notification - Métricas** | [http://localhost:8003/metrics](http://localhost:8003/metrics) | Métricas do serviço de Notificações |
| **RabbitMQ Management** | [http://localhost:15672](http://localhost:15672) | Consola RabbitMQ (Login: `guest` / `guest`) |
| **Prometheus Web UI** | [http://localhost:9090](http://localhost:9090) | Interface do Prometheus |
| **Prometheus Targets** | [http://localhost:9090/targets](http://localhost:9090/targets) | Estado das recolhas de métricas |
| **Grafana Dashboard Fase 1** | [http://localhost:3000/d/be277e65-6d02-436a-9203-8dcbd5d00cad/ticketmei-p1](http://localhost:3000/d/be277e65-6d02-436a-9203-8dcbd5d00cad/ticketmei-p1) | Painel Fase 1 (Login: `admin` / `admin`) |
| **Grafana Dashboard Fase 2** | [http://localhost:3000/d/ticketmei-phase2-dashboard/ticketmei-p2-orders-notifications-rabbitmq](http://localhost:3000/d/ticketmei-phase2-dashboard/ticketmei-p2-orders-notifications-rabbitmq) | Painel Fase 2 (Orders, Notificações, Fila) |
| **cAdvisor UI** | [http://localhost:8080](http://localhost:8080) | Monitorização de hardware e contentores |

---

## 🧪 Testar os Endpoints Manualmente

```powershell
# 1. Consultar eventos
curl http://localhost:8000/events

# 2. Iniciar compra de bilhetes (status 202 Accepted)
curl -X POST http://localhost:8000/orders `
  -H "Content-Type: application/json" `
  -d '{"customer_id": 1001, "event_id": "evt-101", "quantity": 2, "total_amount": 170.0, "customer_email": "cliente@ticketmei.pt"}'

# 3. Consultar a encomenda criada
curl http://localhost:8000/orders/1

# 4. Verificar se a notificação foi processada assincronamente pelo Notification Service
curl http://localhost:8003/notifications

# 5. Inspecionar métricas do Prometheus
curl http://localhost:8002/metrics
curl http://localhost:8003/metrics
```
