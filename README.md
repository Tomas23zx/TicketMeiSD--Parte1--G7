# TicketMEI - Plataforma Eletrónica de Venda de Bilhetes para Eventos (Parte I)

Projeto desenvolvido no âmbito da unidade curricular de **Sistemas Distribuídos (2026/2027)** — Mestrado em Engenharia Informática, Universidade da Madeira.  
**Grupo:** G7  
**Repositório:** [Tomas23zx/TicketMeiSD--Parte1--G7](https://github.com/Tomas23zx/TicketMeiSD--Parte1--G7)

---

## 📖 1. Visão Geral e Arquitetura

O **TicketMEI** é uma plataforma distribuída baseada numa arquitetura de microsserviços concebida para suportar fluxos de consulta e compra de bilhetes para eventos com resiliência, observabilidade e automação GitOps:

- **Fase 1**: Exposição e consulta síncrona do catálogo de eventos através de um **API Gateway**, com monitorização completa em tempo real (**Prometheus**, **Grafana**, **cAdvisor**) e testes de desempenho (**k6**).
- **Fase 2**: Integração dos serviços de compras (**Order**, com registo em estado `PENDING` e limite de 4 bilhetes) e notificações assíncronas (**Notification**, simulando envio de email em 100ms), interligados via **RabbitMQ**.
- **Fase 3**: Resiliência com **Rate Limiting baseado em Redis** no API Gateway, migração completa da infraestrutura para **Kubernetes** (`k8s/`), esteira de **CI/CD com GitHub Actions** (build e publicação de imagens no GitHub Container Registry - GHCR) e reconciliação contínua via **ArgoCD (GitOps)**.

### Diagrama de Arquitetura do Sistema

```
                             +-------------------+
                             |   Cliente / k6    |
                             +---------+---------+
                                       |
                 GET /events           |  POST /orders
                 GET /events/{id}      |  GET /orders/{id}
                                       v
                             +-------------------+       +---------------+
                             |    API Gateway    +<----->+ Redis (Rate   |
                             |      (:8000)      |       | Limiter :6379)|
                             +----+---------+----+       +---------------+
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
                                            [   notification.db      ]

----------------------------------------------------------------------------------
                 Métricas (/metrics) recolhidas por Prometheus (:9090)
                          Visualização via Grafana (:3000)
```

---

## 🧩 2. Componentes e Tecnologias

| Componente | Diretório | Descrição | Tecnologias |
| :--- | :--- | :--- | :--- |
| **API Gateway** | [`api-gateway/`](api-gateway/) | Ponto de entrada do sistema. Encaminha pedidos para Catalog e Order. Implementa **Rate Limiting com Redis** (HTTP 429) e instrumentação Prometheus. | FastAPI, Python 3.12, Redis, HTTPX |
| **Catalog** | [`catalog/`](catalog/) | Serviço de catálogo de eventos. Serve os dados estáticos de `events.json` via endpoints síncronos e expõe métricas de latência e pedidos. | FastAPI, Python 3.12 |
| **Order** | [`order/`](order/) | Gere o fluxo provisório de checkout. Valida limite de bilhetes (máx 4), regista no SQLite (`order.db`) com estado `PENDING` e publica mensagem `OrderCreated` no RabbitMQ. | FastAPI, SQLite, aio-pika |
| **Notification** | [`notification/`](notification/) | Consome mensagens do RabbitMQ de forma assíncrona, simula envio de email (100 ms) e regista auditoria no SQLite (`notification.db`) e ficheiro `notification_logs.json`. | FastAPI, aio-pika, SQLite |
| **RabbitMQ** | - | Message Broker AMQP para comunicação desacoplada e assíncrona entre Order e Notification. Disponibiliza porta AMQP (5672), UI de gestão (15672) e métricas (15692). | Imagem oficial `rabbitmq:3.13-management` |
| **Redis** | - | Armazenamento chave-valor em memória utilizado pelo Rate Limiter do API Gateway com script Lua atómico para contagem e expiração por IP. | Imagem oficial `redis:7-alpine` |
| **Prometheus** | [`prometheus/`](prometheus/) | Motor de métricas time-series. Faz scraping contínuo (intervalo de 5s) dos microsserviços, RabbitMQ e cAdvisor. | Imagem oficial `prom/prometheus:v2.50.0` |
| **Grafana** | [`grafana/`](grafana/) | Dashboards provisionados automaticamente: **Dashboard 1** (Fase 1: Gateway, Latências p95/p99, Erros HTTP) e **Dashboard 2** (Fase 2: Orders, Notifications, Fila RabbitMQ). | Imagem oficial `grafana/grafana:10.3.1` |
| **cAdvisor** | - | Monitorização de recursos ao nível de contentores Docker (CPU, Memória, Rede). | `gcr.io/cadvisor/cadvisor:v0.47.2` |
| **k6** | [`k6/`](k6/) | Scripts de testes de carga e validação: `load-test.js` (catálogo), `load-test-phase2.js` (checkout concorrente) e `rate-limit-test.js` (teste do HTTP 429). | Imagem oficial `grafana/k6:0.49.0` |
| **Kubernetes** | [`k8s/`](k8s/) | Manifestos K8s estruturados com Kustomize (Namespace, Deployments, Services, ConfigMaps, PVCs). | Kubernetes v1.28+, Kustomize |
| **ArgoCD** | [`argocd/`](argocd/) | Orquestrador GitOps que reconcilia automaticamente as alterações na pasta `k8s/` com o cluster local. | ArgoCD Application CRD |

---

## 🚀 3. Como Executar o Projeto

### Opção A: Execução com Docker Compose (Fases 1 e 2)

#### 1. Iniciar toda a infraestrutura
Na raiz do repositório, execute:
```powershell
docker compose up -d --build
```

#### 2. Verificar o estado dos contentores
```powershell
docker compose ps
```
Todos os contentores (`api-gateway`, `catalog`, `order`, `notification`, `rabbitmq`, `redis`, `prometheus`, `grafana`, `cadvisor`) devem apresentar estado `healthy` ou `running`.

#### 3. Executar Testes de Desempenho e Carga (k6)
- **Teste de Carga do Catálogo (Fase 1):**
  ```powershell
  docker compose --profile testing up k6
  ```
- **Teste de Checkout Concorrente de Orders (Fase 2):**
  ```powershell
  docker compose run --rm k6 run load-test-phase2.js
  ```
- **Teste de Validação do Rate Limiter (HTTP 429):**
  ```powershell
  docker compose run --rm k6 run rate-limit-test.js
  ```

#### 4. Parar os serviços
```powershell
docker compose down
```

---

### Opção B: Execução em Kubernetes (Fase 3)

#### Pré-requisitos:
- Cluster Kubernetes ativo (ex.: Kubernetes integrado no **Docker Desktop**, **k3s**, **Minikube** ou **Kind**).
- Ferramenta `kubectl` instalada e configurada.

#### 1. Aplicar todos os manifestos com Kustomize
```powershell
kubectl apply -k k8s
```

#### 2. Verificar o deployment e os pods
```powershell
kubectl get pods -n ticketmei -w
```
Aguarde até que todos os pods estejam em estado `Running`.

#### 3. Verificar os serviços e portas
```powershell
kubectl get svc -n ticketmei
```
- O serviço `api-gateway` está configurado como `LoadBalancer` (acessível diretamente em `http://localhost:8000`).
- No Kubernetes, o Prometheus fica disponível na NodePort `30090` e o Grafana na NodePort `30300` (ou via `kubectl port-forward`).

#### 4. Executar os testes k6 no cluster

Execute os testes individualmente. Cada teste tem um Job e um ConfigMap próprios. Os Jobs executam dentro do Kubernetes, gerando tráfego para os serviços monitorizados pelo Prometheus do cluster.

**Teste de carga do catálogo (Fase 1):**
```powershell
kubectl create configmap ticketmei-k6-phase1 --from-file=load-test.js=k6/load-test.js -n ticketmei --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/load-testing/job-phase1.yaml
kubectl logs -n ticketmei -f job/ticketmei-load-test-phase1
```

**Teste de checkout concorrente (Fase 2):**
```powershell
kubectl create configmap ticketmei-k6-phase2 --from-file=load-test-phase2.js=k6/load-test-phase2.js -n ticketmei --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/load-testing/job-phase2.yaml
kubectl logs -n ticketmei -f job/ticketmei-load-test-phase2
```

**Teste do rate limiter (HTTP 429):**
```powershell
kubectl create configmap ticketmei-k6-rate-limit --from-file=rate-limit-test.js=k6/rate-limit-test.js -n ticketmei --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f k8s/load-testing/job-rate-limit.yaml
kubectl logs -n ticketmei -f job/ticketmei-rate-limit-test
```

O ficheiro `k6/load_test.js` é um alias do teste do catálogo (`k6/load-test.js`), não um teste separado. Para ver o estado dos Jobs e pods:
```powershell
kubectl get jobs,pods -n ticketmei
```

Consulte os dashboards no [Grafana](http://localhost:30300) e confirme que o target `api-gateway` está `UP` nos [targets do Prometheus](http://localhost:30090/targets). Para repetir um teste, apague apenas o Job correspondente e volte a aplicar o respetivo manifesto. Por exemplo, para a Fase 2:
```powershell
kubectl delete job ticketmei-load-test-phase2 -n ticketmei
kubectl apply -f k8s/load-testing/job-phase2.yaml
```

O dashboard **TicketMei - Rate Limiting** fica na pasta `Ticketmei` do Grafana e mostra checkouts aceites (`202`), bloqueados (`429`), total de bloqueios e percentagem bloqueada. Para o carregar no cluster:
```powershell
kubectl apply -k k8s
kubectl rollout restart deployment/grafana -n ticketmei
```

Para interromper um teste antes de terminar, apague o respetivo Job:
```powershell
kubectl delete job ticketmei-load-test-phase1 -n ticketmei
kubectl delete job ticketmei-load-test-phase2 -n ticketmei
kubectl delete job ticketmei-rate-limit-test -n ticketmei
```

#### 5. Remover os recursos do cluster
```powershell
kubectl delete -k k8s
```

---

### Opção C: Deployment GitOps Automatizado com ArgoCD (Fase 3)

1. **Instalar o ArgoCD no cluster local:**
   ```powershell
   kubectl create namespace argocd
   kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml
   ```

2. **Aceder à UI do ArgoCD:**
   ```powershell
   kubectl port-forward svc/argocd-server -n argocd 8080:443
   ```
   - URL: [https://localhost:8080](https://localhost:8080)
   - Utilizador: `admin`
   - Password inicial:
     ```powershell
    kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | ForEach-Object { [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($_)) }
     ```

3. **Submeter a aplicação ArgoCD:**
   ```powershell
   kubectl apply -f argocd/application.yaml
   ```
   O ArgoCD irá monitorizar a pasta `k8s/` do repositório GitHub e aplicar/reconciliar automaticamente quaisquer alterações no cluster.

4. **Aceder às ferramentas de observabilidade**

    O Grafana fica disponível em [http://localhost:30300](http://localhost:30300) e o
    Prometheus em [http://localhost:30090](http://localhost:30090).

5. **Remover a aplicação gerida pelo Argo CD**

    ```powershell
    kubectl delete -f argocd/application.yaml
    ```


---

## 🛡️ 4. Mecanismo de Resiliência: Rate Limiting com Redis

O **API Gateway** inclui um middleware assíncrono de proteção contra sobrecarga baseado em Redis.

- **Funcionamento:** Utiliza um script Lua atómico (`INCR` + `EXPIRE` + `PTTL`) que mantém um contador de pedidos associado ao IP do cliente (`ticketmei:rate-limit:{client_ip}`).
- **Variáveis de Ambiente:**
  - `RATE_LIMIT_REQUESTS`: Limite de pedidos permitidos dentro da janela temporal (ex.: `100` ou `5`).
  - `RATE_LIMIT_WINDOW_SECONDS`: Tamanho da janela em segundos (ex.: `60` ou `1`).
- **Comportamento e Respostas:**
  - Pedidos dentro do limite: Devolvem os cabeçalhos `X-RateLimit-Limit` e `X-RateLimit-Remaining`.
  - Pedidos que excedem o limite: Rejeitados imediatamente com código **`HTTP 429 Too Many Requests`** e cabeçalho `Retry-After` indicando os segundos até à libertação da quota.

### Como testar o Rate Limiter manualmente:
```powershell
# Disparar rajada de pedidos rápidos via PowerShell para testar a rejeição 429
1..30 | ForEach-Object {
    Invoke-RestMethod -Uri "http://localhost:8000/events" -Method Get -SkipHttpErrorCheck
}
```

---

## 🔄 5. Pipeline de CI/CD (GitHub Actions)

A automação do repositório está configurada em [`.github/workflows/ci-cd.yml`](.github/workflows/ci-cd.yml):

```
+--------------------+      +--------------------+      +--------------------+      +--------------------+
|   Push / PR para   | ---> |  Job 'validate':   | ---> |  Job 'publish':    | ---> |  ArgoCD Reconcile  |
|       'main'       |      |  - Sintaxe Python  |      |  - Build Docker    |      |  - Deteta novo SHA |
|                    |      |  - Docker Compose  |      |  - Push para GHCR  |      |  - Rolling update  |
|                    |      |  - Kustomize K8s   |      |  - Update tags k8s |      |    no cluster      |
+--------------------+      +--------------------+      +--------------------+      +--------------------+
```

1. **Validação:** Valida a compilação do código Python, os manifestos de orquestração do Docker Compose e a geração de manifestos do Kubernetes via Kustomize.
2. **Build e Publicação no GHCR:** Gera imagens de cada microsserviço tagadas com o Git SHA do commit (`ghcr.io/luisgsousaa/ticketmei-*:<SHA>`).
3. **Atualização Automática dos Manifestos:** Atualiza os ficheiros `k8s/*/deployment.yaml` com a nova imagem e realiza commit/push automático de volta ao repositório para acionar a sincronização pelo ArgoCD.

---

## 🌐 6. Tabela de Endpoints e Acessos Rápidos

Com os serviços em execução, aceda aos seguintes URLs:

| Serviço | URL (Docker Compose) | URL (Kubernetes) | Descrição |
| :--- | :--- | :--- | :--- |
| **API Gateway - Eventos** | [http://localhost:8000/events](http://localhost:8000/events) | [http://localhost:8000/events](http://localhost:8000/events) | Consulta de catálogo via gateway |
| **API Gateway - Evento Individual** | [http://localhost:8000/events/evt-101](http://localhost:8000/events/evt-101) | [http://localhost:8000/events/evt-101](http://localhost:8000/events/evt-101) | Detalhes do evento `evt-101` |
| **API Gateway - Criar Encomenda** | `POST http://localhost:8000/orders` | `POST http://localhost:8000/orders` | Inicia checkout provisório (`HTTP 202`) |
| **API Gateway - Ver Encomenda** | [http://localhost:8000/orders/1](http://localhost:8000/orders/1) | [http://localhost:8000/orders/1](http://localhost:8000/orders/1) | Consulta detalhes da encomenda `#1` |
| **API Gateway - OpenAPI / Swagger** | [http://localhost:8000/docs](http://localhost:8000/docs) | [http://localhost:8000/docs](http://localhost:8000/docs) | Documentação interativa da API |
| **API Gateway - Métricas** | [http://localhost:8000/metrics](http://localhost:8000/metrics) | [http://localhost:8000/metrics](http://localhost:8000/metrics) | Métricas Prometheus do Gateway |
| **Catalog - Métricas** | [http://localhost:8001/metrics](http://localhost:8001/metrics) | Cluster interno (`:8001/metrics`) | Métricas do catálogo |
| **Order - Métricas** | [http://localhost:8002/metrics](http://localhost:8002/metrics) | Cluster interno (`:8002/metrics`) | Métricas do serviço de compras |
| **Notification - Métricas** | [http://localhost:8003/metrics](http://localhost:8003/metrics) | Cluster interno (`:8003/metrics`) | Métricas do serviço de notificações |
| **Notification - Lista de Notificações** | [http://localhost:8003/notifications](http://localhost:8003/notifications) | Cluster interno (`:8003/notifications`) | Registo de notificações processadas |
| **RabbitMQ - Consola de Gestão** | [http://localhost:15672](http://localhost:15672) | Cluster interno / Port-forward | Web UI RabbitMQ (user: `guest` / pass: `guest`) |
| **Prometheus Web UI** | [http://localhost:9090](http://localhost:9090) | [http://localhost:30090](http://localhost:30090) | Interface de consulta do Prometheus |
| **Grafana Dashboard Fase 1** | [http://localhost:3000](http://localhost:3000) | [http://localhost:30300](http://localhost:30300) | Painel Gateway e Catálogo (admin / admin) |
| **Grafana Dashboard Fase 2** | [http://localhost:3000](http://localhost:3000) | [http://localhost:30300](http://localhost:30300) | Painel Orders, Notifications e RabbitMQ |
| **cAdvisor UI** | [http://localhost:8080](http://localhost:8080) | - | Métricas de hardware e contentores |

---

## 🧪 7. Testes Manuais via Linha de Comandos (cURL / PowerShell)

```powershell
# 1. Consultar todos os eventos disponíveis
curl http://localhost:8000/events

# 2. Consultar detalhes de um evento específico
curl http://localhost:8000/events/evt-101

# 3. Iniciar a compra de 2 bilhetes (retorna HTTP 202 Accepted)
curl -X POST http://localhost:8000/orders `
  -H "Content-Type: application/json" `
  -d '{"customer_id": 1001, "event_id": "evt-101", "quantity": 2, "total_amount": 170.0, "customer_email": "cliente@ticketmei.pt"}'

# 4. Tentar comprar 5 bilhetes (excede o limite de 4 bilhetes -> HTTP 400 Bad Request)
curl -X POST http://localhost:8000/orders `
  -H "Content-Type: application/json" `
  -d '{"customer_id": 1001, "event_id": "evt-101", "quantity": 5, "total_amount": 425.0, "customer_email": "cliente@ticketmei.pt"}'

# 5. Consultar a encomenda registada
curl http://localhost:8000/orders/1

# 6. Inspecionar o registo de envio de email na Notification
curl http://localhost:8003/notifications
```
