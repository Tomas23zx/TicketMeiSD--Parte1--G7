# TicketMEI - Plataforma de Venda de Bilhetes para Eventos (Parte I)

Projeto desenvolvido no âmbito da unidade curricular de **Sistemas Distribuídos (2026/2027)** — Mestrado em Engenharia Informática, Universidade da Madeira.

---

## 📖 Visão Geral e Arquitetura

O **TicketMEI** é uma plataforma baseada numa arquitetura de microsserviços. Na **Fase 1**, a plataforma foca-se na disponibilização e consulta síncrona do catálogo de eventos através de um **API Gateway**, acompanhado por uma pilha completa de observabilidade (**Prometheus**, **Grafana**, **cAdvisor**) e testes de carga (**k6**).

```
                      +-------------------+
                      |   Cliente / k6    |
                      +---------+---------+
                                |
                   GET /events  |  GET /events/{id}
                                v
                      +-------------------+
                      |    API Gateway    | :8000
                      +---------+---------+
                                |
                     (HTTP síncrono interno)
                                v
                      +-------------------+
                      |      Catalog      | :8001
                      +---------+---------+
                                |
                                v
                      [ data/events.json ]
```

---

## 🧩 Explicação de Cada Componente

| Componente | Pasta / Ficheiro | Descrição |
| :--- | :--- | :--- |
| **API Gateway** | [`api-gateway/`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/api-gateway/) | Ponto único de entrada para clientes externos. Encaminha pedidos síncronos para o serviço de Catálogo (`/events` e `/events/{id}`). Mede a latência de cada rota e contabiliza os pedidos e códigos de estado HTTP para o Prometheus. |
| **Catalog** | [`catalog/`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/catalog/) | Microsserviço de gestão de eventos. Lê os dados de `data/events.json` e serve a lista de eventos ou detalhes de um evento específico. Expõe também métricas internas no endpoint `/metrics`. |
| **Fonte de Dados** | [`data/events.json`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/data/events.json) | Ficheiro JSON com os eventos disponíveis (concertos, locais, preços base, total de bilhetes). É montado como volume só de leitura (`:ro`) no contentor do Catalog. |
| **Prometheus** | [`prometheus/`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/prometheus/) | Sistema de recolha de métricas (time-series). Faz scraping a cada 5 segundos dos endpoints `/metrics` do API Gateway, do Catalog e do cAdvisor. |
| **Grafana** | [`grafana/`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/grafana/) e [`dashboard1.json`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/dashboard1.json) | Ferramenta de visualização pré-configurada. Cria automaticamente a datasource Prometheus e carrega o painel que monitoriza o débito (**RPS**), latência percentílica (**p95/p99**) e **taxa de erro HTTP (%)**. |
| **cAdvisor** | Imagem `cadvisor` | Coletor de métricas ao nível dos contentores (utilização de CPU, memória, I/O de rede e do sistema Docker). |
| **k6** | [`k6/load-test.js`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/k6/load-test.js) | Ferramenta de teste de carga. Simula um perfil de tráfego que atinge até 500 utilizadores virtuais em simultâneo (VUs) contra o API Gateway, validando SLAs de latência (p95 < 200ms) e taxas de erro (< 1%). |
| **CI / GitHub Actions** | [`.github/workflows/ci.yml`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/.github/workflows/ci.yml) | Pipeline automatizado que valida a compilação de todas as imagens, inicia o ambiente com Docker Compose, valida os endpoints e corre os testes do k6 a cada `push` ou `pull_request`. |

---

## 🚀 Como Executar o Projeto

### Pré-requisitos
- Ter o **[Docker Desktop](https://www.docker.com/products/docker-desktop/)** instalado e em execução.

---

### Método 1: Execução com Docker Compose (Recomendado)

O ficheiro [`docker-compose.yml`](file:///c:/Users/nelso/Desktop/SD/Parte-I---ficheiros/docker-compose.yml) orquestra toda a pilha de serviços.

#### 1. Iniciar os serviços
Na raiz do projeto (no PowerShell ou terminal), execute:

```powershell
docker compose up --build
```

> **Dica:** Para executar em segundo plano (modo detached), utilize:
> ```powershell
> docker compose up -d --build
> ```

#### 2. Executar os Testes de Carga (k6)
Com os serviços em execução, abra um novo terminal e lance o perfil de testes:

```powershell
docker compose --profile testing up k6
```

#### 3. Parar todos os serviços
Para parar e remover os contentores criados:

```powershell
docker compose down
```

---

### Método 2: Execução Local em Python (Sem Docker - para Desenvolvimento)

Se pretender depurar os serviços individualmente em modo local:

1. **Criar e ativar o ambiente virtual:**
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

2. **Iniciar o serviço Catalog (Terminal 1):**
   ```powershell
   pip install -r catalog/requirements.txt
   uvicorn catalog.main:app --host 0.0.0.0 --port 8001 --reload
   ```

3. **Iniciar o API Gateway (Terminal 2):**
   ```powershell
   pip install -r api-gateway/requirements.txt
   $env:CATALOG_URL="http://localhost:8001"
   uvicorn api-gateway.main:app --host 0.0.0.0 --port 8000 --reload
   ```

---

## 🌐 Endpoints e Acessos Rápidos

Com os contentores ativos, clique nos links abaixo para aceder diretamente a cada serviço:

| Serviço | URL | Descrição |
| :--- | :--- | :--- |
| **API Gateway - Eventos** | [http://localhost:8000/events](http://localhost:8000/events) | Lista todos os eventos via gateway |
| **API Gateway - Evento Individual** | [http://localhost:8000/events/evt-101](http://localhost:8000/events/evt-101) | Detalhe do evento `evt-101` via gateway |
| **API Gateway - Swagger UI** | [http://localhost:8000/docs](http://localhost:8000/docs) | Documentação interativa da API do Gateway |
| **API Gateway - Métricas** | [http://localhost:8000/metrics](http://localhost:8000/metrics) | Métricas expostas para o Prometheus |
| **Catalog - Eventos** | [http://localhost:8001/events](http://localhost:8001/events) | Acesso direto ao serviço de catálogo |
| **Catalog - Swagger UI** | [http://localhost:8001/docs](http://localhost:8001/docs) | Documentação interativa da API do Catalog |
| **Catalog - Métricas** | [http://localhost:8001/metrics](http://localhost:8001/metrics) | Métricas do catálogo |
| **Prometheus Web UI** | [http://localhost:9090](http://localhost:9090) | Interface de consulta do Prometheus |
| **Prometheus Targets** | [http://localhost:9090/targets](http://localhost:9090/targets) | Estado das recolhas (Gateway, Catalog, cAdvisor) |
| **Grafana Dashboard** | [http://localhost:3000/d/be277e65-6d02-436a-9203-8dcbd5d00cad/ticketmei-p1](http://localhost:3000/d/be277e65-6d02-436a-9203-8dcbd5d00cad/ticketmei-p1) | Dashboard TicketMei - P1 (**Login:** `admin` / `admin`) |
| **cAdvisor UI** | [http://localhost:8080](http://localhost:8080) | Monitorização do hardware e contentores |

---

## 🧪 Testar os Endpoints Manualmente

Pode enviar pedidos via `curl` ou PowerShell para testar a comunicação e observar os gráficos no Grafana a atualizarem em tempo real:

```powershell
# Listar todos os eventos
curl http://localhost:8000/events

# Consultar evento específico
curl http://localhost:8000/events/evt-101

# Verificar métricas do gateway
curl http://localhost:8000/metrics
```
