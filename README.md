# Yandex API Gateway HTTP → WebSocket bridge

Bridge [Yandex Cloud API Gateway](https://yandex.cloud/en/docs/api-gateway/) WebSocket callbacks (`CONNECT`, `MESSAGE`, `DISCONNECT`) into one ordinary upstream WebSocket connection per client.

> **Warning:** Experimental vibe-coded proof of concept — not production-hardened. Use at your own risk.

## How it works

```
Client ──WS──► API Gateway ──POST──► Bridge (VM / container) ──WS──► Backend
```

1. API Gateway sends WebSocket lifecycle events to the bridge (`POST /ws`, defined in `openapi.yaml`).
2. On **CONNECT**, the bridge opens a real WebSocket to your backend and stores it keyed by `X-Yc-Apigateway-Websocket-Connection-Id`.
3. On **MESSAGE**, the bridge forwards the HTTP body to that upstream WebSocket (with optional reordering for out-of-order callbacks).
4. A background task reads frames from the upstream WebSocket and pushes them back to the correct API Gateway connection via the [connection management API](https://yandex.cloud/en/docs/api-gateway/concepts/extensions/websocket).
5. On **DISCONNECT**, the bridge closes the upstream WebSocket and drops the session.

## Requirements

- Python 3.12+ (for local runs), or Docker
- A Yandex Cloud API Gateway with the bundled `openapi.yaml` spec
- A VM (or similar) reachable from API Gateway for the bridge
- A service account with the `api-gateway.websocketWriter` role when using instance metadata auth (recommended on YC)

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `UPSTREAM_WS_URL` | yes | — | Backend WebSocket URL, e.g. `wss://backend.example/ws` |
| `YC_IAM_TOKEN` | no | metadata | IAM token for the connection management API; omit on YC to use the [metadata service](https://yandex.cloud/en/docs/compute/operations/vm-connect/auth-inside-vm) |
| `HOST` | no | `0.0.0.0` | Bind address |
| `PORT` | no | `8080` | Listen port |
| `LOG_LEVEL` | no | `INFO` | Log level |
| `UPSTREAM_CONNECT_TIMEOUT` | no | `5` | Seconds to wait when opening the upstream WebSocket |
| `MESSAGE_REORDER_WINDOW` | no | `0.05` | Seconds to buffer out-of-order HTTP callbacks before forwarding |

When `YC_IAM_TOKEN` is unset, the official SDK uses the instance metadata service. The attached service account must have **`api-gateway.websocketWriter`**.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export UPSTREAM_WS_URL="wss://backend.example/ws"
export YC_IAM_TOKEN="..."   # optional outside YC; omit on YC to use metadata

python bridge.py
```

## Run with Docker

Recommended for deployment on a cloud instance:

```bash
docker build -t yc-ws-bridge .

docker run --rm -p 8080:8080 \
  -e UPSTREAM_WS_URL="wss://backend.example/ws" \
  yc-ws-bridge
```

## Yandex Cloud setup

### Initialize CLI

```bash
yc init --no-browser
```

Create compute cloud instance in UI.

### Reserve a static IP

```bash
yc vpc address list
yc vpc address update --reserved=true <ip_id>
```

### Create API Gateway

```bash
yc serverless api-gateway create \
  --name <agw_name> \
  --execution-timeout 5 \
  --spec=openapi.yaml
```

Point the gateway at the bridge:

```bash
yc serverless api-gateway update \
  --id <api_gateway_id> \
  --variables backend_url="http://<cloud_instance_ip>:8080/ws"
```

### Service account and VM

Create a service account with the **`api-gateway.websocketWriter`** role in UI.

Attach it to the compute instance running the bridge:

```bash
yc compute instance list
yc compute instance update <vm_name> --service-account-name <service_account>
yc compute instance stop <vm_name>
yc compute instance start <vm_name>
```

The instance must be restarted for the service account binding to take effect.

## Reorder simulator

Local scripts that replay a logged WebSocket exchange using simple repeated-letter payloads:

| Role | Payload pattern |
|------|-----------------|
| Client requests | `A×1605`, `B×80`, `C×31`, `D×122`, `E×72` |
| Server success | `F×4012`, `G×543`, `H×44`, `I×1622`, `J×63` |
| Server protocol error | `X×24` |

Start the mock server:

```bash
python ws_sim_server.py
```

Client in **sort** mode — reproduces lexicographic reorder from the sample log:

```bash
python ws_sim_client.py --mode sort
```

Client in **arrival** mode — preserves pure arrival order; triggers disorder warning and `X×24` error:

```bash
python ws_sim_client.py --mode arrival
```

The simulator uses placeholder message IDs (`id-1`, `id-2`, …). **sort** orders by `message_id`; **arrival** ignores IDs and keeps arrival sequence.

## Project layout

| File | Purpose |
|------|---------|
| `bridge.py` | HTTP ↔ WebSocket bridge service |
| `openapi.yaml` | API Gateway WebSocket integration spec |
| `ws_sim_server.py` / `ws_sim_client.py` | Local reordering test harness |
| `Dockerfile` | Container image for the bridge |
