# WeCom AIBot Docker Deployment

This document describes how to build and deploy the custom RAGFlow image that
contains the Enterprise WeChat AIBot runner and media support.

## Overview

The WeCom AIBot integration has two runtime parts:

- The normal RAGFlow web/API service, which owns the management page, REST API,
  database model, media public route, and test-message endpoint.
- The `ragflow-wecom-aibot` runner, which keeps the Enterprise WeChat AIBot
  long WebSocket connection outside the web process.

Deploy both parts with the same custom image. The compose overlay intentionally
overrides `ragflow-cpu`, `ragflow-gpu`, and `ragflow-wecom-aibot` to
`RAGFLOW_CUSTOM_IMAGE` so the API, database model, media modules, and runner
code stay on the same build.

The runner uses Redis for the single-connection lock, message de-duplication,
session mapping, and temporary media cache. If Redis is unavailable, the runner
fails closed and does not process messages.

## Prerequisites

- Docker 24 or later and Docker Compose v2.26.1 or later.
- A RAGFlow base image tag that matches the deployment you are extending.
- Access to MySQL, Redis, MinIO, and the selected document engine used by the
  main RAGFlow compose stack.
- A WeCom AIBot binding configured from the agent management page or REST API.
  Do not place BotID or Secret in Docker env files.
- A stable Docker/WSL session for live long-connection testing. If containers
  repeatedly restart or show very small uptimes, fix Docker/WSL stability before
  running live media tests.

On Windows WSL, run Docker commands from the Linux path:

```bash
cd /mnt/c/Users/xiejincheng/Desktop/demo_ragflow/ragflow
```

## Build The Image

Choose one image strategy.

### Full RAGFlow Image

Use this for production when frontend or full application assets changed.

Run from the repository root:

```bash
docker build --platform linux/amd64 -t ragflow-wecom-aibot:local .
```

### Lightweight Overlay Image

Use this for backend and runner smoke deployment when the base RAGFlow image is
already available and only the WeCom AIBot backend code needs to be overlaid.

Run from the repository root:

```bash
docker build \
  -f docker/wecom-aibot/Dockerfile \
  --build-arg RAGFLOW_BASE_IMAGE=infiniflow/ragflow:v0.26.0 \
  -t ragflow-wecom-aibot:local \
  .
```

Set `RAGFLOW_BASE_IMAGE` to the exact image tag used by your deployment. The
overlay Dockerfile installs the WebSocket dependency and copies only the WeCom
AIBot REST API, service modules, runner, database model, and runner entrypoint.
It does not rebuild frontend assets.

## Build Private Release Images

The release workflow builds the WeCom AIBot image in two modes:

- Official release tags build and push `infiniflow/ragflow-wecom-aibot:<tag>`
  next to the normal `infiniflow/ragflow:<tag>` image.
- The `customize` branch builds private custom images in GitHub Container
  Registry:
  - `ghcr.io/<owner>/ragflow:<tag>`
  - `ghcr.io/<owner>/ragflow-wecom-aibot:<tag>`

The private build first builds the full RAGFlow image from the current branch,
then builds the WeCom AIBot runner image from that exact image. This keeps the
frontend, API, database model, service modules, and runner code aligned.

Trigger a private build from GitHub CLI:

```bash
gh workflow run release.yml \
  --repo <owner>/ragflow \
  --ref customize \
  -f private_image_tag=customize
```

If the workflow is not available on the default branch yet, push to the
`customize` branch; the branch push also triggers the private image build.

Use the runner image as the compose custom image:

```bash
export RAGFLOW_CUSTOM_IMAGE=ghcr.io/<owner>/ragflow-wecom-aibot:customize
```

## Configure Compose

Copy the WeCom AIBot environment template:

```bash
cp docker/.env.wecom-aibot.example docker/.env.wecom-aibot
```

Expose the custom image name for compose interpolation:

```bash
export RAGFLOW_CUSTOM_IMAGE=ragflow-wecom-aibot:local
```

You can also add this value to `docker/.env`:

```env
RAGFLOW_CUSTOM_IMAGE=ragflow-wecom-aibot:local
```

Edit `docker/.env.wecom-aibot`:

```env
WECOM_AIBOT_ENABLED=true
WECOM_AIBOT_WS_URL=wss://openws.work.weixin.qq.com
WECOM_AIBOT_STREAM_INTERVAL_MS=2000
WECOM_AIBOT_CONVERSATION_INTERVAL_MS=2000
WECOM_AIBOT_MAX_STREAM_SECONDS=600
WECOM_AIBOT_HEARTBEAT_SECONDS=30
WECOM_AIBOT_LOCK_TTL_SECONDS=60
WECOM_AIBOT_WELCOME_MESSAGE=Hello, I am the assistant.
WECOM_AIBOT_PUBLIC_BASE_URL=
WECOM_AIBOT_MEDIA_PUBLIC_URL_TTL_SECONDS=300
WECOM_AIBOT_MEDIA_MAX_DOWNLOAD_BYTES=20971520
WECOM_AIBOT_MEDIA_DOWNLOAD_TIMEOUT_SECONDS=10
WECOM_AIBOT_MEDIA_ALLOWED_TYPES=image/png,image/jpeg,image/gif,image/webp,application/pdf,text/plain,application/msword,application/vnd.openxmlformats-officedocument.wordprocessingml.document,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-powerpoint,application/vnd.openxmlformats-officedocument.presentationml.presentation,application/zip,application/octet-stream
WECOM_AIBOT_MEDIA_REPLY_MODE=auto
WECOM_AIBOT_MEDIA_TEMP_CACHE_SECONDS=259200
WECOM_AIBOT_MEDIA_PUBLIC_TOKEN_SECRET=
```

Important media settings:

- `WECOM_AIBOT_ENABLED`: set to `true` only when the runner should connect.
- `WECOM_AIBOT_PUBLIC_BASE_URL`: HTTPS base URL that Enterprise WeChat can
  reach. Leave it empty when no public route is available.
- `WECOM_AIBOT_MEDIA_REPLY_MODE`: `auto`, `public_url`, or `upload`. `auto`
  tries public URL delivery first and falls back to temporary media upload.
- `WECOM_AIBOT_MEDIA_PUBLIC_TOKEN_SECRET`: optional signing secret for the
  WeCom-specific public media route. Use a stable random value in production.
- `WECOM_AIBOT_MEDIA_MAX_DOWNLOAD_BYTES`,
  `WECOM_AIBOT_MEDIA_DOWNLOAD_TIMEOUT_SECONDS`, and
  `WECOM_AIBOT_MEDIA_ALLOWED_TYPES`: inbound media safety limits.

Configure BotID and Secret through the agent management page or binding API.
They are encrypted in RAGFlow storage and must not be logged or committed.

## Start Services

Run compose commands from `ragflow/docker`.

CPU deployment:

```bash
cd docker
export RAGFLOW_CUSTOM_IMAGE=ragflow-wecom-aibot:local
docker compose \
  -f docker-compose.yml \
  -f docker-compose-wecom-aibot.yml \
  --profile elasticsearch \
  --profile cpu \
  --profile wecom-aibot \
  up -d mysql redis minio es01 ragflow-cpu ragflow-wecom-aibot
```

GPU deployment:

```bash
cd docker
export RAGFLOW_CUSTOM_IMAGE=ragflow-wecom-aibot:local
docker compose \
  -f docker-compose.yml \
  -f docker-compose-wecom-aibot.yml \
  --profile elasticsearch \
  --profile gpu \
  --profile wecom-aibot \
  up -d mysql redis minio es01 ragflow-gpu ragflow-wecom-aibot
```

If your deployment uses Infinity or another document engine instead of
Elasticsearch, keep the same engine profile and service list used by your main
RAGFlow deployment.

## Verify Deployment

Check container state:

```bash
docker compose -f docker-compose.yml -f docker-compose-wecom-aibot.yml ps
```

Check that both the API service and runner use the custom image:

```bash
docker inspect \
  "$(docker compose -f docker-compose.yml -f docker-compose-wecom-aibot.yml ps -q ragflow-cpu)" \
  --format '{{.Config.Image}}'
docker inspect \
  "$(docker compose -f docker-compose.yml -f docker-compose-wecom-aibot.yml ps -q ragflow-wecom-aibot)" \
  --format '{{.Config.Image}}'
```

For GPU deployments, inspect the `ragflow-gpu` service instead of
`ragflow-cpu`.

Check the RAGFlow API:

```bash
curl http://127.0.0.1:9380/api/v1/system/version
```

Check runner startup logs:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose-wecom-aibot.yml \
  logs -f ragflow-wecom-aibot
```

Run a one-shot runner startup check without opening a long-lived connection:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose-wecom-aibot.yml \
  run --rm ragflow-wecom-aibot --once
```

Then use the agent management page to run the WeCom connection test and local
`test-message` media simulations. Do not mark live media verification complete
until an actual Enterprise WeChat robot has sent image and file input and has
received public URL, temporary media upload, and failure fallback replies.

## Rebuild And Upgrade

After code changes, rebuild the image:

```bash
cd /path/to/ragflow
docker build \
  -f docker/wecom-aibot/Dockerfile \
  --build-arg RAGFLOW_BASE_IMAGE=infiniflow/ragflow:v0.26.0 \
  -t ragflow-wecom-aibot:local \
  .
```

Recreate both the main API service and the runner:

```bash
cd docker
export RAGFLOW_CUSTOM_IMAGE=ragflow-wecom-aibot:local
docker compose \
  -f docker-compose.yml \
  -f docker-compose-wecom-aibot.yml \
  --profile elasticsearch \
  --profile cpu \
  --profile wecom-aibot \
  up -d --force-recreate ragflow-cpu ragflow-wecom-aibot
```

Use the matching GPU service name when deploying with the GPU profile.

Do not run `docker compose down -v` unless you intentionally want to delete
compose-managed data volumes.

## Troubleshooting

- Compose fails with `RAGFLOW_CUSTOM_IMAGE` missing: export the variable or add
  it to `docker/.env`.
- Runner starts but does not connect: confirm `WECOM_AIBOT_ENABLED=true` and an
  enabled WeCom AIBot binding exists for the target agent.
- Connection test fails: verify BotID and Secret in the management page. Do not
  paste secrets into logs or issue reports.
- Duplicate connection or skipped messages: stop extra runner instances. One
  runner per bot is expected; Redis lock protects against accidental overlap.
- Dependencies are not ready: wait for MySQL, Redis, MinIO, and the document
  engine to become healthy before starting the runner.
- Public URL images do not render in Enterprise WeChat: set
  `WECOM_AIBOT_PUBLIC_BASE_URL` to a reachable HTTPS URL or use `upload` mode.
  Internal Docker or MinIO hostnames are usually not reachable by Enterprise
  WeChat clients.
- Temporary media upload replies do not arrive: confirm the deployed image is
  rebuilt and containers were recreated. Older runner code may not route upload
  responses back to the pending upload request.
- Containers keep showing very small uptimes on WSL: treat this as a Docker/WSL
  stability issue before live testing. Prefer compose-managed background
  services and avoid repeated long foreground debug sessions until the host is
  stable.
- A Bot Secret was exposed: rotate it in Enterprise WeChat and update the
  binding through the management page.

## Local Verification Commands

These commands do not require a live Enterprise WeChat callback:

```bash
python -m compileall -q \
  api/apps/services/wecom_aibot \
  api/apps/restful_apis/agent_wecom_api.py \
  api/wecom_aibot_runner.py \
  api/db/db_models.py
```

```bash
python -m pytest test/unit_test/api/apps/services/wecom_aibot -q
```

The management page `test-message` endpoint can simulate text, image, and file
callbacks and returns stream frames, final reply, stored media references,
public URLs, uploaded `media_id` values, rejected media reasons, and image URLs
for local debugging.

## Production Notes

- Expose only the RAGFlow HTTP/API endpoint required by your deployment. The
  runner only needs outbound WebSocket access to Enterprise WeChat.
- Keep media URL TTLs short and use HTTPS for every URL Enterprise WeChat must
  fetch.
- Keep temporary media cache TTL no longer than the platform media validity
  window.
- Build and recreate the API service and runner together to avoid schema or
  protocol mismatches.
