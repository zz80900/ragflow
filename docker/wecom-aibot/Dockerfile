ARG RAGFLOW_BASE_IMAGE=infiniflow/ragflow:v0.26.0

FROM ${RAGFLOW_BASE_IMAGE}

USER root
WORKDIR /ragflow

RUN python3 -m pip install --no-cache-dir "websockets>=16.0,<17.0"

COPY api/apps/restful_apis/agent_wecom_api.py /ragflow/api/apps/restful_apis/agent_wecom_api.py
COPY api/apps/services/wecom_aibot /ragflow/api/apps/services/wecom_aibot
COPY api/wecom_aibot_runner.py /ragflow/api/wecom_aibot_runner.py
COPY api/db/db_models.py /ragflow/api/db/db_models.py
COPY docker/wecom-aibot/entrypoint.sh /ragflow/docker/wecom-aibot/entrypoint.sh

RUN chmod +x /ragflow/docker/wecom-aibot/entrypoint.sh
