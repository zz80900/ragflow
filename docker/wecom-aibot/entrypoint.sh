#!/usr/bin/env bash

set -e

CONF_DIR="/ragflow/conf"
TEMPLATE_FILE="${CONF_DIR}/service_conf.yaml.template"
CONF_FILE="${CONF_DIR}/service_conf.yaml"

rm -f "${CONF_FILE}"
DEF_ENV_VALUE_PATTERN="\$\{([^:]+):-([^}]+)\}"
while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" =~ $DEF_ENV_VALUE_PATTERN ]]; then
        varname="${BASH_REMATCH[1]}"

        if [ -n "${!varname}" ]; then
            eval "echo \"$line\"" >> "${CONF_FILE}"
        else
            echo "$line" | sed -E "s/\\\$\{[^:]+:-([^}]+)\}/\1/g" >> "${CONF_FILE}"
        fi
    else
        eval "echo \"$line\"" >> "${CONF_FILE}"
    fi
done < "${TEMPLATE_FILE}"

export PYTHONPATH=/ragflow
exec python3 /ragflow/api/wecom_aibot_runner.py "$@"
