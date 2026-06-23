#
#  Copyright 2026 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import argparse
import asyncio
import logging
import signal
import sys
import types
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _prepare_service_package_imports() -> None:
    # Avoid importing api.apps.__init__, which builds the REST app and initializes doc storage.
    if "api.apps" in sys.modules:
        return
    api_apps = types.ModuleType("api.apps")
    api_apps.__path__ = [str(Path(__file__).resolve().parent / "apps")]
    sys.modules["api.apps"] = api_apps


_prepare_service_package_imports()

from api.apps.services.wecom_aibot.config import WeComAIBotConfig
from common.log_utils import init_root_logger


def _count_enabled_bindings_for_startup_check() -> int | None:
    try:
        from api.apps.services.wecom_aibot.binding_store import WeComAIBotBindingService

        return len(WeComAIBotBindingService.list_enabled(include_secret=False))
    except Exception as exc:
        logging.warning("WeCom AIBot binding lookup skipped during startup check: %s", exc)
        return None


async def _main() -> None:
    parser = argparse.ArgumentParser(description="Run the RAGFlow WeCom AIBot WebSocket adapter.")
    parser.add_argument("--once", action="store_true", help="Initialize settings and list enabled bindings, then exit.")
    args = parser.parse_args()

    init_root_logger("wecom_aibot_runner")
    config = WeComAIBotConfig.from_env()

    if args.once:
        binding_count = _count_enabled_bindings_for_startup_check()
        logging.info(
            "WeCom AIBot runner initialized. enabled=%s ws_url=%s enabled_bindings=%s",
            config.enabled,
            config.ws_url,
            "unknown" if binding_count is None else binding_count,
        )
        return

    from api.apps.services.wecom_aibot.service import WeComAIBotService
    from api.db.db_models import init_database_tables
    from common import settings

    settings.init_settings()
    init_database_tables()
    service = WeComAIBotService(config=config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, service.stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: service.stop())

    await service.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
