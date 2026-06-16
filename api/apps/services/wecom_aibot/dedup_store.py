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

import logging

from rag.utils.redis_conn import REDIS_CONN


class WeComAIBotDedupStore:
    def __init__(self, ttl_seconds: int):
        self.ttl_seconds = ttl_seconds

    def acquire(self, msgid: str) -> bool:
        if not msgid:
            return True
        if not REDIS_CONN.is_alive():
            logging.error("WeCom AIBot dedup store requires Redis.")
            return False
        try:
            return bool(REDIS_CONN.REDIS.set(f"wecom:aibot:msg:{msgid}", "processing", ex=self.ttl_seconds, nx=True))
        except Exception:
            logging.exception("WeCom AIBot dedup acquire failed.")
            return False
