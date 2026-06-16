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

import base64

from common import settings
from common.crypto_utils import CryptoUtil


MASKED_SECRET = "********"


def _crypto() -> CryptoUtil:
    return CryptoUtil(key=settings.get_secret_key())


def encrypt_secret(secret: str) -> str:
    if not secret:
        return ""
    encrypted = _crypto().encrypt(secret.encode("utf-8"))
    return base64.b64encode(encrypted).decode("utf-8")


def decrypt_secret(secret_ciphertext: str) -> str:
    if not secret_ciphertext:
        return ""
    encrypted = base64.b64decode(secret_ciphertext.encode("utf-8"))
    return _crypto().decrypt(encrypted).decode("utf-8")


def mask_secret(secret_ciphertext: str | None) -> str:
    return MASKED_SECRET if secret_ciphertext else ""
