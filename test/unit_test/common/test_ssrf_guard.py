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
import socket

import pytest

from common.ssrf_guard import assert_host_is_safe, assert_url_is_safe


def _addrinfo(ip_str: str) -> list:
    family = socket.AF_INET6 if ":" in ip_str else socket.AF_INET
    return [(family, socket.SOCK_STREAM, 6, "", (ip_str, 0))]


def test_assert_url_is_safe_blocks_private_address_by_default(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", lambda _h, _p: _addrinfo("10.18.6.223"))

    with pytest.raises(ValueError, match="non-public"):
        assert_url_is_safe("https://outline-ssc.xgd.com/mcp")


def test_assert_url_is_safe_allows_private_address_for_configured_host(monkeypatch):
    monkeypatch.setenv("RAGFLOW_SSRF_ALLOWED_PRIVATE_HOSTS", "outline-ssc.xgd.com")
    monkeypatch.setattr(socket, "getaddrinfo", lambda _h, _p: _addrinfo("10.18.6.223"))

    hostname, resolved_ip = assert_url_is_safe("https://outline-ssc.xgd.com/mcp")

    assert hostname == "outline-ssc.xgd.com"
    assert resolved_ip == "10.18.6.223"


def test_assert_url_is_safe_allows_private_address_for_configured_cidr(monkeypatch):
    monkeypatch.setenv("RAGFLOW_SSRF_ALLOWED_PRIVATE_CIDRS", "10.18.6.0/24")
    monkeypatch.setattr(socket, "getaddrinfo", lambda _h, _p: _addrinfo("10.18.6.223"))

    hostname, resolved_ip = assert_url_is_safe("https://outline-ssc.xgd.com/mcp")

    assert hostname == "outline-ssc.xgd.com"
    assert resolved_ip == "10.18.6.223"


def test_assert_url_is_safe_keeps_loopback_blocked_even_when_host_allowed(monkeypatch):
    monkeypatch.setenv("RAGFLOW_SSRF_ALLOWED_PRIVATE_HOSTS", "outline-ssc.xgd.com")
    monkeypatch.setattr(socket, "getaddrinfo", lambda _h, _p: _addrinfo("127.0.0.1"))

    with pytest.raises(ValueError, match="non-public"):
        assert_url_is_safe("https://outline-ssc.xgd.com/mcp")


def test_assert_host_is_safe_uses_private_allowlist(monkeypatch):
    monkeypatch.setenv("RAGFLOW_SSRF_ALLOWED_PRIVATE_HOSTS", "database.internal")
    monkeypatch.setattr(socket, "getaddrinfo", lambda _h, _p: _addrinfo("10.18.6.10"))

    assert assert_host_is_safe("database.internal") == "10.18.6.10"
