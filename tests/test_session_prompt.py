import os
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import Config

try:  # pragma: no cover - optional dependency for tests
    import telethon  # type: ignore  # noqa: F401
except ModuleNotFoundError:  # pragma: no cover - provide lightweight stubs
    telethon_module = types.ModuleType("telethon")
    telethon_module.__path__ = []  # mark as package

    class DummyTelegramClient:  # pragma: no cover - simple stand-in
        def __init__(self, *args, **kwargs):
            pass

    telethon_module.TelegramClient = DummyTelegramClient

    telethon_errors_module = types.ModuleType("telethon.errors")
    telethon_errors_module.__path__ = []

    class DummyRPCError(Exception):  # pragma: no cover - simple stand-in
        pass

    telethon_errors_module.RPCError = DummyRPCError

    telethon_tl_module = types.ModuleType("telethon.tl")
    telethon_tl_module.__path__ = []

    telethon_tl_types_module = types.ModuleType("telethon.tl.types")

    for name in (
        "Channel",
        "Chat",
        "MessageMediaDocument",
        "MessageMediaPhoto",
        "PeerChannel",
    ):
        setattr(telethon_tl_types_module, name, type(name, (), {}))

    telethon_tl_module.types = telethon_tl_types_module
    telethon_module.errors = telethon_errors_module
    telethon_module.tl = telethon_tl_module

    sys.modules["telethon"] = telethon_module
    sys.modules["telethon.errors"] = telethon_errors_module
    sys.modules["telethon.tl"] = telethon_tl_module
    sys.modules["telethon.tl.types"] = telethon_tl_types_module

from Scrape_Coordinates import prompt_session_name


def test_prompt_session_name_uses_existing_env(monkeypatch, tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text("")

    monkeypatch.setenv("TELEGRAM_SESSION_NAME", "geolocation_scraper")

    calls = []

    def record_set_key(path, key, value):
        calls.append((path, key, value))

    monkeypatch.setattr("Scrape_Coordinates.set_key", record_set_key)

    def fail_prompt(*args, **kwargs):
        raise AssertionError("prompt should not be invoked when session exists")

    monkeypatch.setattr("Scrape_Coordinates.prompt_with_smart_default", fail_prompt)

    config = Config(env_path)

    result = prompt_session_name("Enter the session name", config=config, env_path=env_path)

    assert result == "geolocation_scraper"
    assert os.environ["TELEGRAM_SESSION_NAME"] == "geolocation_scraper"
    assert calls == [(str(env_path), "TELEGRAM_SESSION_NAME", "geolocation_scraper")]


def test_prompt_session_name_prompts_and_saves(monkeypatch, tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text("")

    monkeypatch.delenv("TELEGRAM_SESSION_NAME", raising=False)

    captured = []

    def fake_prompt(*args, **kwargs):
        captured.append((args, kwargs))
        return "custom_session"

    monkeypatch.setattr("Scrape_Coordinates.prompt_with_smart_default", fake_prompt)

    writes = []

    def record_set_key(path, key, value):
        writes.append((path, key, value))

    monkeypatch.setattr("Scrape_Coordinates.set_key", record_set_key)

    config = Config(env_path)

    result = prompt_session_name("Enter the session name", config=config, env_path=env_path)

    assert captured, "prompt_with_smart_default should be called when no session exists"
    assert result == "custom_session"
    assert os.environ["TELEGRAM_SESSION_NAME"] == "custom_session"
    assert writes == [(str(env_path), "TELEGRAM_SESSION_NAME", "custom_session")]
