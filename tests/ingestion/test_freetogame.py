import pytest
import requests

from ingestion.api import freetogame


class _Resp:
    def __init__(self, status=200, payload=None):
        self.status_code = status
        self._payload = payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def json(self):
        return self._payload


def test_fetch_games_returns_list():
    ok = _Resp(payload=[{"id": 1, "title": "A"}])
    games = freetogame.fetch_games(url="http://x", timeout=1, http_get=lambda *a, **k: ok)
    assert games == [{"id": 1, "title": "A"}]


def test_fetch_games_retries_then_succeeds(monkeypatch):
    monkeypatch.setattr(freetogame.time, "sleep", lambda s: None)
    calls = {"n": 0}

    def flaky(*a, **k):
        calls["n"] += 1
        return _Resp(status=503) if calls["n"] < 3 else _Resp(payload=[{"id": 1, "title": "A"}])

    assert freetogame.fetch_games(url="http://x", timeout=1, http_get=flaky)
    assert calls["n"] == 3


def test_fetch_games_gives_up_after_attempts(monkeypatch):
    monkeypatch.setattr(freetogame.time, "sleep", lambda s: None)
    with pytest.raises(freetogame.GamesApiError):
        freetogame.fetch_games(url="http://x", timeout=1, attempts=2, http_get=lambda *a, **k: _Resp(status=500))


def test_fetch_games_rejects_non_list():
    with pytest.raises(freetogame.GamesApiError):
        bad = _Resp(payload={"status": 0})
        freetogame.fetch_games(url="http://x", timeout=1, attempts=1, http_get=lambda *a, **k: bad)


def test_validate_drops_records_missing_required_fields():
    valid, rejected = freetogame.validate([{"id": 1, "title": "A"}, {"id": 2}, {"title": "B"}, "junk"])
    assert valid == [{"id": 1, "title": "A"}]
    assert rejected == 3
