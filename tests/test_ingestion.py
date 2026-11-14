import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dlt_pipeline"))
import extract_load  # noqa: E402


def test_password_special_characters_are_escaped(monkeypatch):
    monkeypatch.setenv("PG_PASSWORD", "p@ss/word")
    url = extract_load.build_pg_url()
    assert "p%40ss%2Fword" in url          # '@' and '/' must not break the URL
    assert "p@ss/word" not in url


def test_missing_password_fails_loudly(monkeypatch):
    monkeypatch.delenv("PG_PASSWORD", raising=False)
    with pytest.raises(ValueError):
        extract_load.build_pg_url()
