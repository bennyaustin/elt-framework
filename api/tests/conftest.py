"""Shared pytest fixtures: sets required env vars before the app config is loaded and
provides a TestClient with authentication disabled."""
import os

os.environ.setdefault("SQL_SERVER", "localhost")
os.environ.setdefault("SQL_DATABASE", "controlDB")
os.environ.setdefault("API_AAD_TENANT_ID", "00000000-0000-0000-0000-000000000000")
os.environ.setdefault("API_AAD_AUDIENCE", "api://test")
os.environ.setdefault("AUTH_DISABLED", "true")

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture()
def client() -> TestClient:
    return TestClient(app)
