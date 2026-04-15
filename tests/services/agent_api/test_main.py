"""Tests for services/agent_api/main.py.

The main module performs heavy initialization at import time (BigQuery client,
Anthropic client, prompt file reads). We mock external dependencies before
importing to avoid requiring credentials or network access.
"""

import os
import unittest.mock as mock

# Mock external dependencies before the module-level code in main.py runs.
# BigQueryClient.__init__ calls google.cloud.bigquery.Client which needs credentials.
# anthropic.Anthropic() needs ANTHROPIC_API_KEY.
with mock.patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key", "GCP_PROJECT_ID": "test-project"}):
    with mock.patch("google.cloud.bigquery.Client"):
        with mock.patch("anthropic.Anthropic"):
            from services.agent_api.main import app
            from services.agent_api.tools import _qualify_tables

from fastapi.testclient import TestClient


client = TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_ok(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["service"] == "agent_api"


class TestQualifyTables:
    def test_replaces_cur_dataset(self):
        sql = "SELECT * FROM `cur.fact_observation`"
        result = _qualify_tables(sql, "test-project")
        assert "`test-project.cur.fact_observation`" in result

    def test_replaces_quality_dataset(self):
        sql = "SELECT * FROM `quality.observation_quality`"
        result = _qualify_tables(sql, "test-project")
        assert "`test-project.quality.observation_quality`" in result

    def test_replaces_both_datasets(self):
        sql = (
            "SELECT * FROM `cur.fact_observation` f "
            "JOIN `quality.observation_quality` q ON f.observation_id = q.observation_id"
        )
        result = _qualify_tables(sql, "test-project")
        assert "`test-project.cur.fact_observation`" in result
        assert "`test-project.quality.observation_quality`" in result

    def test_does_not_alter_other_refs(self):
        sql = "SELECT * FROM `cur.fact_observation` WHERE metric_id = 'abc'"
        result = _qualify_tables(sql, "test-project")
        assert "metric_id = 'abc'" in result

    def test_no_datasets_unchanged(self):
        sql = "SELECT 1"
        result = _qualify_tables(sql, "test-project")
        assert result == "SELECT 1"
