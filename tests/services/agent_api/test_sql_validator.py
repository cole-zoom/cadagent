"""Tests for services/agent_api/sql_validator.py."""

import pytest

from services.agent_api.sql_validator import SQLValidationError, validate_sql


class TestValidateSQL:
    def test_valid_select(self):
        sql = "SELECT * FROM `cur.fact_observation` LIMIT 10"
        result = validate_sql(sql, "test-project")
        assert result == sql

    def test_reject_insert(self):
        with pytest.raises(SQLValidationError):
            validate_sql("INSERT INTO `cur.fact_observation` VALUES ('a')", "test-project")

    def test_reject_update(self):
        with pytest.raises(SQLValidationError):
            validate_sql("UPDATE `cur.fact_observation` SET metric_id='x'", "test-project")

    def test_reject_delete(self):
        with pytest.raises(SQLValidationError):
            validate_sql("DELETE FROM `cur.fact_observation`", "test-project")

    def test_reject_drop(self):
        with pytest.raises(SQLValidationError):
            validate_sql("DROP TABLE `cur.fact_observation`", "test-project")

    def test_reject_non_select(self):
        with pytest.raises(SQLValidationError, match="Only SELECT"):
            validate_sql("SHOW TABLES", "test-project")

    def test_reject_raw_dataset(self):
        with pytest.raises(SQLValidationError, match="non-curated dataset"):
            validate_sql("SELECT * FROM `raw.documents` LIMIT 10", "test-project")

    def test_reject_stg_dataset(self):
        with pytest.raises(SQLValidationError, match="non-curated dataset"):
            validate_sql("SELECT * FROM `stg.headers` LIMIT 10", "test-project")

    def test_allow_cur_dataset(self):
        sql = "SELECT * FROM `cur.dim_metric` LIMIT 10"
        result = validate_sql(sql, "test-project")
        assert "cur.dim_metric" in result

    def test_allow_quality_dataset(self):
        sql = "SELECT * FROM `quality.observation_quality` LIMIT 10"
        result = validate_sql(sql, "test-project")
        assert "quality.observation_quality" in result

    def test_add_limit_when_missing(self):
        sql = "SELECT * FROM `cur.fact_observation`"
        result = validate_sql(sql, "test-project")
        assert "LIMIT 1000" in result

    def test_cap_excessive_limit(self):
        sql = "SELECT * FROM `cur.fact_observation` LIMIT 5000"
        result = validate_sql(sql, "test-project")
        assert "LIMIT 1000" in result
        assert "LIMIT 5000" not in result

    def test_preserve_reasonable_limit(self):
        sql = "SELECT * FROM `cur.fact_observation` LIMIT 50"
        result = validate_sql(sql, "test-project")
        assert "LIMIT 50" in result

    def test_reject_create(self):
        with pytest.raises(SQLValidationError):
            validate_sql("SELECT 1; CREATE TABLE `cur.evil` (a INT)", "test-project")

    def test_reject_alter(self):
        with pytest.raises(SQLValidationError):
            validate_sql("SELECT 1; ALTER TABLE `cur.fact_observation` ADD COLUMN x INT", "test-project")

    def test_reject_truncate(self):
        with pytest.raises(SQLValidationError):
            validate_sql("SELECT 1; TRUNCATE TABLE `cur.fact_observation`", "test-project")

    def test_reject_merge(self):
        with pytest.raises(SQLValidationError):
            validate_sql("SELECT 1; MERGE INTO `cur.fact_observation` USING src ON TRUE", "test-project")

    def test_reject_grant(self):
        with pytest.raises(SQLValidationError):
            validate_sql("SELECT 1; GRANT SELECT ON `cur.fact_observation` TO user", "test-project")

    def test_reject_revoke(self):
        with pytest.raises(SQLValidationError):
            validate_sql("SELECT 1; REVOKE SELECT ON `cur.fact_observation` FROM user", "test-project")

    def test_case_insensitive_select(self):
        sql = "select * from `cur.fact_observation` limit 10"
        result = validate_sql(sql, "test-project")
        assert result == sql

    def test_semicolon_stripped_before_limit(self):
        sql = "SELECT * FROM `cur.fact_observation`;"
        result = validate_sql(sql, "test-project")
        assert "LIMIT 1000" in result
        assert not result.endswith(";LIMIT")
