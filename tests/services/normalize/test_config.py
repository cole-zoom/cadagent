"""Tests for services/normalize/config.py."""

from services.normalize.config import NormalizeConfig


class TestNormalizeConfig:
    def test_default_min_classification_confidence(self):
        cfg = NormalizeConfig()
        assert cfg.min_classification_confidence == 0.5

    def test_default_auto_approve_threshold(self):
        cfg = NormalizeConfig()
        assert cfg.auto_approve_threshold == 0.8

    def test_default_batch_size(self):
        cfg = NormalizeConfig()
        assert cfg.batch_size == 100

    def test_default_max_observations_per_table(self):
        cfg = NormalizeConfig()
        assert cfg.max_observations_per_table == 50000

    def test_custom_min_classification_confidence(self):
        cfg = NormalizeConfig(min_classification_confidence=0.7)
        assert cfg.min_classification_confidence == 0.7

    def test_custom_auto_approve_threshold(self):
        cfg = NormalizeConfig(auto_approve_threshold=0.9)
        assert cfg.auto_approve_threshold == 0.9

    def test_custom_batch_size(self):
        cfg = NormalizeConfig(batch_size=200)
        assert cfg.batch_size == 200

    def test_custom_max_observations_per_table(self):
        cfg = NormalizeConfig(max_observations_per_table=10000)
        assert cfg.max_observations_per_table == 10000

    def test_all_custom_values(self):
        cfg = NormalizeConfig(
            min_classification_confidence=0.6,
            auto_approve_threshold=0.95,
            batch_size=50,
            max_observations_per_table=25000,
        )
        assert cfg.min_classification_confidence == 0.6
        assert cfg.auto_approve_threshold == 0.95
        assert cfg.batch_size == 50
        assert cfg.max_observations_per_table == 25000
