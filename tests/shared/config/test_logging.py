"""Tests for shared/config/logging.py."""

import json
import logging

import pytest

from shared.config.logging import JsonFormatter, configure_logging


class TestJsonFormatter:
    def test_format_returns_valid_json(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert isinstance(parsed, dict)

    def test_format_has_required_keys(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.WARNING,
            pathname="test.py",
            lineno=10,
            msg="Warning message",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert "timestamp" in parsed
        assert "level" in parsed
        assert "logger" in parsed
        assert "message" in parsed

    def test_format_level_value(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="my_logger",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="Error occurred",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "ERROR"

    def test_format_logger_name(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="my.custom.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="hello",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["logger"] == "my.custom.logger"

    def test_format_message(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Hello %s",
            args=("world",),
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["message"] == "Hello world"

    def test_format_with_exception(self):
        formatter = JsonFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            import sys
            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="An error",
            args=(),
            exc_info=exc_info,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert "exception" in parsed
        assert "ValueError" in parsed["exception"]

    def test_format_without_exception(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="No error",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        parsed = json.loads(output)
        assert "exception" not in parsed


class TestConfigureLogging:
    def test_sets_root_log_level(self):
        configure_logging(level="DEBUG", service="test-service")
        root = logging.getLogger()
        assert root.level == logging.DEBUG

    def test_sets_service_log_level(self):
        configure_logging(level="WARNING", service="my-service")
        service_logger = logging.getLogger("my-service")
        assert service_logger.level == logging.WARNING

    def test_default_level_info(self):
        configure_logging(service="test")
        root = logging.getLogger()
        assert root.level == logging.INFO

    def test_adds_handler(self):
        configure_logging(level="INFO", service="test")
        root = logging.getLogger()
        assert len(root.handlers) > 0

    def test_handler_uses_json_formatter(self):
        configure_logging(level="INFO", service="test")
        root = logging.getLogger()
        handler = root.handlers[0]
        assert isinstance(handler.formatter, JsonFormatter)

    def test_clears_previous_handlers(self):
        root = logging.getLogger()
        root.addHandler(logging.StreamHandler())
        root.addHandler(logging.StreamHandler())
        initial_count = len(root.handlers)
        configure_logging(level="INFO", service="test")
        # Should have exactly 1 handler after configure_logging
        assert len(root.handlers) == 1
