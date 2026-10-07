import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tap_google_sheets import streams


class DummyMessage:
    def __init__(self, payload):
        self._payload = payload

    def asdict(self):
        return self._payload


class TestStreamsEncoding(unittest.TestCase):
    def test_new_format_message_falls_back_for_cp1252_incompatible_chars(self):
        """Falls back to ASCII-escaped JSON when stdout encoding cannot encode payload."""
        message = DummyMessage({"value": "bad\u0080char"})

        with patch("sys.stdout", new=SimpleNamespace(encoding="cp1252")):
            formatted = streams.new_format_message(message, ensure_ascii=False)

        self.assertIn("\\u0080", formatted)

    def test_new_format_message_keeps_unicode_when_stdout_supports_it(self):
        """Keeps unicode characters when stdout encoding supports output."""
        message = DummyMessage({"value": "hello\u00f1"})

        with patch("sys.stdout", new=SimpleNamespace(encoding="utf-8")):
            formatted = streams.new_format_message(message, ensure_ascii=False)

        self.assertIn("ñ", formatted)

    def test_new_format_message_respects_explicit_ensure_ascii(self):
        """Always ASCII-escapes when ensure_ascii is explicitly requested."""
        message = DummyMessage({"value": "hello\u00f1"})

        with patch("sys.stdout", new=SimpleNamespace(encoding="utf-8")):
            formatted = streams.new_format_message(message, ensure_ascii=True)

        self.assertIn("\\u00f1", formatted)
