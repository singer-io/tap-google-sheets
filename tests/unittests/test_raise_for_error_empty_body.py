import unittest
import requests

from tap_google_sheets.client import (
    raise_for_error,
    GoogleError,
    GoogleUnauthorizedError,
    GoogleForbiddenError,
    GoogleNotFoundError,
    GoogleMethodNotAllowedError,
)


class MockResponse:
    """Minimal stand-in for a `requests.Response` with a configurable body."""

    def __init__(self, status_code, content=b""):
        self.status_code = status_code
        self.content = content
        self.reason = "error"

    def raise_for_status(self):
        raise requests.exceptions.HTTPError("mock http error")

    def json(self):
        # An empty body cannot be parsed as JSON; mimic that failure so tests
        # exercise the same code path a real empty-body response would hit.
        if not self.content:
            raise ValueError("No JSON object could be decoded")
        import json
        return json.loads(self.content)


class TestRaiseForErrorEmptyBody(unittest.TestCase):
    """Regression tests: an empty response body must not suppress the
    status-code-mapped exception. Previously, `raise_for_error` returned
    silently for any non-2xx response with an empty body, which let callers
    (e.g. `discover.check_stream_access`) treat a denied/failed request as a
    success.
    """

    def test_empty_body_401_raises_unauthorized(self):
        with self.assertRaises(GoogleUnauthorizedError):
            raise_for_error(MockResponse(401, content=b""))

    def test_empty_body_403_raises_forbidden(self):
        with self.assertRaises(GoogleForbiddenError):
            raise_for_error(MockResponse(403, content=b""))

    def test_empty_body_404_raises_not_found(self):
        with self.assertRaises(GoogleNotFoundError):
            raise_for_error(MockResponse(404, content=b""))

    def test_empty_body_405_raises_method_not_allowed(self):
        with self.assertRaises(GoogleMethodNotAllowedError):
            raise_for_error(MockResponse(405, content=b""))

    def test_empty_body_unmapped_status_raises_google_error(self):
        """Status codes with no specific mapping still raise the generic base error."""
        with self.assertRaises(GoogleError):
            raise_for_error(MockResponse(418, content=b""))

    def test_non_empty_but_unparsable_body_still_raises_mapped_error(self):
        """A non-empty body that isn't valid JSON must not suppress the error either."""
        with self.assertRaises(GoogleUnauthorizedError):
            raise_for_error(MockResponse(401, content=b"not json"))


if __name__ == '__main__':
    unittest.main()
