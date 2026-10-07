import unittest
from unittest.mock import MagicMock, patch

from tap_google_sheets.discover import discover, check_stream_access
from tap_google_sheets.client import (
    GoogleUnauthorizedError,
    GoogleForbiddenError,
    GoogleNotFoundError,
    GoogleMethodNotAllowedError,
    GoogleBadRequestError,
)


# ---------------------------------------------------------------------------
# check_stream_access
# ---------------------------------------------------------------------------

class TestCheckStreamAccess(unittest.TestCase):

    def _client(self):
        return MagicMock()

    def test_returns_true_when_accessible(self):
        client = self._client()
        result = check_stream_access(client, 'spreadsheet123')
        self.assertTrue(result)
        client.get.assert_called_once()

    def test_probes_correct_path(self):
        client = self._client()
        check_stream_access(client, 'my_sheet_id')
        call_kwargs = client.get.call_args
        self.assertIn('my_sheet_id', call_kwargs.kwargs.get('path', ''))

    def test_returns_false_on_unauthorized(self):
        """401 is an authentication failure, not an access/not-found case;
        it must propagate unchanged, preserving the original status/message."""
        client = self._client()
        client.get.side_effect = GoogleUnauthorizedError('401 invalid credentials')
        with self.assertRaises(GoogleUnauthorizedError) as ctx:
            check_stream_access(client, 'spreadsheet123')
        self.assertEqual('401 invalid credentials', str(ctx.exception))

    def test_reraises_on_forbidden_with_context(self):
        """403 is a genuine access failure; re-raised with spreadsheet id and
        remediation guidance, while retaining the original HTTP message."""
        client = self._client()
        client.get.side_effect = GoogleForbiddenError('403 insufficient permission')
        with self.assertRaises(GoogleForbiddenError) as ctx:
            check_stream_access(client, 'spreadsheet123')
        message = str(ctx.exception)
        self.assertIn("spreadsheet123", message)
        self.assertIn('403 insufficient permission', message)

    def test_reraises_on_not_found_with_context(self):
        """404 means the spreadsheet doesn't exist or is inaccessible; it's a
        genuine not-found case, re-raised with context and original message retained."""
        client = self._client()
        client.get.side_effect = GoogleNotFoundError('404 not found')
        with self.assertRaises(GoogleNotFoundError) as ctx:
            check_stream_access(client, 'spreadsheet123')
        message = str(ctx.exception)
        self.assertIn("spreadsheet123", message)
        self.assertIn('404 not found', message)

    def test_returns_false_on_method_not_allowed(self):
        """405 indicates a client/contract issue, not a spreadsheet
        permissions problem; it must propagate unchanged."""
        client = self._client()
        client.get.side_effect = GoogleMethodNotAllowedError('405 method not allowed')
        with self.assertRaises(GoogleMethodNotAllowedError) as ctx:
            check_stream_access(client, 'spreadsheet123')
        self.assertEqual('405 method not allowed', str(ctx.exception))

    def test_reraises_on_non_auth_google_error(self):
        """Non-auth API errors aren't access-related; preserve the original failure."""
        client = self._client()
        client.get.side_effect = GoogleBadRequestError('400')
        with self.assertRaises(GoogleBadRequestError):
            check_stream_access(client, 'spreadsheet123')

    def test_reraises_non_google_errors(self):
        client = self._client()
        client.get.side_effect = ConnectionError('network timeout')
        with self.assertRaises(ConnectionError):
            check_stream_access(client, 'spreadsheet123')


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):

    def _client(self):
        return MagicMock()

    @patch(
        'tap_google_sheets.discover.check_stream_access',
        side_effect=GoogleForbiddenError(
            "Spreadsheet 'spreadsheet123' was not found or the credentials do not have "
            "access to it. Verify the spreadsheet ID and that the API credentials have "
            "the required permissions. HTTP-Error-Message: '403 insufficient permission'"
        ),
    )
    def test_inaccessible_spreadsheet_raises_exception(self, mock_check):
        with self.assertRaises(GoogleForbiddenError) as ctx:
            discover(self._client(), 'spreadsheet123')
        self.assertIn("Spreadsheet 'spreadsheet123' was not found or the credentials do not have access to it.", str(ctx.exception))
        self.assertIn('403 insufficient permission', str(ctx.exception))

    @patch(
        'tap_google_sheets.discover.check_stream_access',
        side_effect=GoogleUnauthorizedError('401 invalid credentials'),
    )
    def test_unauthorized_spreadsheet_propagates_unchanged(self, mock_check):
        """Authentication failures from check_stream_access must propagate
        unchanged through discover(), not be masked as a generic access error."""
        with self.assertRaises(GoogleUnauthorizedError) as ctx:
            discover(self._client(), 'spreadsheet123')
        self.assertEqual('401 invalid credentials', str(ctx.exception))

    @patch('tap_google_sheets.discover.check_stream_access', return_value=True)
    def test_accessible_spreadsheet_proceeds_to_schema_loading(self, mock_check):
        """When access is granted, discover() continues to call get_schemas()."""
        client = self._client()
        # SpreadSheetMetadata.get_schemas() will be called; mock it to avoid real API calls
        with patch('tap_google_sheets.streams.SpreadSheetMetadata.get_schemas') as mock_gs:
            mock_gs.return_value = ({}, {})
            with patch('tap_google_sheets.streams.SheetMetadata.get_schemas') as mock_sm:
                mock_sm.return_value = ({}, {})
                with patch('tap_google_sheets.streams.SheetsLoaded.get_schemas') as mock_sl:
                    mock_sl.return_value = ({}, {})
                    from singer.catalog import Catalog
                    result = discover(client, 'spreadsheet123')
                    self.assertIsInstance(result, Catalog)
        mock_check.assert_called_once_with(client, 'spreadsheet123')


if __name__ == '__main__':
    unittest.main()
