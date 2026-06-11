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
        client = self._client()
        client.get.side_effect = GoogleUnauthorizedError('401')
        result = check_stream_access(client, 'spreadsheet123')
        self.assertFalse(result)

    def test_returns_false_on_forbidden(self):
        client = self._client()
        client.get.side_effect = GoogleForbiddenError('403')
        result = check_stream_access(client, 'spreadsheet123')
        self.assertFalse(result)

    def test_returns_false_on_not_found(self):
        """404 means the spreadsheet doesn't exist or is inaccessible."""
        client = self._client()
        client.get.side_effect = GoogleNotFoundError('404')
        result = check_stream_access(client, 'spreadsheet123')
        self.assertFalse(result)

    def test_returns_false_on_method_not_allowed(self):
        client = self._client()
        client.get.side_effect = GoogleMethodNotAllowedError('405')
        result = check_stream_access(client, 'spreadsheet123')
        self.assertFalse(result)

    def test_returns_true_on_non_auth_google_error(self):
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

    @patch('tap_google_sheets.discover.check_stream_access', return_value=False)
    def test_inaccessible_spreadsheet_raises_exception(self, mock_check):
        with self.assertRaises(Exception) as ctx:
            discover(self._client(), 'spreadsheet123')
        self.assertIn('Spreadsheet 'spreadsheet123' was not found or the credentials do not have access to it.', str(ctx.exception))

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
