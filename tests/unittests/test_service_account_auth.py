import unittest
import json
from unittest import mock
from datetime import datetime, timedelta

from tap_google_sheets.client import GoogleClient, SCOPES
from tap_google_sheets import validate_auth_config


class MockCredentials:
    """Mock service account credentials for testing."""
    def __init__(self):
        self.token = 'mock_sa_access_token'
        self.expiry = datetime.utcnow() + timedelta(hours=1)

    def refresh(self, request):
        self.token = 'refreshed_mock_sa_access_token'
        self.expiry = datetime.utcnow() + timedelta(hours=1)


class MockResponse:
    """Mock response object for requests calls."""
    def __init__(self, resp, status_code, content=[""], headers=None):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers

    def json(self, object_pairs_hook=None):
        return self.json_data


class TestConfigValidation(unittest.TestCase):
    """Test authentication configuration validation."""

    def test_valid_oauth2_config(self):
        """Test that valid OAuth2 config returns 'oauth2'."""
        config = {
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret',
            'refresh_token': 'test_refresh_token',
            'spreadsheet_id': 'test_spreadsheet_id',
            'start_date': '2024-01-01T00:00:00Z',
            'user_agent': 'test-agent'
        }
        auth_type = validate_auth_config(config)
        self.assertEqual(auth_type, 'oauth2')

    def test_valid_service_account_file_config(self):
        """Test that valid service account file config returns 'service_account'."""
        config = {
            'credentials_file': '/path/to/key.json',
            'spreadsheet_id': 'test_spreadsheet_id',
            'start_date': '2024-01-01T00:00:00Z',
            'user_agent': 'test-agent'
        }
        auth_type = validate_auth_config(config)
        self.assertEqual(auth_type, 'service_account')

    def test_valid_service_account_json_config(self):
        """Test that valid service account JSON config returns 'service_account'."""
        config = {
            'credentials_json': '{"type": "service_account", "project_id": "test"}',
            'spreadsheet_id': 'test_spreadsheet_id',
            'start_date': '2024-01-01T00:00:00Z',
            'user_agent': 'test-agent'
        }
        auth_type = validate_auth_config(config)
        self.assertEqual(auth_type, 'service_account')

    def test_error_both_oauth2_and_service_account(self):
        """Test that providing both OAuth2 and service account credentials raises error."""
        config = {
            'client_id': 'test_client_id',
            'client_secret': 'test_client_secret',
            'refresh_token': 'test_refresh_token',
            'credentials_file': '/path/to/key.json',
            'spreadsheet_id': 'test_spreadsheet_id',
            'start_date': '2024-01-01T00:00:00Z',
            'user_agent': 'test-agent'
        }
        with self.assertRaises(Exception) as context:
            validate_auth_config(config)
        self.assertIn('Cannot specify both OAuth2 credentials', str(context.exception))

    def test_error_neither_oauth2_nor_service_account(self):
        """Test that providing neither OAuth2 nor service account credentials raises error."""
        config = {
            'spreadsheet_id': 'test_spreadsheet_id',
            'start_date': '2024-01-01T00:00:00Z',
            'user_agent': 'test-agent'
        }
        with self.assertRaises(Exception) as context:
            validate_auth_config(config)
        self.assertIn('Must specify either OAuth2 credentials', str(context.exception))

    def test_error_both_credentials_file_and_json(self):
        """Test that providing both credentials_file and credentials_json raises error."""
        config = {
            'credentials_file': '/path/to/key.json',
            'credentials_json': '{"type": "service_account"}',
            'spreadsheet_id': 'test_spreadsheet_id',
            'start_date': '2024-01-01T00:00:00Z',
            'user_agent': 'test-agent'
        }
        with self.assertRaises(Exception) as context:
            validate_auth_config(config)
        self.assertIn('Cannot specify both credentials_file and credentials_json', str(context.exception))

    def test_empty_credentials_file_is_ignored(self):
        """Test that empty credentials_file falls back to requiring OAuth2."""
        config = {
            'credentials_file': '',
            'spreadsheet_id': 'test_spreadsheet_id',
            'start_date': '2024-01-01T00:00:00Z',
            'user_agent': 'test-agent'
        }
        with self.assertRaises(Exception) as context:
            validate_auth_config(config)
        self.assertIn('Must specify either OAuth2 credentials', str(context.exception))


class TestGoogleClientServiceAccount(unittest.TestCase):
    """Test GoogleClient with service account authentication."""

    @mock.patch('tap_google_sheets.client.service_account.Credentials.from_service_account_file')
    def test_load_credentials_from_file(self, mock_from_file):
        """Test loading service account credentials from file."""
        mock_credentials = MockCredentials()
        mock_from_file.return_value = mock_credentials

        client = GoogleClient(
            credentials_file='/path/to/key.json',
            user_agent='test-agent'
        )

        # Trigger credential loading
        client._load_service_account_credentials()

        mock_from_file.assert_called_once_with(
            '/path/to/key.json',
            scopes=SCOPES
        )

    @mock.patch('tap_google_sheets.client.service_account.Credentials.from_service_account_info')
    def test_load_credentials_from_json(self, mock_from_info):
        """Test loading service account credentials from inline JSON."""
        mock_credentials = MockCredentials()
        mock_from_info.return_value = mock_credentials

        credentials_json = json.dumps({
            'type': 'service_account',
            'project_id': 'test-project',
            'private_key_id': 'key-id',
            'private_key': 'fake-key',
            'client_email': 'sa@test.iam.gserviceaccount.com',
            'client_id': '123456789'
        })

        client = GoogleClient(
            credentials_json=credentials_json,
            user_agent='test-agent'
        )

        # Trigger credential loading
        client._load_service_account_credentials()

        mock_from_info.assert_called_once()
        call_args = mock_from_info.call_args
        self.assertEqual(call_args[1]['scopes'], SCOPES)
        self.assertEqual(call_args[0][0]['type'], 'service_account')

    @mock.patch('tap_google_sheets.client.GoogleAuthRequest')
    @mock.patch('tap_google_sheets.client.service_account.Credentials.from_service_account_file')
    def test_service_account_token_refresh(self, mock_from_file, mock_auth_request):
        """Test that service account token is refreshed when needed."""
        mock_credentials = mock.MagicMock()
        mock_credentials.token = None  # Token not yet obtained
        mock_credentials.expiry = None
        mock_from_file.return_value = mock_credentials

        # Set up refresh to populate token
        def do_refresh(request):
            mock_credentials.token = 'refreshed_token'
            mock_credentials.expiry = datetime.utcnow() + timedelta(hours=1)
        mock_credentials.refresh.side_effect = do_refresh

        client = GoogleClient(
            credentials_file='/path/to/key.json',
            user_agent='test-agent'
        )

        client._get_service_account_token()

        # Verify refresh was called
        mock_credentials.refresh.assert_called_once()

    @mock.patch('tap_google_sheets.client.requests.Session.request')
    @mock.patch('tap_google_sheets.client.GoogleAuthRequest')
    @mock.patch('tap_google_sheets.client.service_account.Credentials.from_service_account_file')
    def test_request_uses_service_account_bearer_token(self, mock_from_file, mock_auth_request, mock_session_request):
        """Test that API requests use the service account Bearer token."""
        mock_credentials = MockCredentials()
        mock_credentials.token = 'sa_bearer_token_123'
        mock_from_file.return_value = mock_credentials

        mock_session_request.return_value = MockResponse({'data': 'test'}, 200)

        client = GoogleClient(
            credentials_file='/path/to/key.json',
            user_agent='test-agent'
        )

        # Get access token first
        client.get_access_token()

        # Make a request
        client.request('GET', 'test/path')

        # Verify Bearer token was used in request
        call_kwargs = mock_session_request.call_args[1]
        self.assertEqual(
            call_kwargs['headers']['Authorization'],
            'Bearer sa_bearer_token_123'
        )

    def test_auth_type_detection_service_account_file(self):
        """Test that auth type is correctly detected for credentials_file."""
        client = GoogleClient(
            credentials_file='/path/to/key.json',
            user_agent='test-agent'
        )
        self.assertEqual(client._GoogleClient__auth_type, 'service_account')

    def test_auth_type_detection_service_account_json(self):
        """Test that auth type is correctly detected for credentials_json."""
        client = GoogleClient(
            credentials_json='{"type": "service_account"}',
            user_agent='test-agent'
        )
        self.assertEqual(client._GoogleClient__auth_type, 'service_account')

    def test_auth_type_detection_oauth2(self):
        """Test that auth type is correctly detected for OAuth2."""
        client = GoogleClient(
            client_id='test_client_id',
            client_secret='test_client_secret',
            refresh_token='test_refresh_token',
            user_agent='test-agent'
        )
        self.assertEqual(client._GoogleClient__auth_type, 'oauth2')


class TestGoogleClientOAuth2Backward(unittest.TestCase):
    """Test backward compatibility with OAuth2 authentication."""

    @mock.patch('tap_google_sheets.client.requests.Session.post')
    def test_oauth2_still_works(self, mock_post):
        """Test that OAuth2 authentication still works as before."""
        mock_post.return_value = MockResponse({
            'access_token': 'oauth2_token_123',
            'expires_in': 3600
        }, 200)

        client = GoogleClient(
            client_id='test_client_id',
            client_secret='test_client_secret',
            refresh_token='test_refresh_token',
            user_agent='test-agent'
        )

        client.get_access_token()

        # Verify OAuth2 endpoint was called
        call_args = mock_post.call_args
        self.assertIn('oauth2.googleapis.com/token', call_args[1]['url'])

    @mock.patch('tap_google_sheets.client.requests.Session.request')
    @mock.patch('tap_google_sheets.client.requests.Session.post')
    def test_oauth2_request_uses_correct_token(self, mock_post, mock_session_request):
        """Test that OAuth2 requests use the correct Bearer token."""
        mock_post.return_value = MockResponse({
            'access_token': 'oauth2_token_456',
            'expires_in': 3600
        }, 200)
        mock_session_request.return_value = MockResponse({'data': 'test'}, 200)

        client = GoogleClient(
            client_id='test_client_id',
            client_secret='test_client_secret',
            refresh_token='test_refresh_token',
            user_agent='test-agent'
        )

        client.get_access_token()
        client.request('GET', 'test/path')

        # Verify Bearer token was used
        call_kwargs = mock_session_request.call_args[1]
        self.assertEqual(
            call_kwargs['headers']['Authorization'],
            'Bearer oauth2_token_456'
        )


if __name__ == '__main__':
    unittest.main()
