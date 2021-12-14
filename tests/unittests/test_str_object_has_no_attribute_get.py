import unittest
from tap_google_sheets.client import raise_for_error, GoogleBadRequestError
import requests

# mock responce
class Mockresponse:
    def __init__(self, resp, status_code, content=[""], headers=None, raise_error=False, text={}):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text
        self.reason = "error"

    def prepare(self):
        return (self.json_data, self.status_code, self.content, self.headers, self.raise_error)

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code

        raise requests.exceptions.HTTPError("mock sample message")

    def json(self):
        return self.text

class TestErrorWithStrbject(unittest.TestCase):
    """
    Test that the tap does not break in case of a str error object in response.
    """
    def test_tap_handle_error_with_str_object_and_error_description(self):
        """
        Test that tap raise GoogleBadRequestError with status code 400 and proper message when error_description available in response.
        """
        error = {'error': 'invalid_grant', 'error_description': 'Bad Request'}
        try:
            raise_for_error(Mockresponse("", 400, raise_error=True, text=error))
        except GoogleBadRequestError as e:
            # Verifying the message formed for the exception
            self.assertEqual(str(e), "HTTP-error-code: 400 invalid_grant: Bad Request")

    def test_tap_handle_error_with_str_object_and_no_error_description(self):
        """
        Test that tap raise GoogleBadRequestError with status code 400 and Unknown Error message when error_description
        not available in response.
        """
        error = {'error': 'invalid_grant'}
        try:
            raise_for_error(Mockresponse("", 400, raise_error=True, text=error))
        except GoogleBadRequestError as e:
            # Verifying the message formed for the exception
            self.assertEqual(str(e), "HTTP-error-code: 400 invalid_grant: Unknown Error")
