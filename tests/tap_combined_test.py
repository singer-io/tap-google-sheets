import unittest
import os
from datetime import datetime as dt
from datetime import timedelta

from tap_tester import menagerie
import tap_tester.runner as runner
import tap_tester.connections as connections
from tap_tester.scenario import SCENARIOS


class TapCombinedTest(unittest.TestCase):
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"

    @staticmethod
    def name():
        return "tap_google_sheets_combined_test"

    @staticmethod
    def tap_name():
        return "tap-google-sheets"

    @staticmethod
    def get_type():
        return "platform.google-sheets"

    def expected_check_streams(self):
        return set(self.expected_pks().keys())

    def expected_sync_streams(self):
        return set(self.expected_pks().keys())

    @staticmethod
    def expected_pks():
        return {
            "file_metadata": {"id"},
            "sheet_metadata": {"sheetId"},
            "sheets_loaded": {"spreadsheetId", "sheetId", "loadDate"},
            "spreadsheet_metadata": {"spreadsheetId"},
            "Test-1": {"__sdc_row"},
            "Test 2": {"__sdc_row"},
            "SKU COGS": {"__sdc_row"},
            "Item Master": {"__sdc_row"},
            "Retail Price": {"__sdc_row"},
            "Retail Price NEW": {"__sdc_row"},
            "Forecast Scenarios": {"__sdc_row"},
            "Promo Type": {"__sdc_row"},
            "Shipping Method": {"__sdc_row"}
        }


    def get_properties(self):
        return_value = {
            'start_date': os.getenv("TAP_GOOGLE_SHEETS_START_DATE"),
            'spreadsheet_id': os.getenv("TAP_GOOGLE_SHEETS_SPREADSHEET_ID")
        }

        return return_value

    @staticmethod
    def get_credentials():
        return {
            "client_id": os.getenv("TAP_GOOGLE_SHEETS_CLIENT_ID"),
            "client_secret": os.getenv("TAP_GOOGLE_SHEETS_CLIENT_SECRET"),
            "refresh_token": os.getenv("TAP_GOOGLE_SHEETS_REFRESH_TOKEN"),
        }

    def setUp(self):
        missing_envs = [x for x in [
            "TAP_GOOGLE_SHEETS_SPREADSHEET_ID",
            "TAP_GOOGLE_SHEETS_START_DATE",
            "TAP_GOOGLE_SHEETS_CLIENT_ID",
            "TAP_GOOGLE_SHEETS_CLIENT_SECRET",
            "TAP_GOOGLE_SHEETS_REFRESH_TOKEN",
        ] if os.getenv(x) is None]

        if missing_envs:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    def test_run(self):

        conn_id = connections.ensure_connection(self, payload_hook=None)

        # Run the tap in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify the check's exit status
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify that there are catalogs found
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        subset = self.expected_check_streams().issubset(found_catalog_names)
        self.assertTrue(subset, msg="Expected check streams are not subset of discovered catalog")
        #
        # # Select some catalogs
        our_catalogs = [c for c in found_catalogs if c.get('tap_stream_id') in self.expected_sync_streams()]
        for catalog in our_catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, schema, [], [])

        # # Verify that all streams sync at least one row for initial sync
        # # This test is also verifying access token expiration handling. If test fails with
        # # authentication error, refresh token was not replaced after expiring.
        menagerie.set_state(conn_id, {})
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(),
                                                                   self.expected_pks())
        zero_count_streams = {k for k, v in record_count_by_stream.items() if v == 0}
        self.assertFalse(zero_count_streams,
                         msg="The following streams did not sync any rows {}".format(zero_count_streams))



SCENARIOS.add(TapCombinedTest)
