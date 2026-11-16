from tap_tester import connections, runner

from base import GoogleSheetsBaseTest


class GoogleSheetsStartDateTest(GoogleSheetsBaseTest):
    """Verify that start_date does not filter full-table sheet streams."""

    STREAM = "Test-1"

    @staticmethod
    def name():
        return "tap_tester_google_sheets_start_date"

    def test_run(self):
        first_conn_id = connections.ensure_connection(self)
        first_catalogs = self.run_and_verify_check_mode(first_conn_id)
        first_test_catalogs = [
            catalog for catalog in first_catalogs
            if catalog.get("stream_name") == self.STREAM
        ]
        self.perform_and_verify_table_and_field_selection(
            first_conn_id, first_test_catalogs, select_all_fields=True
        )
        first_counts = self.run_and_verify_sync(first_conn_id)
        first_records = runner.get_records_from_target_output()

        self.start_date = "2099-01-01T00:00:00Z"
        second_conn_id = connections.ensure_connection(self, original_properties=False)
        second_catalogs = self.run_and_verify_check_mode(second_conn_id)
        second_test_catalogs = [
            catalog for catalog in second_catalogs
            if catalog.get("stream_name") == self.STREAM
        ]
        self.perform_and_verify_table_and_field_selection(
            second_conn_id, second_test_catalogs, select_all_fields=True
        )
        second_counts = self.run_and_verify_sync(second_conn_id)
        second_records = runner.get_records_from_target_output()

        primary_key = next(iter(self.expected_primary_keys()[self.STREAM]))
        first_primary_keys = {
            message["data"][primary_key]
            for message in first_records[self.STREAM]["messages"]
            if message["action"] == "upsert"
        }
        second_primary_keys = {
            message["data"][primary_key]
            for message in second_records[self.STREAM]["messages"]
            if message["action"] == "upsert"
        }

        self.assertGreater(first_counts[self.STREAM], 0)
        self.assertEqual(first_counts[self.STREAM], second_counts[self.STREAM])
        self.assertSetEqual(first_primary_keys, second_primary_keys)
