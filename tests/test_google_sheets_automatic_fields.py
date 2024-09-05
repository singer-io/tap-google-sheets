"""
Test that with no fields selected for a stream automatic fields are still replicated

"""
import os

from tap_tester import runner, connections

from base import GoogleSheetsBaseTest


class AutomaticFields(GoogleSheetsBaseTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    @staticmethod
    def name():
        return "tap_tester_google_sheets_automatic_fields"

    def test_run(self):
        """
        Ensure running the tap with all streams selected and all fields deselected results in the
        replication of just the primary keys and replication keys (automatic fields).
         - Verify we can deselect all fields except when inclusion=automatic (SaaS Taps).
         - Verify that only the automatic fields are sent to the target.
        """

        expected_streams = self.expected_sync_streams()

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_automatic_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_automatic_fields, select_all_fields=False,
        )

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_keys = self.expected_automatic_fields().get(stream)

                # collect actual values
                messages = synced_records.get(stream)
                record_messages_keys = [set(message['data'].keys()) for message in messages['messages']
                                        if message['action'] == 'upsert']

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)
