import copy
import datetime
import os
from tap_tester import runner, connections, menagerie, LOGGER

from base import GoogleSheetsBaseTest


class BookmarksTest(GoogleSheetsBaseTest):
    """Ensure all sheets streams will replicate in full table mode and create appropriate bookmarks"""

    conn_id = ""
    expected_test_streams = ""
    record_count_by_stream_1 = ""

    @staticmethod
    def name():
        return "tap_tester_google_sheets_bookmarks"

    def test_run(self):
        """
        Run check mode, perform table and field selection, and run a sync.
        - Verify initial sync message actions include activate versions and the upserts
        - check if bookmark include activate versions for all streams
        """
        skipped_streams = {stream
                           for stream in self.expected_streams()
                           if stream.startswith('sadsheet')}
        self.expected_test_streams = self.expected_streams() - skipped_streams

        # Grab connection, and run discovery and initial sync
        self.starter()

        synced_records_1 = runner.get_records_from_target_output()

        # Grab state to be updated later
        state = menagerie.get_state(self.conn_id)

        # BUG full table streams are saving bookmarks unnecessarily https://jira.talendforge.org/browse/TDL-14343

        # BUG there are no activate version messages in the sheet_metadata, spreadsheet_metadata
        #          or sheets_loaded streams, even though they are full table https://jira.talendforge.org/browse/TDL-14346
        # verify message actions are correct
        for stream in self.expected_test_streams.difference({'sheet_metadata', 'spreadsheet_metadata', 'sheets_loaded'}):
            with self.subTest(stream=stream):
                sync1_message_actions = [message['action'] for message in synced_records_1[stream]['messages']]
                self.assertEqual('activate_version', sync1_message_actions[0])
                self.assertEqual('activate_version', sync1_message_actions[-1])
                self.assertSetEqual({'upsert'}, set(sync1_message_actions[1:-1]))
                self.assertIn(stream, state["bookmarks"].keys())

    def starter(self):
        """
        Instantiate connection, run discovery, and initial sync.

        This entire process needs to retry if we get rate limited so that we are using a fresh connection
        and can test the activate version messages.
        """

        ##########################################################################
        ### Instantiate connection
        ##########################################################################
        self.conn_id = connections.ensure_connection(self)

        ##########################################################################
        ### Discovery without the backoff
        ##########################################################################
        check_job_name = runner.run_check_mode(self, self.conn_id)
        exit_status = menagerie.get_exit_status(self.conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)


        found_catalogs = menagerie.get_catalogs(self.conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(self.conn_id))
        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))

        self.assertSetEqual(self.expected_streams(), found_catalog_names, msg="discovered schemas do not match")
        LOGGER.info("discovered schemas are OK")


        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in self.expected_test_streams]

        self.perform_and_verify_table_and_field_selection(
            self.conn_id, test_catalogs, select_all_fields=True,
        )

        ##########################################################################
        ### Initial sync without the backoff
        ##########################################################################
        sync_job_name = runner.run_sync_mode(self, self.conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(self.conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        self.record_count_by_stream_1 = runner.examine_target_output_file(
            self, self.conn_id, self.expected_streams(), self.expected_primary_keys())
        self.assertGreater(
            sum(self.record_count_by_stream_1.values()), 0,
            msg="failed to replicate any data: {}".format(self.record_count_by_stream_1)
        )
        LOGGER.info("total replicated row count: %s", sum(self.record_count_by_stream_1.values()))
