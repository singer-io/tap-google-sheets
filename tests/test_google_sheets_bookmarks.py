"""
Test that with no fields selected for a stream automatic fields are still replicated

"""
import copy
import datetime
import os

from tap_tester import runner, connections, menagerie

from base import GoogleSheetsBaseTest


class BookmarksTest(GoogleSheetsBaseTest):
    """Test that with no fields selected for a stream automatic fields are still replicated"""

    @staticmethod
    def name():
        return "tap_tester_google_sheets_bookmarks"

    def test_run(self):
        """
        TODO fix this docstring
        Ensure running the tap with all streams selected and all fields deselected results in the
        replication of just the primary keys and replication keys (automatic fields).
         - Verify we can deselect all fields except when inclusion=automatic (SaaS Taps).
         - Verify that only the automatic fields are sent to the target.
         - TODO Verify that you get more than a page of data w/ ony automatic fields.
        """
        skipped_streams = {stream
                           for stream in self.expected_streams()
                           if stream.startswith('sadsheet')}.union({
                                   'file_metadata' # testing case without file_metadata selected, but still providing bookmark
                           })


        expected_streams = self.expected_streams() - skipped_streams

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs, select_all_fields=True,
        )

        # run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id)
        synced_records_1 = runner.get_records_from_target_output()

        # Update state to be prior to the last file_metadata state for stream
        state = menagerie.get_state(conn_id)
        # BUG full table streams are saving bookmarks unnecessarily https://jira.talendforge.org/browse/TDL-14343

        # BUG there are no activate version messages in the sheet_metadata, spreadsheet_metadata
        #          or sheets_loaded streams, even though they are full table https://jira.talendforge.org/browse/TDL-14346
        # verify message actions are correct
        for stream in expected_streams.difference({'sheet_metadata', 'spreadsheet_metadata', 'sheets_loaded'}):
            with self.subTest(stream=stream):
                sync1_messages = synced_records_1[stream]['messages']
                sync1_message_actions = [message['action'] for message in synced_records_1[stream]['messages']]
                self.assertEqual('activate_version', sync1_message_actions[0])
                self.assertEqual('activate_version', sync1_message_actions[-1])
                self.assertSetEqual({'upsert'}, set(sync1_message_actions[1:-1]))


        # run a sync again, this time we shouldn't get any records back
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
        record_count_by_stream_2 = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())

        # verify we do not sync any unexpected streams
        self.assertSetEqual(set(), set(record_count_by_stream_2.keys()))

        # verify no records were synced for our expected streams
        for stream in expected_streams:
            with self.subTest(stream=stream):
                self.assertEqual(0, record_count_by_stream_2.get(stream, 0))

        # roll back the state of the file_metadata stream to ensure that we sync sheets
        # based off of this state
        file_metadata_stream = 'file_metadata'
        file_metadata_bookmark = state['bookmarks'][file_metadata_stream]
        bookmark_datetime = datetime.datetime.strptime(file_metadata_bookmark, self.BOOKMARK_COMPARISON_FORMAT)
        target_datetime = bookmark_datetime + datetime.timedelta(days=-1)
        target_bookmark = datetime.datetime.strftime(target_datetime, self.BOOKMARK_COMPARISON_FORMAT)

        new_state = copy.deepcopy(state)
        new_state['bookmarks'][file_metadata_stream] = target_bookmark

        menagerie.set_state(conn_id, new_state)

        record_count_by_stream_3 = self.run_and_verify_sync(conn_id)
        synced_records_3 = runner.get_records_from_target_output()

        # verify we sync sheets based off the state of file_metadata
        self.assertDictEqual(record_count_by_stream_1, record_count_by_stream_3)
