import copy
import datetime
import os
import backoff
from tap_tester import runner, connections, menagerie

from base import GoogleSheetsBaseTest, TapRateLimitError


class BookmarksTest(GoogleSheetsBaseTest):
    """Ensure all sheets streams will replicate based off of the most recent bookmarked state for 'file_metadata'"""
  
    conn_id = ""
    expected_test_streams = ""
    record_count_by_stream_1 = ""
    @staticmethod
    def name():
        return "tap_tester_google_sheets_bookmarks"

    def test_run(self):
        """
        Run check mode, perform table and field selection, and run a sync.
        Replication can be triggered by pushing back state to prior 'file_metadata' state.
        Run a second sync after not updating state to verify no streams are being synced
        Run a 3rd sync and ensure full table streams are triggered by the simulated bookmark value.

        - Verify initial sync message actions include activate versions and the upserts
        - Verify no streams are synced when 'file_metadata' bookmark does not change
        - Verify that the third sync with the updated simulated bookmark has the same synced streams as the first sync
        - Verify that streams will sync based off of 'file_metadata' even when it is not selected
        """
        skipped_streams = {stream
                           for stream in self.expected_streams()
                           if stream.startswith('sadsheet')}.union({
                                   'file_metadata' # testing case without file_metadata selected, but still providing bookmark
                           })
        self.expected_test_streams = self.expected_streams() - skipped_streams
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


        # run a sync again, this time we shouldn't get any records back
        sync_job_name = runner.run_sync_mode(self, self.conn_id)
        exit_status = menagerie.get_exit_status(self.conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
        record_count_by_stream_2 = runner.examine_target_output_file(
            self, self.conn_id, self.expected_streams(), self.expected_primary_keys())

        # verify we do not sync any unexpected streams
        self.assertSetEqual(set(), set(record_count_by_stream_2.keys()))

        # verify no records were synced for our expected streams
        for stream in self.expected_test_streams:
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

        menagerie.set_state(self.conn_id, new_state)

        record_count_by_stream_3 = self.run_and_verify_sync(self.conn_id)
        synced_records_3 = runner.get_records_from_target_output()

        # verify we sync sheets based off the state of file_metadata
        self.assertDictEqual(self.record_count_by_stream_1, record_count_by_stream_3)

    # BUG_TDL-14407 https://jira.talendforge.org/browse/TDL-14407
    @backoff.on_exception(backoff.constant, TapRateLimitError, interval=100, max_tries=2)
    def starter(self):
        """
        instantiate connection, run discovery, and sync 
        """

        # instantiate connection
        self.conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(self.conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in self.expected_test_streams]

        self.perform_and_verify_table_and_field_selection(
            self.conn_id, test_catalogs, select_all_fields=True,
        )

        # run initial sync
        self.record_count_by_stream_1 = self.run_and_verify_sync(self.conn_id)
       
    
       
