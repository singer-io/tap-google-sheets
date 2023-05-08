"""
Test tap pagination of streams
"""
from tap_tester import connections, runner

from base import GoogleSheetsBaseTest




class PaginationTest(GoogleSheetsBaseTest):
    """ Test the tap pagination to get multiple pages of data """

    @staticmethod
    def name():
        return "tap_tester_google_sheets_pagination_test"


    def test_run(self):
        """
        Verify that for each stream you can get multiple pages of data
        and that when all fields are selected more than the automatic fields are replicated.

        Verify by primary keys that data is unique for page
        PREREQUISITE
        This test relies on the existence of a specific sheet with the name Pagination that has a column
        called 'id' with values 1 -> 238.
        """
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select all applicable streams and all fields within those streams
        testable_streams = {"Pagination", "sadsheet-pagination", "Pagination-1000-empty", "Pagination-1001-empty", "Pagination-999-empty" }
        test_catalogs = [catalog for catalog in found_catalogs if
                         catalog.get('tap_stream_id') in testable_streams]
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs, select_all_fields=True)

        # run the sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Added back `sadsheet-pagination` to testable_streams as # BUG TDL-14376 resolved.
        for stream in testable_streams:
            with self.subTest(stream=stream):

                our_fake_pk = 'id'
                fake_pk_list = [message.get('data').get(our_fake_pk)
                                for message in synced_records[stream]['messages']
                                if message['action'] == 'upsert']

                our_actual_pk = list(self.expected_primary_keys()[stream])[0]
                actual_pk_list = [message.get('data').get(our_actual_pk)
                                  for message in synced_records[stream]['messages']
                                  if message['action'] == 'upsert']

                # verify that we can paginate with all fields selected
                self.assertGreater(record_count_by_stream.get(stream, 0), self.API_LIMIT)

                if stream == "Pagination":
                    # verify the data for the "Pagination" stream is free of any duplicates or breaks by checking
                    # our fake pk value ('id')
                    # THIS ASSERTION CAN BE MADE BECAUSE WE SETUP DATA IN A SPECIFIC WAY. DONT COPY THIS

                    self.assertEqual(list(map(str, (range(1, 239)))), fake_pk_list)
                    # verify the data for the "Pagination" stream is free of any duplicates or breaks by checking
                    # the actual primary key values (__sdc_row)
                    self.assertEqual(list(range(2, 240)), actual_pk_list)
                else:
                    # Setup max rows to check and null rows to check as per the testcase
                    max_range = 1007
                    if stream == "sadsheet-pagination":
                        max_range = 238
                        null_rows = [198, 199]
                        sdc_null_rows = [199, 200]
                    elif stream == "Pagination-1000-empty":
                        null_rows = [1000]
                        sdc_null_rows = [1001]
                    elif stream == "Pagination-1001-empty":
                        null_rows = [1001]
                        sdc_null_rows = [1002]
                    elif stream == "Pagination-999-empty":
                        null_rows = [999]
                        sdc_null_rows = [1000]

                    # verify the data for the rest of the streams in testable_streams is free of any duplicates or breaks by
                    # checking our fake pk value ('id')
                    expected_pk_list = range(1, max_range)
                    expected_pk_list = [x for x in expected_pk_list if x not in null_rows]
                    expected_pk_list = list(map(str, expected_pk_list))
                    self.assertEqual(expected_pk_list, fake_pk_list)
                    
                    # verify the data for the rest of the streams in testable_streams is free of any duplicates or breaks by
                    # checking the actual primary key values (__sdc_row)
                    expected_pk_list = range(2, max_range + 1)
                    expected_pk_list = [x for x in expected_pk_list if x not in sdc_null_rows]
                    self.assertEqual(expected_pk_list, actual_pk_list)
                    
