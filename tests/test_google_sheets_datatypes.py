import datetime
import os
from decimal import Decimal

from tap_tester import connections, runner

from base import GoogleSheetsBaseTest


#  BUG_TDL-14371 | https://jira.talendforge.org/browse/TDL-14371
#                  there are md keys in the schema: selected and inclusion

# expected mapping of google types to json schema
google_datatypes_to_json_schema = {
    'stringValue': {'inclusion': 'available',
                    'selected': True,
                    'type': ['null', 'string']},
    'boolValue': {'inclusion': 'available',
                  'selected': True,
                  'type': ['null', 'boolean', 'string']},
    'numberType.DATE': {'anyOf': [{'format': 'date',
                                   'type': ['null', 'string']},
                                  {'type': ['null', 'string']}],
                        'inclusion': 'available',
                        'selected': True},
    'numberType.DATE_TIME': {'anyOf': [{'format': 'date-time',
                                        'type': ['null', 'string']},
                                       {'type': ['null', 'string']}],
                             'inclusion': 'available',
                             'selected': True},
    'numberType': {'anyOf': [{'multipleOf': Decimal('1E-15'),
                              'type': 'number'},
                             {'type': ['null', 'string']}],
                   'inclusion': 'available',
                   'selected': True},
    'numberType.TIME': {'anyOf': [{'format': 'time',
                                   'type': ['null', 'string']},
                                  {'type': ['null', 'string']}],
                        'inclusion': 'available',
                        'selected': True},
}


class DatatypesTest(GoogleSheetsBaseTest):

    @staticmethod
    def name():
        return "tap_tester_google_sheets_datatypes_test"

    def test_run(self):
        sadsheets = {stream for stream in self.expected_sync_streams() if stream.startswith('sadsheet-')}
        tested_streams = sadsheets.union({'happysheet', 'sheet_metadata'}) 

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                      if catalog.get('tap_stream_id') in tested_streams]
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs_all_fields, select_all_fields=True)

        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        our_message_actions_by_stream = {stream: [message['action']
                                                  for message in synced_records[stream]['messages']]
                                         for stream in tested_streams}

        ##########################################################################
        ### Test the consistency between a sheet and the sheet_metadata stream
        ##########################################################################

        # grab all expected sheets
        expected_sheets = {stream
                           for stream in self.expected_streams()
                           if self.is_sheet(stream)}

        # grab sheet metadata from the replicated records for the sheet_metadata stream
        sheet_metadata_records = [message['data']
                                  for message in synced_records['sheet_metadata']['messages']
                                  if message.get('data')]
        sheet_metadata_titles = [record['title'] for record in sheet_metadata_records]
        sheet_metadata_titles_set = set(sheet_metadata_titles)

        # Verify only the expected sheets are accounted for in sheet_metadata without dupes
        self.assertEqual(len(sheet_metadata_titles), len(sheet_metadata_titles_set))
        self.assertSetEqual(expected_sheets, sheet_metadata_titles_set)

        test_sheet = 'happysheet'
        sheet_metadata_entry = [record['columns']
                                for record in sheet_metadata_records
                                if record['title'] == test_sheet][0]
        column_name_to_type = {entry['columnName']: entry['columnType']
                               for entry in sheet_metadata_entry}
        md_column_names = set(column_name_to_type.keys())
        test_sheet_schema = synced_records[test_sheet]['schema']
        schema_column_names = {schema_property
                               for schema_property in test_sheet_schema['properties'].keys()
                               if not schema_property.startswith('__sdc')}

        # Verify the sheet metadata accounts for all columns in the schema
        self.assertSetEqual(schema_column_names, md_column_names)

        # Verify that the sheet metadata column types are consistent with the sheet schema
        for column_name in schema_column_names:
            with self.subTest(column=column_name):
                column_type = column_name_to_type[column_name]

                expected_schema = google_datatypes_to_json_schema[column_type]
                actual_schema = test_sheet_schema['properties'][column_name]

                self.assertDictEqual(expected_schema, actual_schema)

        ##########################################################################
        ### Test the happy path datatype values
        ##########################################################################

        # Verify that all data has been coerced to the expected column datatype
        record_data = [message['data'] for message in synced_records['happysheet']['messages'] if message.get('data')]
        data_map = {
	    "Currency": Decimal, # BUG Currency is being identified as a decimal type rather than string https://jira.talendforge.org/browse/TDL-14360
	    "Datetime": str,
            "Time": str,
            "Date": str,
	    "String": str,
            "Number": Decimal,
            "Boolean": bool,
        }

        # TODO setup data in a way that does not require the try catch
        #      use the test case columns to determine if string fallback is necessary rather than leaving
        #      an ambiguous assertion in this test

        for record in record_data:
            with self.subTest(record=record):
                for col in data_map.keys():
                    with self.subTest(col=col):
                        try:
                            if record.get(col):
                                self.assertTrue(isinstance(record[col], data_map[col]), msg=f'actual={type(record[col])}, expected={data_map[col]}')
                        # some datatypes can also be str
                        except AssertionError as error:
                            print(f"{error} error caught, string assertion made instead")
                            self.assertTrue(isinstance(record[col], str), msg=f'actual={type(record[col])}, expected=string')



        ##########################################################################
        ### Test the unhappy path datatype values
        ##########################################################################


        ##########################################################################
        ### BUGs
        ##########################################################################

        # NB | Reproducing Bugs
        #      In order to reproduce the following bugs, you must access the test data via the
        #      google-sheets ui. Each sadsheet-<datatype> will have a valid first row entry to satisfy
        #      discovery and force the column datatype. The remaining rows consist of a comment description
        #      of the error case and the value that errors. ONLY 1 VALUE CAN BE TESTED AT A TIME. Just move
        #      the desired value back into the datatype column and run the test to see the failures

        # BUG | <ticket> | <stream>

        #  BUG_TDL-14372 | https://jira.talendforge.org/browse/TDL-14372 | sadsheet-number
        #                  Decimals only supported to e-15, values exceeding this result in critical errors

        #  BUG_TDL-14374 | https://jira.talendforge.org/browse/TDL-14374 | sadsheet-number
        #                  "Record does not pass schema validation: [<class 'decimal.DivisionImpossible'>]"
        #                     largest number?	1.80E+308

        # BUG_TDL-14389 | https://jira.talendforge.org/browse/TDL-14389  | sadsheet-number
        #                 Integer is coming across as a decimal, (just the same thing as decimal)


        # BUG TDL-14386 | https://jira.talendforge.org/browse/TDL-14386 | sadshseet-date
        #                 Date value out of range error is not handled, tap throws critical error
        #                    minimum date	11/21/00-1
        #                    big date (not max)	7/13/15589

        # Other Bugs that do not correspond to specific sadsheet

        # BUG_TDL-14344 | https://jira.talendforge.org/browse/TDL-14344
        #                 Tap is inconsistent in datatyping specific values for Boolean
