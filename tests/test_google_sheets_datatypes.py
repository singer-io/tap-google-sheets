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
    'numberType': {'anyOf': [{'format': 'singer.decimal', 'type': ['null', 'string']},
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

    def assertStringFormat(self, str_value, str_format):
            """
            Verify a given value (str_value) is  of the specified string format (str_format).
            else raise an AssertionError
            """
            try:
                date_stripped = datetime.datetime.strptime(str_value, str_format)
            except ValueError:
                raise AssertionError(f"{str_value} is not of the specified format {str_format}")

    def assertNotStringFormat(self, str_value, str_format):
            """
            Verify a given value (str_value) is NOT of the specified string format (str_format).
            else raise an AssertionError
            """
            try:
                date_stripped = datetime.datetime.strptime(str_value, str_format)
                raise AssertionError(f"{str_value} is UNEXPECTEDLY of the specified format {str_format}")
            except ValueError:
                pass

    def test_run(self):
        """
        Run discovery, initial sync.
        Test sheets:
        happysheet tests the happy flows where we ensure proper data typing of common values
        happysheet-string-fallback tests the flows where we inserted different data type values into a specific data type column and we expect to fall back to a string
        Assertions:
        Verify that the google defined datatypes in the sheet metadata stream map to the JSON schema as expected
        Verify usage of JSON schema
        -for a given datatype, we are able to use the string and nullible types
        Verify date, datetime, time formatting meets expectations
        Verify tap can support data for all supported datatypes
        """
        sadsheets = {stream for stream in self.expected_sync_streams() if stream.startswith('sadsheet-')}
        tested_streams = sadsheets.union({'happysheet', 'happysheet-string-fallback', 'sheet_metadata', 'sad-sheet-effective-format', 'test-sheet-date'})

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

        # get column names and google-sheets datatypes from the tested sheet's sheet_metadata record
        sheet_metadata_entry = [record['columns']
                                for record in sheet_metadata_records
                                if record['title'] == test_sheet][0]
        column_name_to_type = {entry['columnName']: entry['columnType']
                               for entry in sheet_metadata_entry}
        md_column_names = set(column_name_to_type.keys())

        # get field names for the tested sheet from the replicated schema
        test_sheet_schema = synced_records[test_sheet]['schema']
        schema_column_names = {schema_property
                               for schema_property in test_sheet_schema['properties'].keys()
                               if not schema_property.startswith('__sdc')}

        # Verify the sheet metadata accounts for all columns in the schema
        self.assertSetEqual(schema_column_names, md_column_names)

        # Verify that the sheet's sheet_metadata column types are consistent with the sheet's schema
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
        record_data = [message['data'] for message in synced_records[test_sheet]['messages'] if message.get('data')]
        data_map = {
	    "Currency": str,
	    "Datetime": str,
            "Time": str,
            "Date": str,
	    "String": str,
            "Number": str,
            "Boolean": bool,
        }
        string_column_formats = {
	    "Datetime": "%Y-%m-%dT%H:%M:%S.%fZ",
            "Time":  "%H:%M:%S"
        }

        for record in record_data:

            sdc_row = record['__sdc_row']

            with self.subTest(__sdc_row=sdc_row):
                for column in data_map.keys():

                    test_case = record.get(f'{column} Test Case')

                    with self.subTest(column=column, test_case=test_case):
                        if record.get(column):

                            # verify the datatypes for happysheet values match expectations
                            self.assertTrue(
                                isinstance(record[column], data_map[column]),
                                msg=f'actual_type={type(record[column])}, expected_type={data_map[column]}, value={record[column]}'
                            )

                            # BUG_TDL-14482 | https://jira.talendforge.org/browse/TDL-14482
                            #                 Rows of numberType.Time are falling back to string when there
                            #                 is an underlying date value associated with the formatted time value.

                            # verify dates, times and datetimes are of the expected format
                            if column in string_column_formats.keys():

                                if column == 'Time' and 'epoch' in test_case:  # BUG_TDL-14482
                                    continue  # skip assertion

                                self.assertStringFormat(record[column], string_column_formats[column])

        ##########################################################################
        ### Test the happy path datatype values
        ##########################################################################

        test_sheet = 'sad-sheet-effective-format'
        data_type_map = {
            "Currency": "stringValue",
            "Datetime": "numberType.DATE_TIME",
            "Time": "numberType.TIME",
            "String": "stringValue",
            "Number": "numberType",
            "Boolean": "boolValue",
        }

        # get column names and google-sheets datatypes from the tested sheet's sheet_metadata record
        sheet_metadata_entry = [record['columns']
                                for record in sheet_metadata_records
                                if record['title'] == test_sheet][0]
        column_type = {entry['columnName']: entry['columnType']
                               for entry in sheet_metadata_entry}
        md_column_names = set(column_type.keys())

        # get field names for the tested sheet from the replicated schema
        test_sheet_schema = synced_records[test_sheet]['schema']
        schema_column_names = {schema_property
                               for schema_property in test_sheet_schema['properties'].keys()
                               if not schema_property.startswith('__sdc')}

        # Verify the sheet metadata accounts for all columns in the schema
        self.assertSetEqual(schema_column_names, md_column_names)

        # Verify that the sheet's sheet_metadata column types are as expected
        for column_name in data_type_map.keys():
            with self.subTest(column=column_name):
                column_type = column_name_to_type[column_name]

                expected_type = data_type_map[column_name]

                self.assertEqual(column_type, expected_type)

        ##########################################################################
        ### Test the string fallbacks for each datatype
        ##########################################################################

        test_sheet = 'happysheet-string-fallback'

        # get field names for the tested sheet from the replicated schema
        record_data = [message['data']
                       for message in synced_records[test_sheet]['messages']
                       if message.get('data')]
        columns = set(data_map.keys())

        for record in record_data[1:]:  # skip the __sdc_row 2 since it is a valid type
            sdc_row = record['__sdc_row']

            with self.subTest(__sdc_row=sdc_row):
                for column in columns:

                    test_case = record.get(f"{column} Test Case")
                    with self.subTest(column=column, test_case=test_case):

                        value = record.get(column)

                        if test_case is None or 'empty' in test_case: # some rows we expect empty values rather than strings

                            # verify the expected rows are actually Null
                            self.assertIsNone(value)

                        # As "'0" returns false which does not satisfy th below test case for boolean column
                        elif value is not None or value != "":

                            if column == 'Boolean' and value  in (-1, 1, 0): # special integer values falls back to boolean
                                self.assertTrue(isinstance(value, bool), msg=f'test case: {test_case}  value: {value}')
                                continue
                            # verify the non-standard value has fallen back to a string type
                            self.assertTrue(isinstance(value, str), msg=f'test case: {test_case}  value: {value}')

                            # BUG_TDL-18932 [https://jira.talendforge.org/browse/TDL-18932]
                            #               Date and Datetime do not fall back to string for boolean, time, or numbers

                            # verify dates, times and datetimes DO NOT COERCE VALUES to the standard format
                            if column in string_column_formats.keys():
                                if column in ["Date", "Datetime"] and sdc_row in [3, 4, 6, 7]:  # BUG_TDL-18932
                                    continue  # skip assertion

                                self.assertNotStringFormat(value, string_column_formats[column])

                        else:
                            raise AssertionError(f"An unexpected row was empty! test case: {test_case}  value: {value}")


        ##########################################################################
        ### Test the unhappy path datatype values
        ##########################################################################

        # TODO Once some of the sadsheet (unhappy-path) test cases have been fixed we can implement
        #      assertions here for them.

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



        # Other Bugs that do not correspond to specific sadsheet

        # BUG_TDL-14344 | https://jira.talendforge.org/browse/TDL-14344
        #                 Tap is inconsistent in datatyping specific values for Boolean
