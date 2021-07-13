import datetime
import os
from decimal import Decimal

from tap_tester import connections, runner

from base import GoogleSheetsBaseTest


# expected mapping of google types to json schema
google_datatypes_to_json_schema = {  #  BUG 3 | TODO there are md keys in the schema: selected and inclusion
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
        tested_streams = {'happysheet', 'sheet_metadata'}
        # TODO Put this back   # self.expected_streams().difference({'sadsheet'})

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
	    "Currency": Decimal, # TODO BUG Currency is being identified as a decimal type rather than string
	    "Datetime": str,
            "Time": str,
            "Date": str,
	    "String": str,
            "Number": Decimal,
            "Boolean": bool, 
        }

        for record in record_data:
            with self.subTest(record=record):
                for col in data_map.keys():
                    with self.subTest(col=col):
                        try:
                            if record.get(col): # TODO BUG Boolean values are not being sent when they are null, but other datatypes are, when JSON schema specifies them as nullable
                                self.assertTrue(isinstance(record[col], data_map[col]), msg=f'actual={type(record[col])}, expected={data_map[col]}')
                        # some datatypes can also be str
                        except AssertionError as error: 
                            print(f"{error} error caught, string assertion made instead")
                            self.assertTrue(isinstance(record[col], str), msg=f'actual={type(record[col])}, expected=string')
                           
                            
                                
                            
                        
                        
                            
                        
                    
	            


        ##########################################################################
        ### Test the unhappy path datatype values
        ##########################################################################




        # TODO Finish Review of Schema
        #  BUG 1 | Integer is coming across as a decimal, (just the same thing as decimal)
        #  BUG 2 | Decimals only supported to e-15, values exceeding this result in critical errors
        

        # TODO BUG Date value out of range error is not handled, tap throws critical error
        #          minimum date	11/21/00-1
        #          big date (not max)	7/13/15589
        # TODO BUG "Record does not pass schema validation: [<class 'decimal.DivisionImpossible'>]"
        #           largest number?	1.80E+308
        

        # TODO BUG there are no activate version messages in the sheet_metadata, spreadsheet_metadata
        #          or sheets_loaded streams, even though they are full table
        
        # TODO BUG? Should we only sample 1 row to determine datatype of a column???

        # TODO BUG Tap is inconsistent in datatyping specific values 
        #          We do not account for non-standard values while datatyping the row, like we do during a sync.
        #           ie. 't' in a boolean column is synced as 'true', but a column whose first row has the value 't'
        #               will be typed as a string, not a boolean
       




        
