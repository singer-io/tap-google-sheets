"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import unittest
import os
from datetime import timedelta
from datetime import datetime as dt

from tap_tester import connections, menagerie, runner

class GoogleSheetsBaseTest(unittest.TestCase):
    """
    Setup expectations for test sub classes.
    Metadata describing streams.

    A bunch of shared methods that are used in tap-tester tests.
    Shared tap-specific methods (as needed).
    """
    AUTOMATIC_FIELDS = "automatic"
    UNSUPPORTED_FIELDS = "unsupported"
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = 200
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    BOOKMARK_COMPARISON_FORMAT =  "%Y-%m-%dT%H:%M:%S.%fZ"

    start_date = ""

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-google-sheets"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.google-sheets"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date': os.getenv("TAP_GOOGLE_SHEETS_START_DATE"),
            'spreadsheet_id': os.getenv("TAP_GOOGLE_SHEETS_SPREADSHEET_ID")
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    @staticmethod
    def get_credentials():
        """Authentication information for the test account"""
        return {
            "client_id": os.getenv("TAP_GOOGLE_SHEETS_CLIENT_ID"),
            "client_secret": os.getenv("TAP_GOOGLE_SHEETS_CLIENT_SECRET"),
            "refresh_token": os.getenv("TAP_GOOGLE_SHEETS_REFRESH_TOKEN"),
        }

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""
        default_sheet = {
            self.PRIMARY_KEYS:{"__sdc_row"},
            self.REPLICATION_METHOD: self.FULL_TABLE,  # DOCS_BUG TDL-14240 | DOCS say INC but it is FULL
            # self.REPLICATION_KEYS: {"modified_at"}
        }
        return {
            "file_metadata": {
                self.PRIMARY_KEYS: {"id", },
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"modifiedTime"}
            },
            "sheet_metadata": {
                self.PRIMARY_KEYS: {"sheetId"}, # "spreadsheetId"}, # BUG? | This is not in the real tap, "spreadsheetId"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
            },
            "sheets_loaded":{
                self.PRIMARY_KEYS:{"spreadsheetId", "sheetId", "loadDate"},  # DOCS_BUG  TDL-14240 | loadDate
                self.REPLICATION_METHOD: self.FULL_TABLE
            },
            "spreadsheet_metadata": {
                self.PRIMARY_KEYS: {"spreadsheetId"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
            },
            "Test-1": default_sheet,
            "SKU COGS":default_sheet,
            "Item Master":  {
                self.PRIMARY_KEYS:{"__sdc_row"},
                self.REPLICATION_METHOD: self.FULL_TABLE,  # DOCS_BUG TDL-14240 | DOCS say INC but it is FULL
                self.UNSUPPORTED_FIELDS: {
                    'ATT3', 'ATT4', 'ATT5', 'ATT7', 'ATT6'
                },
            },
            "Retail Price": default_sheet,
            "Retail Price NEW":default_sheet,
            "Forecast Scenarios": default_sheet,
            "Promo Type": default_sheet,
            "Shipping Method":default_sheet,
            "Pagination": default_sheet,
            "happysheet": default_sheet,
            "happysheet-string-fallback": default_sheet,
            "sadsheet-pagination": default_sheet,
            "sadsheet-number": default_sheet,
            "sadsheet-datetime": default_sheet,
            "sadsheet-date": default_sheet,
            "sadsheet-currency": default_sheet,
            "sadsheet-time": default_sheet,
            "sadsheet-string": default_sheet,
            "sadsheet-empty-row-2": default_sheet,
            "sadsheet-headers-only": default_sheet,
            "sadsheet-duplicate-headers-case": default_sheet,
            "sad-sheet-effective-format": default_sheet, # WIP
            "test-sheet-date": default_sheet, # WIP
            "test-sheet-number": default_sheet, # WIP
            "sadsheet-column-skip-bug": {
                self.PRIMARY_KEYS:{"__sdc_row"},
                self.REPLICATION_METHOD: self.FULL_TABLE,  # DOCS_BUG TDL-14240 | DOCS say INC but it is FULL
                self.UNSUPPORTED_FIELDS: {'__sdc_skip_col_06'},
            }
        }

    def expected_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def expected_sync_streams(self):
        remove_streams = {
            'sadsheet-duplicate-headers-case', # BUG |https://jira.talendforge.org/browse/TDL-14398 comment out to reproduce headers case
            'sadsheet-empty-row-2',
            'sadsheet-headers-only'
        }
        sync_streams = self.expected_streams().difference(remove_streams)
        return sync_streams

    def child_streams(self):
        """
        Return a set of streams that are child streams
        based on having foreign key metadata
        """
        return {stream for stream, metadata in self.expected_metadata().items()
                if metadata.get(self.FOREIGN_KEYS)}

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_automatic_fields(self):
        """
        return a dictionary with key of table name
        and value as a set of automatic key fields
        """
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()).union(v.get(self.REPLICATION_KEYS, set()))
        return auto_fields

    def expected_replication_method(self):
        """return a dictionary with key of table name nd value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def setUp(self):
        missing_envs = [x for x in [
            "TAP_GOOGLE_SHEETS_START_DATE",
            "TAP_GOOGLE_SHEETS_SPREADSHEET_ID",
            "TAP_GOOGLE_SHEETS_CLIENT_ID",
            "TAP_GOOGLE_SHEETS_CLIENT_SECRET",
            "TAP_GOOGLE_SHEETS_REFRESH_TOKEN",
        ] if os.getenv(x) is None]
        if len(missing_envs) != 0:
            raise Exception(f"missing variables: {missing_envs}")


    #########################
    #   Helper Methods      #
    #########################

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.

        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))

        self.assertSetEqual(self.expected_streams(), found_catalog_names, msg="discovered schemas do not match")
        print("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of record count for each stream.
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())
        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    def perform_and_verify_table_and_field_selection(self,
                                                     conn_id,
                                                     test_catalogs,
                                                     select_all_fields=True):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters.

        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(
            conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('stream_name') for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify all testable streams are selected
            top_level_md = [md_entry for md_entry in catalog_entry['metadata']
                            if md_entry['breadcrumb'] == []]
            selected = top_level_md[0]['metadata'].get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                field_level_md = [md_entry for md_entry in catalog_entry['metadata']
                                  if md_entry['breadcrumb'] != []]
                for field_md in field_level_md:
                    field = field_md['breadcrumb'][1]
                    field_selected = field_md['metadata'].get('selected')
                    print("\tValidating selection on {}.{}: {}".format(
                        cat['stream_name'], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat['stream_name'])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
                # BUG TDL-14241 | Replication keys are not automatic
                if cat['stream_name'] == "file_metadata":
                    expected_automatic_fields.remove('modifiedTime')
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (
                field['metadata']['selected'] is True or \
                field['metadata']['inclusion'] == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields


    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)

    @staticmethod
    def parse_date(date_value):
        """
        Pass in string-formatted-datetime, parse the value, and return it as an unformatted datetime object.
        """
        date_formats = {
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f+00:00",
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%d"
        }
        for date_format in date_formats:
            try:
                date_stripped = dt.strptime(date_value, date_format)
                return date_stripped
            except ValueError:
                continue

        raise NotImplementedError("Tests do not account for dates of this format: {}".format(date_value))

    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)

            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
            try:
                date_stripped = dt.strptime(dtime, self.BOOKMARK_COMPARISON_FORMAT)
                return_date = date_stripped + timedelta(days=days)

                return dt.strftime(return_date, self.BOOKMARK_COMPARISON_FORMAT)

            except ValueError:
                valid_formats = [self.BOOKMARK_COMPARISON_FORMAT, self.START_DATE_FORMAT]
                return Exception(f"Datetime object is not in an expected format: {valid_formats}")

    ##########################################################################
    ### Tap Specific Methods
    ##########################################################################

    def is_sheet(self, stream):
        non_sheets_streams = {'sheet_metadata', 'file_metadata', 'sheets_loaded', 'spreadsheet_metadata'}
        return stream in self.expected_streams().difference(non_sheets_streams)

    def undiscoverable_sheets(self):
        undiscoverable_streams = {'sadsheet-duplicate-headers', 'sadsheet-empty-row-1', 'sadsheet-empty'}
        return undiscoverable_streams

    def expected_unsupported_fields(self):
        """
        return a dictionary with key of table name
        and value as a set of automatic key fields
        """
        bad_fields = {}
        for k, v in self.expected_metadata().items():
            bad_fields[k] = v.get(self.UNSUPPORTED_FIELDS, set())

        return bad_fields
