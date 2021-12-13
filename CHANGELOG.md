# Changelog

## 1.2.0
  * Fixed Pagination Failure [#50](https://github.com/singer-io/tap-google-sheets/pull/50)
  * Implemented Request Timeout [#54](https://github.com/singer-io/tap-google-sheets/pull/54)
  * Added a logger message when the sheet has the first row empty(noheaders) [$46](https://github.com/singer-io/tap-google-sheets/pull/46)
  * Added unsupported inclusion property and description [#47](https://github.com/singer-io/tap-google-sheets/pull/47)
  * Email address typo corrected [#53] (https://github.com/singer-io/tap-google-sheets/pull/53)


## 1.1.4
  * Removes PII from logging [#40](https://github.com/singer-io/tap-google-sheets/pull/40)

## 1.1.3
  * Add padding to columns without data and ignore hidden sheets [#35](https://github.com/singer-io/tap-google-sheets/pull/35)

## 1.1.2
  * Increase python version to `3.8.10`

## 1.1.1
  * Added better error messages for 429 errors

## 1.1.0
  * Allow Google `numberTypes` and date-time types to fall back to a string schema [#25](https://github.com/singer-io/tap-google-sheets/pull/25)

## 1.0.4
  * Return an empty list when we retrieve cells that return no values [#17](https://github.com/singer-io/tap-google-sheets/pull/17)

## 1.0.3
  * Fix issues: slashes `/` in sheet name 404 error; Discovery malformed sheet error when 2nd row final column value(s) are `NULL`.

## 1.0.2
  * Skip sheets for which we fail to generate a schema

## 1.0.1
  * Emit state file for incremental sync where bookmark not exceeded.

## 1.0.0
  * No change from `v0.0.4`

## 0.0.4
  * Add logic to skip empty worksheets in Discovery and Sync mode.

## 0.0.3
  * Update README.md documentation. Improved logging and handling of errors and warnings. Better null handling in Discovery and Sync. Fix issues with activate version messages.

## 0.0.2
  * Change number json schema to anyOf with multipleOf; skip empty rows; move write_bookmark to end of sync.py

## 0.0.1
  * Initial commit
