# Changelog

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
