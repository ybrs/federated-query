# Changelog

All notable changes to this project are recorded here, newest first.
History before this file exists only in `git log`.

## [Unreleased]

### Added

- Native Parquet manifest ingestion over explicit file lists, with
  JSON string extraction (`json_extract_string`) running natively on the
  source and coordinator.

### Fixed

- State-file header reads go through SQLite's VFS so closing a
  descriptor cannot release the process's SQLite POSIX locks.
