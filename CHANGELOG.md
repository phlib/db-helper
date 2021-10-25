# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) 
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

## [2.0.0] - 2021-10-25
### Added
- Add support for PHP v8.
- Type declarations have been added to all properties, method parameters and
  return types where possible.
### Changed
- *BulkInsert*: **BC Break**: Private visibility for `fetchSql()` method. This
  shouldn't be used in regular implementations, as the SQL is used directly by
  `write()`.
- **BC break**: Reduce visibility of internal methods and properties. These
  members are not part of the public API. No impact to standard use of this
  package. If an implementation has a use case which needs to override these
  members, please submit a pull request explaining the change.
- Upgrade underlying `phlib/db` to v2.
### Removed
- **BC break**: Removed support for PHP versions <= v7.3 as they are no longer
  [actively supported](https://php.net/supported-versions.php) by the PHP project.

## [1.0.2] - 2021-10-24
### Fixed
- *BulkInsert*: Catch `RuntimeException` from underlying *phlib/db* for deadlocks.
  Migration from *phlib/db* mistakenly created a new `RuntimeException` class.

## [1.0.1] - 2017-04-19
### Fixed
- SECURITY: *BulkInsert* values were not properly quoted when building the SQL.
  In most cases this means that *BulkInsert* simply didn't work.
 
## [1.0.0] - 2017-04-13
### Added
- Add helpers from `phlib/db`: `BulkInsert`, `BigResult`, `QueryPlanner` 
