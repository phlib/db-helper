# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) 
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]

## [1.0.1] - 2017-04-19
### Fixed
- SECURITY: *BulkInsert* values were not properly quoted when building the SQL.
  In most cases this means that *BulkInsert* simply didn't work.
 
## [1.0.0] - 2017-04-13
### Added
- Add helpers from `phlib/db`: `BulkInsert`, `BigResult`, `QueryPlanner` 
