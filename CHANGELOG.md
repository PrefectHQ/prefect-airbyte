# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Changed

### Deprecated

### Removed

### Fixed
- Docstring for `trigger_sync` task and removed inappropriate default value for `connection_id` - [#26](https://github.com/PrefectHQ/prefect-airbyte/pull/26)

## 0.1.2

Released on September 8, 2022.

### Fixed

- Status code handling in Airbyte client - [#20](https://github.com/PrefectHQ/prefect-airbyte/pull/20)

## 0.1.1

Released on September 2, 2022.

### Added

- a `timeout` parameter to `trigger_sync` and `export_configuration` passed to `httpx.AsyncClient` - [#18](https://github.com/PrefectHQ/prefect-airbyte/pull/18)

### Fixed

- Using `asyncio.sleep` instead of `time.sleep` within `trigger_sync` task - [#13](https://github.com/PrefectHQ/prefect-airbyte/pull/13)

### Security

## 0.1.0

Released on July 26th, 2022.

### Added

- `trigger_sync` and `export_configuration` tasks - [#1](https://github.com/PrefectHQ/prefect-airbyte/pull/1)
