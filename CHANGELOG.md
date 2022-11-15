# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Changed

### Fixed

### Deprecated

### Removed

## 0.2.0

Released on November 16, 2022.

This release introduces breaking changes by changing the default interface for tasks in this collection.

### Added
- Airbyte server block to handle client generation and support for NGINX authentication on OSS instances - [#40](https://github.com/PrefectHQ/prefect-airbyte/pull/40)
- Deprecation warning on `AirbyteClient.export_configuration`, as OSS airbyte v0.40.7 has removed the corresponding endpoint - [#40](https://github.com/PrefectHQ/prefect-airbyte/pull/40)

### Changed
- Task inputs for `trigger_sync` and `export_configuration` from accepting Airbyte network configurations as separate kwargs to accepting an `AirbyteServer` block instance - [#40](https://github.com/PrefectHQ/prefect-airbyte/pull/40)


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
