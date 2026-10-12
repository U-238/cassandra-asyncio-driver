# Changelog

## 1.0.0

### Compatibility

- Require Python 3.10 or newer and `cassandra-driver>=3.30.1,<4`.
- `aexecute()` now returns `AsyncResultSet`, a subclass of upstream `ResultSet`,
  instead of raw callback rows. Synchronous iteration, `all()`, `one()`, metadata,
  and synchronous page fetching remain available. Code relying on a plain list
  should use `list(rows)` or `rows.all()`.

### Added

- `session.aexecute_graph()` for DSE Graph queries.
- `session.aexecute_concurrent()` with ordered execution results, positive
  concurrency validation, immediate empty-input results, and worker-thread
  submission to avoid upstream blocking the application event loop.
- Async result iteration, `aall()`, and `afetch_next_page()`. Partial async
  consumption resumes at the next row; exhausted async results stay exhausted.
- Cancellation-safe page requests and continuous graph reads. Cancelling an
  await stops waiting; already submitted queries continue running.
- Ruff linting and formatting commands, Python 3.10–3.14 CI checks, live Cassandra
  integration checks, and clean wheel installation validation before publishing.

### Fixed

- Prepared insert integration tests use CQL `?` placeholders.
- Async callbacks safely handle completion after cancellation and subsequent pages.

### Validation limits

- Live integration coverage uses Apache Cassandra 5.0.9. Graph behavior is covered
  by unit tests; optional live DSE Graph validation requires an external server.
