# Cassandra AsyncIO Driver

Wrapper for `cassandra-driver` that makes blocking calls awaitable using `asyncio`

Requires Python 3.10+ and Apache Cassandra Python Driver 3.30.1+ (3.x).

## Installation

```shell
$ pip install cassandra-asyncio-driver
```

## Usage

To adapt existing code to use the asyncio driver, replace:

```python
from cassandra.cluster import Cluster
```

with:

```python
from cassandra_asyncio.cluster import Cluster
```

Queries can be executed asynchronously using `await session.aexecute()`.

For synchronous queries, `session.execute()` can still be used as normal.

`aexecute()` accepts the same arguments as `execute_async()` and returns the
`AsyncResultSet`, preserving upstream column metadata, paging state, and
`current_rows`. Use `async for row in rows` for automatic nonblocking paging,
`await rows.aall()` to collect rows, or `await rows.afetch_next_page()` for manual
paging. Ordinary `for`, `list(rows)`, `rows.all()`, and `rows.fetch_next_page()`
retain upstream synchronous behavior and may block while fetching pages. Use one
consumer at a time and avoid mixing synchronous and async consumption of a result.
Partial async consumption resumes at the next row when the iterator is closed;
exhausted async results stay exhausted. Use `await iterator.aclose()` when
explicitly ending an iterator early.

`await session.aexecute_graph(...)` wraps upstream `execute_graph_async()` for
DSE Graph queries and returns the same async result interface. Continuous graph
paging uses worker threads to consume upstream's blocking generator.

Use `await session.aexecute_concurrent()` to execute multiple statements with
bounded concurrency. It accepts the same arguments as upstream
`execute_concurrent_async()` and returns its ordered list of
`ExecutionResult(success, result_or_exc)` entries. By default,
`raise_on_first_error=False`, so inspect each entry for errors; pass `True` to
raise an execution error instead. Successful entries contain `AsyncResultSet`
objects with the same async iteration interface.

```python
prepared = session.prepare("SELECT * FROM movies_to_watch WHERE genre = ?")
results = await session.aexecute_concurrent(
    [(prepared, ["Action"]), (prepared, ["Black Comedy"])],
    concurrency=2,
    raise_on_first_error=True,
)
for success, rows in results:
    async for row in rows:
        print(row)
```

Cancelling `aexecute_concurrent()` stops waiting, while submitted queries continue
running and the upstream future remains active. Empty input returns `[]`;
`concurrency` must be a positive integer. Upstream submission runs in a worker
thread because it can block even though the upstream method is named async.

Connecting, preparing statements, and shutting down remain synchronous driver
operations. Cancelling an await does not cancel the query on the server.
Always shut down the cluster when finished, for example with `with Cluster(...)`.

All other Cassandra classes should continue to be imported from the usual paths.

For example:
```python
import csv
import asyncio
from cassandra_asyncio.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory


async def main():
    with open("movies.csv", "w", newline="") as output_file:
        # Prepare CSV writer
        fieldnames = ["genre", "name", "rating", "seen"]
        writer = csv.DictWriter(output_file, fieldnames=fieldnames, dialect="excel")
        writer.writeheader()

        # Connect to Cassandra using `cassandra_asyncio.cluster.Cluster`
        auth_provider = PlainTextAuthProvider(
            username="cassandra", password="cassandra"
        )
        with Cluster(auth_provider=auth_provider) as cluster:
            session = cluster.connect("lists")
            session.row_factory = dict_factory

            # Execute query asynchronously using `session.aexecute()`
            rows = await session.aexecute(
                "SELECT genre, name, rating, seen FROM movies_to_watch"
            )

            # Write rows to output file
            async for row in rows:
                writer.writerow(row)


if __name__ == "__main__":
    asyncio.run(main())
```

## Development

Install [PDM](https://pdm-project.org/) and use Python 3.10 or newer:

```shell
pdm sync -G test -G lint
pdm run lint       # Ruff lint checks and formatting checks
pdm run format     # Apply safe lint fixes and formatting
pdm run test       # Unit tests; no Cassandra server needed
pdm run check      # Lint, formatting, and unit tests together
```

CI runs these checks on Python 3.10 through 3.14 before building the package.
Generated version files are excluded from Ruff.

### Integration tests with Docker

With Docker installed and running, start a disposable instance using the
[official Cassandra image](https://hub.docker.com/_/cassandra). Port 9042 must be
available on the host. Run the tests from your host, in the project directory.

```shell
docker volume create cassandra-asyncio-data
docker run -d \
    --name cassandra-asyncio-tests \
    -v cassandra-asyncio-data:/var/lib/cassandra \
    --env MAX_HEAP_SIZE=1G \
    --env HEAP_NEWSIZE=256M \
    -p 127.0.0.1:9042:9042 \
    cassandra:5.0.9
```

Startup can take a few minutes. Wait until Cassandra accepts CQL queries:

```shell
until docker exec cassandra-asyncio-tests \
    cqlsh -e 'SELECT release_version FROM system.local;' >/dev/null 2>&1; do
    sleep 5
done
```

If it keeps waiting, press Ctrl+C and inspect `docker logs cassandra-asyncio-tests`.
Allow Docker enough memory for Cassandra and its JVM (at least 2 GB).

Create the keyspace, then run the integration tests:

```shell
docker exec -i cassandra-asyncio-tests cqlsh <<'CQL'
CREATE KEYSPACE IF NOT EXISTS lists
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CQL

pdm sync -G test -G lint
pdm run test-integration
```

The image's default configuration disables authentication. The tests supply
`cassandra` / `cassandra` credentials, which are ignored in this configuration;
the driver may log a warning about it. For an existing local server with password
authentication enabled, those credentials must be valid.

These tests create, truncate, and drop the `lists.movies_to_watch` table; use a
disposable development database and run the whole file in order.

When finished, remove the container and its anonymous data volume:

```shell
docker rm --force --volumes cassandra-asyncio-tests
```
