# Cassandra AsyncIO Driver

Wrapper for `cassandra-driver` that makes blocking calls awaitable using `asyncio`

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

All other Cassandra classes should continue to be imported from the usual paths.

For example:

```python
import csv
import asyncio
from cassandra_asyncio.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory

async def main():
    with open('movies.csv', 'w', newline='') as output_file:
        # Prepare CSV writer
        fieldnames = ['genre', 'name', 'rating', 'seen']
        writer = csv.DictWriter(output_file, fieldnames=fieldnames, dialect='excel')
        writer.writeheader()

        # Connect to Cassandra using `cassandra_asyncio.cluster.Cluster`
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(auth_provider=auth_provider)
        session = cluster.connect('lists')
        session.row_factory = dict_factory

        # Execute query asynchronously using `session.aexecute()`
        rows = await session.aexecute('SELECT genre, name, rating, seen FROM movies_to_watch')

        # Write rows to output file
        for row in rows:
            writer.writerow(row)

if __name__ == '__main__':
    asyncio.run(main())
```
