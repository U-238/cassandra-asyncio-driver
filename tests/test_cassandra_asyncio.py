import asyncio
import csv
import io
import sys

import pytest
from cassandra import InvalidRequest
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, dict_factory

from cassandra_asyncio.cluster import Cluster

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def close_clusters(monkeypatch):
    clusters = []
    original_init = Cluster.__init__

    def tracked_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        clusters.append(self)

    monkeypatch.setattr(Cluster, "__init__", tracked_init)
    yield
    for cluster in clusters:
        cluster.shutdown()


def _get_session():
    auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
    cluster = Cluster(auth_provider=auth_provider)
    session = cluster.connect("lists")
    session.row_factory = dict_factory
    return session


def _get_csv_writer(fieldnames):
    csvfile = io.StringIO(newline="")
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect="excel")
    writer.writeheader()
    return csvfile, writer


async def _test_create_table():
    session = _get_session()

    query = "DROP TABLE IF EXISTS movies_to_watch"
    await session.aexecute(query)

    query = (
        "CREATE TABLE movies_to_watch ("
        "genre text, "
        "name text, "
        "rating int, "
        "seen boolean, "
        "PRIMARY KEY (genre, name) "
        ")"
    )
    await session.aexecute(query)


def test_create_table():
    asyncio.run(_test_create_table())


async def _test_insert():
    session = _get_session()

    query = (
        "INSERT INTO movies_to_watch (genre, name, rating, seen) "
        "VALUES (%s, %s, %s, %s)"
    )

    await session.aexecute(query, parameters=["Action", "Cyborg", None, False])
    await session.aexecute(
        query, parameters=["Black Comedy", "Dr Strangelove", 10, True]
    )


def test_insert():
    asyncio.run(_test_insert())


async def _test_select():
    session = _get_session()

    fieldnames = ["genre", "name", "rating", "seen"]
    csvfile, writer = _get_csv_writer(fieldnames)

    rows = await session.aexecute(
        "SELECT genre, name, rating, seen FROM movies_to_watch"
    )
    async for row in rows:
        writer.writerow(row)

    expected_csv = (
        "genre,name,rating,seen\r\n"
        "Action,Cyborg,,False\r\n"
        "Black Comedy,Dr Strangelove,10,True\r\n"
    )
    assert csvfile.getvalue() == expected_csv


def test_select():
    asyncio.run(_test_select())


async def _test_truncate():
    session = _get_session()

    await session.aexecute("TRUNCATE movies_to_watch")


def test_truncate():
    asyncio.run(_test_truncate())


async def _test_prepared_insert():
    session = _get_session()

    query = (
        "INSERT INTO movies_to_watch (genre, name, rating, seen) VALUES (?, ?, ?, ?)"
    )
    prepared = session.prepare(query)

    await session.aexecute(prepared, parameters=["Action", "Cyborg", None, False])
    await session.aexecute(
        prepared, parameters=["Black Comedy", "Dr Strangelove", 10, True]
    )


def test_prepared_insert():
    asyncio.run(_test_prepared_insert())


async def _test_prepared_select():
    session = _get_session()

    fieldnames = ["genre", "name", "rating", "seen"]
    csvfile, writer = _get_csv_writer(fieldnames)

    prepared = session.prepare("SELECT genre, name, rating, seen FROM movies_to_watch")
    rows = await session.aexecute(prepared)
    async for row in rows:
        writer.writerow(row)

    expected_csv = (
        "genre,name,rating,seen\r\n"
        "Action,Cyborg,,False\r\n"
        "Black Comedy,Dr Strangelove,10,True\r\n"
    )
    assert csvfile.getvalue() == expected_csv


def test_prepared_select():
    asyncio.run(_test_prepared_select())


async def _test_exception():
    session = _get_session()

    with pytest.raises(InvalidRequest):
        await session.aexecute("SELECT non, existent, columns FROM non_existent_table")


def test_exception():
    asyncio.run(_test_exception())


def test_concurrent():
    async def run():
        session = _get_session()
        prepared = session.prepare(
            "SELECT genre, name FROM lists.movies_to_watch WHERE genre = ?"
        )
        results = await session.aexecute_concurrent(
            [(prepared, ["Action"]), (prepared, ["Black Comedy"])],
            concurrency=2,
            raise_on_first_error=True,
        )
        assert all(result.success for result in results)
        assert [
            row["name"]
            for result in results
            for row in await result.result_or_exc.aall()
        ] == [
            "Cyborg",
            "Dr Strangelove",
        ]

        results = await session.aexecute_concurrent(
            [
                ("SELECT release_version FROM system.local", None),
                ("SELECT * FROM lists.non_existent_table", None),
            ],
            concurrency=2,
        )
        assert results[0].success
        assert not results[1].success
        assert isinstance(results[1].result_or_exc, InvalidRequest)

    asyncio.run(run())


def test_async_pagination():
    async def run():
        session = _get_session()
        rows = await session.aexecute(
            SimpleStatement(
                "SELECT genre, name FROM lists.movies_to_watch", fetch_size=1
            )
        )
        assert len(rows.current_rows) == 1
        assert rows.has_more_pages
        names = [row["name"] async for row in rows]
        assert sorted(names) == ["Cyborg", "Dr Strangelove"]

    asyncio.run(run())


if __name__ == "__main__":
    sys.exit(pytest.main())
