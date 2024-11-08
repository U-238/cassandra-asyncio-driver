import io
import sys
import csv
import pytest
import asyncio

from cassandra_asyncio.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
from cassandra import InvalidRequest


def _get_session():
    auth_provider = PlainTextAuthProvider(username='root', password='cassandra')
    cluster = Cluster(auth_provider=auth_provider)
    session = cluster.connect('lists')
    session.row_factory = dict_factory
    return session

def _get_csv_writer(fieldnames):
    csvfile = io.StringIO(newline='')
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect='excel')
    writer.writeheader()
    return csvfile, writer

async def _test_create_table():
    session = _get_session()

    query = "DROP TABLE IF EXISTS movies_to_watch"
    await session.aexecute(query)

    query = "CREATE TABLE movies_to_watch (" \
                "genre text, " \
                "name text, " \
                "rating int, " \
                "seen boolean, " \
                "PRIMARY KEY (genre, name) " \
            ")"
    await session.aexecute(query)

def test_create_table():
    asyncio.run(_test_create_table())

async def _test_insert():
    session = _get_session()

    query = "INSERT INTO movies_to_watch (genre, name, rating, seen) " \
              "VALUES (%s, %s, %s, %s)"

    await session.aexecute(query, parameters=["Action", "Cyborg", None, False])
    await session.aexecute(query, parameters=["Black Comedy", "Dr Strangelove", 10, True])

def test_insert():
    asyncio.run(_test_insert())

async def _test_select():
    session = _get_session()

    fieldnames = ['genre', 'name', 'rating', 'seen']
    csvfile, writer = _get_csv_writer(fieldnames)

    rows = await session.aexecute('SELECT genre, name, rating, seen FROM movies_to_watch')
    for row in rows:
        writer.writerow(row)

    expected_csv = "genre,name,rating,seen\r\nAction,Cyborg,,False\r\nBlack Comedy,Dr Strangelove,10,True\r\n"
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

    query = "INSERT INTO movies_to_watch (genre, name, rating, seen) " \
              "VALUES (%s, %s, %s, %s)"
    prepared = session.prepare(query)

    await session.aexecute(prepared, parameters=["Action", "Cyborg", None, False])
    await session.aexecute(prepared, parameters=["Black Comedy", "Dr Strangelove", 10, True])

def test_prepared_insert():
    asyncio.run(_test_insert())

async def _test_prepared_select():
    session = _get_session()

    fieldnames = ['genre', 'name', 'rating', 'seen']
    csvfile, writer = _get_csv_writer(fieldnames)

    prepared = session.prepare('SELECT genre, name, rating, seen FROM movies_to_watch')
    rows = await session.aexecute(prepared)
    #rows = session.execute(prepared)
    for row in rows:
        writer.writerow(row)

    expected_csv = "genre,name,rating,seen\r\nAction,Cyborg,,False\r\nBlack Comedy,Dr Strangelove,10,True\r\n"
    assert csvfile.getvalue() == expected_csv

def test_prepared_select():
    asyncio.run(_test_prepared_select())

async def _test_exception():
    session = _get_session()

    with pytest.raises(InvalidRequest):
        await session.aexecute('SELECT non, existent, columns FROM non_existent_table')

def test_exception():
    asyncio.run(_test_exception())

if __name__ == '__main__':
    sys.exit(pytest.main())
