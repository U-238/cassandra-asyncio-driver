"""Optional read-only smoke test against an existing DSE Graph server."""

import asyncio
import os

import pytest
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import GraphExecutionProfile
from cassandra.datastax.graph import GraphOptions

from cassandra_asyncio.cluster import Cluster

pytestmark = pytest.mark.integration


def test_live_graph():
    host = os.environ.get("DSE_GRAPH_HOST")
    graph = os.environ.get("DSE_GRAPH_NAME")
    if not host or not graph:
        pytest.skip(
            "Set DSE_GRAPH_HOST and DSE_GRAPH_NAME for live DSE Graph validation"
        )
    auth = None
    if os.environ.get("DSE_GRAPH_USERNAME"):
        auth = PlainTextAuthProvider(
            username=os.environ["DSE_GRAPH_USERNAME"],
            password=os.environ["DSE_GRAPH_PASSWORD"],
        )
    profile = GraphExecutionProfile(graph_options=GraphOptions(graph_name=graph))
    with Cluster(
        [host],
        port=int(os.environ.get("DSE_GRAPH_PORT", "9042")),
        auth_provider=auth,
        execution_profiles={"graph-smoke": profile},
    ) as cluster:
        session = cluster.connect()

        async def run():
            rows = await session.aexecute_graph(
                "g.inject(1, 2)", execution_profile="graph-smoke"
            )
            assert [row.value async for row in rows] == [1, 2]

        asyncio.run(run())
