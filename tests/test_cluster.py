import asyncio
from concurrent.futures import Future
from unittest.mock import Mock

import pytest
from cassandra.cluster import Cluster as DriverCluster
from cassandra.cluster import ResponseFuture, ResultSet
from cassandra.concurrent import ExecutionResult
from cassandra.query import SimpleStatement, dict_factory

from cassandra_asyncio.cluster import (
    Cluster,
    _aexecute,
    _aexecute_concurrent,
    _aexecute_graph,
)


def response_future():
    session = Mock()
    session.row_factory = dict_factory
    session.cluster._default_load_balancing_policy.make_query_plan.return_value = iter(
        []
    )
    return ResponseFuture(
        session, Mock(), SimpleStatement("SELECT * FROM movies"), None
    )


@pytest.mark.parametrize("in_thread", [False, True])
@pytest.mark.parametrize("fails", [False, True])
def test_completion(in_thread, fails):
    async def run():
        driver_future = response_future()
        session = Mock()
        session.execute_async.return_value = driver_future
        error = ValueError("query failed")
        rows = [{"name": "Cyborg"}]
        driver_future._col_names = ["name"]
        driver_future._col_types = ["text"]

        def complete():
            if fails:
                driver_future._set_final_exception(error)
            else:
                driver_future._set_final_result(rows)

        if in_thread:
            task = asyncio.create_task(_aexecute(session, "query", timeout=2))
            await asyncio.sleep(0)
            await asyncio.to_thread(complete)
        else:
            # Upstream invokes callbacks immediately for finished queries.
            complete()
            task = asyncio.create_task(_aexecute(session, "query", timeout=2))

        if fails:
            with pytest.raises(ValueError) as raised:
                await task
            assert raised.value is error
        else:
            result = await task
            assert isinstance(result, ResultSet)
            assert result.current_rows == rows
            assert result.column_names == ["name"]
            assert result.response_future is driver_future
        session.execute_async.assert_called_once_with("query", timeout=2)

    asyncio.run(run())


@pytest.mark.parametrize("fails", [False, True])
def test_late_completion_after_cancellation(fails):
    async def run():
        errors = []
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, ctx: errors.append(ctx)
        )
        driver_future = response_future()
        session = Mock(execute_async=Mock(return_value=driver_future))
        task = asyncio.create_task(_aexecute(session, "query"))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        if fails:
            await asyncio.to_thread(
                driver_future._set_final_exception, ValueError("late")
            )
        else:
            await asyncio.to_thread(driver_future._set_final_result, [])
        await asyncio.sleep(0)
        assert errors == []

    asyncio.run(run())


def test_connect_binds_aexecute(monkeypatch):
    session = Mock()
    connect = Mock(return_value=session)
    monkeypatch.setattr(DriverCluster, "connect", connect)
    cluster = Cluster.__new__(Cluster)
    assert cluster.connect("lists", wait_for_all_pools=True) is session
    connect.assert_called_once_with("lists", wait_for_all_pools=True)
    assert session.aexecute_graph.__func__ is _aexecute_graph
    assert session.aexecute.__self__ is session
    assert session.aexecute.__func__ is _aexecute
    assert session.aexecute_concurrent.__self__ is session
    assert session.aexecute_concurrent.__func__ is _aexecute_concurrent


def test_execute_async_exception():
    session = Mock()
    session.execute_async.side_effect = ValueError("invalid query")
    with pytest.raises(ValueError, match="invalid query"):
        asyncio.run(_aexecute(session, "query"))


def test_result_set_paging_does_not_complete_asyncio_future_twice():
    async def run():
        errors = []
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, ctx: errors.append(ctx)
        )
        driver_future = response_future()
        driver_future._paging_state = b"next-page"
        driver_future._set_final_result([{"name": "first"}])
        session = Mock(execute_async=Mock(return_value=driver_future))
        result = await _aexecute(session, "query")
        assert result.has_more_pages
        assert result.paging_state == b"next-page"

        def next_page():
            driver_future._paging_state = None
            driver_future._set_final_result([{"name": "second"}])

        driver_future.start_fetching_next_page = next_page
        assert await result.aall() == [{"name": "first"}, {"name": "second"}]
        await asyncio.sleep(0)
        assert errors == []

    asyncio.run(run())


@pytest.mark.parametrize("in_thread", [False, True])
@pytest.mark.parametrize("fails", [False, True])
def test_concurrent_completion(in_thread, fails):
    async def run():
        driver_future = Future()
        session = Mock(execute_concurrent_async=Mock(return_value=driver_future))
        statements = [("query", [1]), ("query", [2])]
        results = [
            ExecutionResult(True, [{"id": 1}]),
            ExecutionResult(False, ValueError("individual error")),
        ]
        error = ValueError("execution failed")

        def complete():
            if fails:
                driver_future.set_exception(error)
            else:
                driver_future.set_result(results)

        if in_thread:
            task = asyncio.create_task(
                _aexecute_concurrent(
                    session,
                    statements,
                    concurrency=2,
                    raise_on_first_error=True,
                    execution_profile="custom",
                )
            )
            await asyncio.sleep(0)
            await asyncio.to_thread(complete)
        else:
            complete()
            task = asyncio.create_task(
                _aexecute_concurrent(
                    session,
                    statements,
                    concurrency=2,
                    raise_on_first_error=True,
                    execution_profile="custom",
                )
            )

        if fails:
            with pytest.raises(ValueError) as raised:
                await task
            assert raised.value is error
        else:
            assert await task == results
        forwarded = session.execute_concurrent_async.call_args.args[0]
        assert list(forwarded) == statements
        session.execute_concurrent_async.assert_called_once_with(
            forwarded,
            concurrency=2,
            raise_on_first_error=True,
            execution_profile="custom",
        )

    asyncio.run(run())


@pytest.mark.parametrize("fails", [False, True])
def test_concurrent_cancellation_leaves_upstream_future_running(fails):
    async def run():
        driver_future = Future()
        session = Mock(execute_concurrent_async=Mock(return_value=driver_future))
        errors = []
        asyncio.get_running_loop().set_exception_handler(
            lambda loop, ctx: errors.append(ctx)
        )
        task = asyncio.create_task(_aexecute_concurrent(session, [("query", None)]))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not driver_future.cancelled()
        if fails:
            await asyncio.to_thread(driver_future.set_exception, ValueError("late"))
        else:
            await asyncio.to_thread(driver_future.set_result, [])
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert errors == []

    asyncio.run(run())


def test_concurrent_submission_exception():
    session = Mock()
    session.execute_concurrent_async.side_effect = ValueError("invalid statements")
    with pytest.raises(ValueError, match="invalid statements"):
        asyncio.run(_aexecute_concurrent(session, [("q", None)]))


@pytest.mark.parametrize("fails", [False, True])
def test_graph_completion(fails):
    async def run():
        driver_future = response_future()
        session = Mock(execute_graph_async=Mock(return_value=driver_future))
        if fails:
            driver_future._set_final_exception(ValueError("graph failed"))
            with pytest.raises(ValueError, match="graph failed"):
                await _aexecute_graph(session, "g.V()", execution_profile="graph")
        else:
            driver_future._set_final_result([{"vertex": 1}])
            rows = await _aexecute_graph(session, "g.V()", execution_profile="graph")
            assert await rows.aall() == [{"vertex": 1}]
            assert list(rows) == [{"vertex": 1}]
        session.execute_graph_async.assert_called_once_with(
            "g.V()", execution_profile="graph"
        )

    asyncio.run(run())


@pytest.mark.parametrize("fails", [False, True])
def test_async_paging_empty_pages_and_errors(fails):
    async def run():
        driver_future = response_future()
        driver_future._paging_state = b"next"
        driver_future._set_final_result([])
        result = await _aexecute(
            Mock(execute_async=Mock(return_value=driver_future)), "q"
        )
        pages = iter([[], [{"id": 1}]])
        calls = []

        def next_page():
            calls.append(True)
            rows = next(pages)
            driver_future._paging_state = b"next" if not rows else None
            if fails:
                asyncio.get_running_loop().call_soon(
                    driver_future._set_final_exception, ValueError("page failed")
                )
            else:
                asyncio.get_running_loop().call_soon(
                    driver_future._set_final_result, rows
                )

        driver_future.start_fetching_next_page = next_page
        if fails:
            with pytest.raises(ValueError, match="page failed"):
                await result.aall()
        else:
            assert await result.aall() == [{"id": 1}]
            assert len(calls) == 2

    asyncio.run(run())


def test_cancelled_page_fetch_can_be_awaited_again():
    async def run():
        driver_future = response_future()
        driver_future._paging_state = b"next"
        driver_future._set_final_result([1])
        result = await _aexecute(
            Mock(execute_async=Mock(return_value=driver_future)), "q"
        )
        driver_future.start_fetching_next_page = Mock()
        task = asyncio.create_task(result.afetch_next_page())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        driver_future._paging_state = None
        await asyncio.to_thread(driver_future._set_final_result, [2])
        await result.afetch_next_page()
        assert result.current_rows == [2]
        driver_future.start_fetching_next_page.assert_called_once()

    asyncio.run(run())


def test_continuous_graph_rows():
    async def run():
        driver_future = response_future()
        driver_future._continuous_paging_session = Mock()
        driver_future._set_final_result(iter([1, 2]))
        rows = await _aexecute_graph(
            Mock(execute_graph_async=Mock(return_value=driver_future)), "g.V()"
        )
        assert await rows.aall() == [1, 2]

    asyncio.run(run())


@pytest.mark.parametrize("operation", ["iterate", "all", "fetch_next_page"])
def test_synchronous_result_set_compatibility(operation):
    async def run():
        driver_future = response_future()
        driver_future._paging_state = b"next"
        driver_future._set_final_result([1])
        result = await _aexecute(
            Mock(execute_async=Mock(return_value=driver_future)), "q"
        )

        def next_page():
            driver_future._paging_state = None
            driver_future._set_final_result([2])

        driver_future.start_fetching_next_page = next_page
        if operation == "iterate":
            assert list(result) == [1, 2]
        elif operation == "all":
            assert result.all() == [1, 2]
        else:
            result.fetch_next_page()
            assert result.current_rows == [2]
        assert not result.has_more_pages

    asyncio.run(run())


@pytest.mark.parametrize("concurrency", [0, -1, False, 1.5])
def test_invalid_concurrency(concurrency):
    session = Mock()
    with pytest.raises(ValueError, match="positive integer"):
        asyncio.run(_aexecute_concurrent(session, [], concurrency=concurrency))
    session.execute_concurrent_async.assert_not_called()


@pytest.mark.parametrize("statements", [[], iter([])])
def test_empty_concurrent_input(statements):
    session = Mock()
    assert asyncio.run(_aexecute_concurrent(session, statements)) == []
    session.execute_concurrent_async.assert_not_called()


def test_real_upstream_concurrent_submission_does_not_block_loop():
    from threading import Event, Timer

    from cassandra.concurrent import execute_concurrent_async

    async def run():
        response = response_future()
        session = response.session
        entered = Event()

        def submit(*args, **kwargs):
            entered.set()
            return response

        session.execute_async.side_effect = submit
        session.execute_concurrent_async.side_effect = lambda *args, **kwargs: (
            execute_concurrent_async(session, *args, **kwargs)
        )
        watchdog = Timer(2, response._set_final_result, args=([1],))
        watchdog.start()
        task = asyncio.create_task(_aexecute_concurrent(session, [("q", None)]))
        try:
            # If submission blocks the loop, this coroutine cannot release the query.
            assert await asyncio.to_thread(entered.wait, 2)
            assert not response._event.is_set()
            assert not task.done()
        finally:
            watchdog.cancel()
            response._set_final_result([1])
        results = await asyncio.wait_for(task, 2)
        assert await results[0].result_or_exc.aall() == [1]

    asyncio.run(run())


def test_async_iteration_resumes_and_stays_exhausted():
    async def run():
        response = response_future()
        response._set_final_result([1, 2, 3])
        rows = await _aexecute(Mock(execute_async=Mock(return_value=response)), "q")
        iterator = rows.__aiter__()
        assert await anext(iterator) == 1
        await iterator.aclose()
        assert await rows.aall() == [2, 3]
        assert await rows.aall() == []
        # The synchronous snapshot API keeps upstream semantics.
        assert rows.current_rows == [1, 2, 3]

    asyncio.run(run())


def test_continuous_graph_cancellation_does_not_lose_row():
    from threading import Event

    async def run():
        started, release = Event(), Event()

        def source():
            started.set()
            release.wait(2)
            yield 1
            yield 2

        response = response_future()
        response._continuous_paging_session = Mock()
        response._set_final_result(source())
        rows = await _aexecute_graph(
            Mock(execute_graph_async=Mock(return_value=response)), "g.V()"
        )
        iterator = rows.__aiter__()
        task = asyncio.create_task(anext(iterator))
        try:
            assert await asyncio.to_thread(started.wait, 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
        assert await rows.aall() == [1, 2]
        assert await rows.aall() == []

    asyncio.run(run())
