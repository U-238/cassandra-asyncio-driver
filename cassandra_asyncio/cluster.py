import asyncio
from concurrent.futures import Future
from itertools import chain

from cassandra.cluster import EXEC_PROFILE_DEFAULT, ResultSet
from cassandra.cluster import Cluster as DriverCluster

__all__ = ["AsyncResultSet", "Cluster"]


def _handle_result(result, fut):
    if not fut.done():
        fut.set_result(result)


def _handle_error(exc, fut):
    if not fut.done():
        fut.set_exception(exc)


class _ResponseBridge:
    def __init__(self, driver_future):
        self.driver_future = driver_future
        self.future = Future()
        driver_future.add_callback(self._result)
        driver_future.add_errback(self._error)

    def _result(self, rows):
        _handle_result(rows, self.future)

    def _error(self, exc):
        _handle_error(exc, self.future)

    async def wait(self):
        return await asyncio.shield(asyncio.wrap_future(self.future))


def _next_row(iterator):
    try:
        return True, next(iterator)
    except StopIteration:
        return False, None


def _consume_task_exception(task):
    if not task.cancelled():
        task.exception()


class AsyncResultSet(ResultSet):
    """Result metadata and rows with nonblocking async iteration.

    Use one consumer at a time; synchronous operations retain upstream behavior.
    """

    def __init__(self, response_future, rows, bridge=None):
        super().__init__(response_future, rows)
        self._bridge = bridge or _ResponseBridge(response_future)
        self._pending_page = False
        self._async_rows = iter(self.current_rows)
        self._async_exhausted = False
        self._async_active = False
        self._next_task = None

    async def __aiter__(self):
        if self._async_active:
            raise RuntimeError("Only one async consumer may iterate a result at a time")
        self._async_active = True
        try:
            while not self._async_exhausted:
                if self.response_future._continuous_paging_session:
                    # Keep an in-flight generator read across cancellation, so a
                    # worker cannot consume a row that the next iterator loses.
                    if self._next_task is None:
                        self._next_task = asyncio.create_task(
                            asyncio.to_thread(_next_row, self._async_rows)
                        )
                        self._next_task.add_done_callback(_consume_task_exception)
                    available, row = await asyncio.shield(self._next_task)
                    self._next_task = None
                else:
                    available, row = _next_row(self._async_rows)
                if available:
                    yield row
                elif self._pending_page or self.has_more_pages:
                    if self.response_future._continuous_paging_session:
                        self._async_exhausted = True
                    else:
                        await self.afetch_next_page()
                else:
                    self._async_exhausted = True
        finally:
            self._async_active = False

    async def afetch_next_page(self):
        """Fetch the next page asynchronously, replacing current_rows."""
        if not self._pending_page:
            if not self.has_more_pages:
                self._current_rows = []
                return
            self._bridge.future = Future()
            self._pending_page = True
            try:
                self.response_future.start_fetching_next_page()
            except BaseException:
                self._pending_page = False
                raise
        rows = await self._bridge.wait()
        self._set_current_rows(rows)
        self._async_rows = iter(self.current_rows)
        self._async_exhausted = False
        self._pending_page = False

    async def aall(self):
        """Collect all rows using async paging."""
        return [row async for row in self]


async def _await_response(driver_future):
    bridge = _ResponseBridge(driver_future)
    rows = await bridge.wait()
    return AsyncResultSet(driver_future, rows, bridge)


async def _aexecute(session, *args, **kwargs):
    return await _await_response(session.execute_async(*args, **kwargs))


async def _aexecute_graph(session, *args, **kwargs):
    return await _await_response(session.execute_graph_async(*args, **kwargs))


async def _aexecute_concurrent(
    session,
    statements_and_parameters,
    concurrency=100,
    raise_on_first_error=False,
    execution_profile=EXEC_PROFILE_DEFAULT,
):
    """Await concurrent execution without blocking the loop or cancelling queries."""
    if (
        not isinstance(concurrency, int)
        or isinstance(concurrency, bool)
        or concurrency <= 0
    ):
        raise ValueError("concurrency must be a positive integer")
    statements = iter(statements_and_parameters)
    try:
        first = next(statements)
    except StopIteration:
        return []

    async def execute():
        # In upstream 3.30.1, even submission waits for query completion.
        driver_future = await asyncio.to_thread(
            session.execute_concurrent_async,
            chain([first], statements),
            concurrency=concurrency,
            raise_on_first_error=raise_on_first_error,
            execution_profile=execution_profile,
        )
        results = await asyncio.wrap_future(driver_future)
        return [
            result._replace(
                result_or_exc=AsyncResultSet(
                    result.result_or_exc.response_future,
                    result.result_or_exc.current_rows,
                )
            )
            if result.success and isinstance(result.result_or_exc, ResultSet)
            else result
            for result in results
        ]

    # Shield submission as well as the future: queries outlive cancelled awaits.
    task = asyncio.create_task(execute())
    task.add_done_callback(_consume_task_exception)
    return await asyncio.shield(task)


class Cluster(DriverCluster):
    """Upstream Cluster with an awaitable query method on connected sessions."""

    def connect(self, *args, **kwargs):
        session = super().connect(*args, **kwargs)
        session.aexecute = _aexecute.__get__(session)
        session.aexecute_graph = _aexecute_graph.__get__(session)
        session.aexecute_concurrent = _aexecute_concurrent.__get__(session)
        return session
