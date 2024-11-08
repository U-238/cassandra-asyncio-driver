import asyncio
from cassandra.cluster import Cluster as DriverCluster

__all__ = ['Cluster']

def _handle_result(result, fut):
    if not fut.cancelled():
        fut.set_result(result)

def _handle_result_threadsafe(result, loop, fut):
    loop.call_soon_threadsafe(_handle_result, result, fut)

def _handle_error(exc, fut):
    if not fut.cancelled():
        fut.set_exception(exc)

def _handle_error_threadsafe(exc, loop, fut):
    loop.call_soon_threadsafe(_handle_error, exc, fut)

async def _aexecute(session, *args, **kwargs):
    driver_future = session.execute_async(*args, **kwargs)
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    driver_future.add_callback(_handle_result_threadsafe, loop, future)
    driver_future.add_errback(_handle_error_threadsafe, loop, future)
    return await future

class Cluster(DriverCluster):
    def connect(self, *args, **kwargs):
        session = super().connect(*args, **kwargs)
        session.aexecute = _aexecute.__get__(session)
        return session
