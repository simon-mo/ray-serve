import time
import asyncio

import pytest

import ray
from serve.async_task import AsyncLoopWorkerActor


def test_async_worker(ray_instance):
    worker = AsyncLoopWorkerActor.remote()
    
    # make sure the worker is ready
    ray.get(worker.is_ready.remote())

    async def sleep_and_return(something):
        start = time.time()
        await asyncio.sleep(1)
        end = time.time()
        return something, start, end
    
    ten_coroutines = [worker.submit.remote(sleep_and_return, i) for i in range(10)]
    ten_coroutines_oids_bytes = ray.get(ten_coroutines)
    ten_coroutines_oids = [ray.ObjectID(b) for b in ten_coroutines_oids_bytes]
    final_result = ray.get(ten_coroutines_oids)

    # sanity check for result
    assert [result[0] for result in final_result] == list(range(10))

    # we ran 10 coroutines and each sleep 1s. Total runtime shouldn't be 10s
    earlist_start = min(r[1] for r in final_result)
    latest_end = max(r[2] for r in final_result)
    total_duration = latest_end - earlist_start
    assert total_duration < 10

    # make sure we have the current result
    assert sum(r[0] for r in final_result) == sum(range(10))

def test_async_worker_exception_handling(ray_instance):
    worker = AsyncLoopWorkerActor.remote()

    def throw():
        raise Exception("")
    
    ray.get(worker.is_ready.remote())

    with pytest.raises(ray.exceptions.RayTaskError):
        fut_oid = ray.ObjectID(ray.get(worker.submit.remote(throw)))
        ray.get(fut_oid) # should throw
    