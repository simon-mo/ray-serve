import asyncio
import ray
import inspect
from threading import Thread
import time
import sys
import traceback

# loop = asyncio.new_event_loop()


def create_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def unit_of_work(result_oid_bytes, func, *args):
    result_oid = ray.ObjectID(result_oid_bytes)
    try:
        result = await func(*args)
    except Exception as e:
        print(e)
        traceback_str = traceback.format_exc()
        result = ray.exceptions.RayTaskError(str(func), traceback_str)
    print(f"result is {result}")
    ray.worker.global_worker.put_object(result_oid, result)


def get_new_oid():
    worker = ray.worker.global_worker
    object_id = ray._raylet.compute_put_id(
        worker.current_task_id, worker.task_context.put_index
    )
    worker.task_context.put_index += 1
    return object_id

class AsyncLoopWorker:
    def __init__(self):
        self.loop = asyncio.new_event_loop()

        self.main_loop_thread = Thread(
            target=create_loop, args=(self.loop,), daemon=True
        )
        self.main_loop_thread.start()

    def is_ready(self):
        return True

    def submit(self, func, *args):
        assert inspect.iscoroutinefunction(func)

        new_oid = get_new_oid()

        asyncio.run_coroutine_threadsafe(
            unit_of_work(new_oid.binary(), func, *args), loop=self.loop
        )

        return new_oid.binary()

@ray.remote
class AsyncLoopWorkerActor(AsyncLoopWorker):
    pass