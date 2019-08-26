import ray
from serve.async_task import AsyncLoopWorker

class Supervisor:
    def __init__(self, kv_store_actor_handle):
        self.actor_nursery = []
        self.orphan_object_ids = []

        self.loop_worker = AsyncLoopWorker()
        self.futures_in_asyncio_loop = []

        self.kv_store_actor = kv_store_actor_handle
    
    def watch(self, namespace, ):
        pass