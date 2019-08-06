import random
from collections import defaultdict, deque
from typing import Any

from dataclasses import dataclass
from dataclasses_json import dataclass_json

import ray


def _get_new_oid():
    worker = ray.worker.global_worker
    object_id = ray._raylet.compute_put_id(
        worker.current_task_id, worker.task_context.put_index
    )
    worker.task_context.put_index += 1
    return object_id


@dataclass
class Query:
    request_body: Any
    result_oid: ray.ObjectID

    @staticmethod
    def new(req: Any):
        return Query(request_body=req, result_oid=_get_new_oid())


@dataclass
class WorkIntent:
    work_oid: ray.ObjectID

    @staticmethod
    def new():
        return WorkIntent(work_oid=_get_new_oid())


class CentralizedQueues:
    def __init__(self):
        # svc_name -> queue
        self.queues = defaultdict(deque)

        # svc_name -> backend_name
        self.traffic = defaultdict(dict)

        # backend_name -> queue
        self.workers = defaultdict(deque)

    def produce(self, svc, req):
        print("Producer", svc)
        query = Query.new(req)
        self.queues[svc].append(query)
        self._flush()
        return query.result_oid.binary()

    def consume(self, backend):
        print("Consumer", backend)
        intention = WorkIntent.new()
        self.workers[backend].append(intention)
        self._flush()  # .remote?
        return intention.work_oid.binary()

    def link(self, svc, backend):
        print("Link", svc, backend)
        self.traffic[svc] = backend
        self._flush()

    def _flush(self):
        for svc, queue in self.queues.items():
            backend = self.traffic[svc]
            while len(queue) and len(self.workers[backend]):
                req, work = queue.popleft(), self.workers[backend].popleft()
                ray.worker.global_worker.put_object(work.work_oid, req)


@ray.remote
class CentralizedQueuesActor(CentralizedQueues):
    pass


if __name__ == "__main__":
    ray.init(object_store_memory=int(1e8))

    q = CentralizedQueues()
    q.link("svc", "backend")

    result_oid = q.produce("svc", 1)
    work_oid = q.consume("backend")
    got_work = ray.get(ray.ObjectID(work_oid))
    assert got_work.request_body == 1

    ray.worker.global_worker.put_object(got_work.result_oid, 2)
    assert ray.get(ray.ObjectID(result_oid)) == 2
