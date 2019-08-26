import random
from collections import defaultdict, deque
from typing import Any

from dataclasses import dataclass
from dataclasses_json import dataclass_json
import numpy as np

import ray
from serve.utils import logger, get_custom_object_id


@dataclass
class Query:
    request_body: Any
    result_oid: ray.ObjectID

    @staticmethod
    def new(req: Any):
        return Query(request_body=req, result_oid=get_custom_object_id())


@dataclass
class WorkIntent:
    work_oid: ray.ObjectID

    @staticmethod
    def new():
        return WorkIntent(work_oid=get_custom_object_id())


class CentralizedQueues:
    def __init__(self):
        # svc_name -> queue
        self.queues = defaultdict(deque)

        # svc_name -> backend_name
        self.traffic = defaultdict(dict)

        # backend_name -> queue
        self.workers = defaultdict(deque)

    def produce(self, svc, req):
        # logger.debug("Producer %s", svc)
        query = Query.new(req)
        self.queues[svc].append(query)
        self.flush()
        return query.result_oid.binary()

    def consume(self, backend):
        # logger.debug("Consumer %s", backend)
        intention = WorkIntent.new()
        self.workers[backend].append(intention)
        self.flush()
        return intention.work_oid.binary()

    def link(self, svc, backend):
        logger.debug("Link %s with %s", svc, backend)
        self.traffic[svc][backend] = 1.0
        self.flush()
    
    def set_traffic(self, svc, traffic_dict):
        logger.debug("Setting traffic for service %s to %s", svc, traffic_dict)
        self.traffic[svc] = traffic_dict
        self.flush()

    def flush(self):
        self._flush()

    def _get_available_backends(self, svc):
        backends_in_policy = set(self.traffic[svc].keys())
        available_workers = set(
            (backend for backend, queues in self.workers.items() if len(queues) > 0)
        )
        return list(backends_in_policy.intersection(available_workers))

    def _flush(self):
        for svc, queue in self.queues.items():
            ready_backends = self._get_available_backends(svc)

            while len(queue) and len(ready_backends):
                # fast track, only one backend available
                if len(ready_backends) == 1:
                    backend = ready_backends[0]
                    req, work = queue.popleft(), self.workers[backend].popleft()
                    ray.worker.global_worker.put_object(work.work_oid, req)

                # roll a dice among the rest
                else:
                    backend_weights = np.array([
                        self.traffic[svc][backend_name]
                        for backend_name in ready_backends
                    ])
                    # normalize the weights to 1
                    backend_weights /= backend_weights.sum()
                    chosen_backend = np.random.choice(ready_backends, p=backend_weights)
                    chosen_backend = chosen_backend.squeeze()
                    req, work = queue.popleft(), self.workers[chosen_backend].popleft()
                    ray.worker.global_worker.put_object(work.work_oid, req)

                ready_backends = self._get_available_backends(svc)


@ray.remote
class CentralizedQueuesActor(CentralizedQueues):
    self_handle = None

    def register_self_handle(self, handle_to_this_actor):
        self.self_handle = handle_to_this_actor

    def flush(self):
        if self.self_handle:
            self.self_handle._flush.remote()
        else:
            self._flush()
