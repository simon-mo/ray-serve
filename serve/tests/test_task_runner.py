import pytest

import ray
from ray.experimental import register_actor
from serve.queues import CentralizedQueuesActor
from serve.task_runner import TaskRunner, TaskRunnerActor, wrap_to_ray_error


def test_runner_basic():
    echo = lambda i: i
    r = TaskRunner(echo)
    assert r(1) == 1


def test_runner_wraps_error():
    echo = lambda i: i
    assert wrap_to_ray_error(echo, 2) == 2

    error = lambda _: 1 / 0
    assert isinstance(wrap_to_ray_error(error, 1), ray.exceptions.RayTaskError)


def test_runner_actor(serve_instance):
    q = CentralizedQueuesActor.remote()
    echo = lambda i: i

    QUEUE_NAME = "q"
    CONSUMER_NAME = "runner"
    PRODUCER_NAME = "prod"

    runner = TaskRunnerActor.remote(echo)

    register_actor(QUEUE_NAME, q)
    register_actor(CONSUMER_NAME, runner)

    runner.setup.remote(my_name=CONSUMER_NAME, router_name=QUEUE_NAME)
    runner.main_loop.remote()

    q.link.remote(PRODUCER_NAME, CONSUMER_NAME)

    for query in [333, 444, 555]:
        result_token = ray.ObjectID(ray.get(q.produce.remote(PRODUCER_NAME, query)))
        assert ray.get(result_token) == query
