import os
import shlex
import sys
import time
from subprocess import STDOUT, Popen, call

import ray

from ray.experimental import register_actor
from serve.api_svc import RouteServerActor
from serve.queues import CentralizedQueuesActor
from serve.task_runner import TaskRunnerActor
from serve.utils import logger

API_SERVICE_NAME = "API"
ROUTER_NAME = "router"


class GlobalState:
    def __init__(self):
        self.actor_nursery = []

        self.api_handle = None
        self.server_proc = None

    def init_api_server(self):
        logger.info("[Global State] Initalizing Routing Table")
        self.api_handle = RouteServerActor.remote()
        logger.info(
            "[Global State] Health Checking Routing Table %s",
            ray.get(self.api_handle.get_request_count.remote()),
        )
        register_actor(API_SERVICE_NAME, self.api_handle)

    def init_http_server(self, redis_addr):
        logger.info("[Global State] Initializing HTTP Server")
        script = "uvicorn server:app"
        new_env = os.environ.copy()
        new_env.update(
            {
                "RAY_SERVE_ADMIN_NAME": API_SERVICE_NAME,
                "RAY_ADDRESS": redis_addr,
                "RAY_ROUTER_NAME": ROUTER_NAME,
            }
        )
        self.server_proc = Popen(
            script,
            stdout=sys.stdout,
            stderr=STDOUT,
            shell=True,
            env=new_env,
            cwd=os.path.split(os.path.abspath(__file__))[0],
        )

    def init_router(self):
        # TODO: sharded later
        logger.info("[Global State] Initializing Queuing System")
        self.router = CentralizedQueuesActor.remote()
        register_actor(ROUTER_NAME, self.router)

    def shutdown(self):
        if self.server_proc:
            self.server_proc.terminate()

        ray.shutdown()

    def __del__(self):
        self.shutdown()

    def wait_until_http_ready(self):
        req_cnt = 0
        retries = 5

        while not req_cnt:
            req_cnt = ray.get(self.api_handle.get_request_count.remote())
            logger.info("[Global State] Making sure HTTP Server is ready.")
            time.sleep(1)
            retries -= 1
            if retries == 0:
                raise Exception("Too many retries, HTTP is not ready")


global_state = GlobalState()


def init(blocking=False):
    redis_addr = ray.init(object_store_memory=int(1e8))["redis_address"]
    global_state.init_api_server()
    global_state.init_http_server(redis_addr)
    global_state.init_router()
    if blocking:
        global_state.wait_until_http_ready()


def create_endpoint(endpoint_name, route_expression, blocking=True):
    fut = global_state.api_handle.register_service.remote(
        route_expression, endpoint_name
    )
    if blocking:
        ray.get(fut)


def create_backend(func, backend_tag):
    runner = TaskRunnerActor.remote(func)
    global_state.actor_nursery.append(runner)

    register_actor(backend_tag, runner)
    runner.setup.remote(my_name=backend_tag, router_name=ROUTER_NAME)
    runner.main_loop.remote()


def link(endpoint_name, backend_tag):
    global_state.router.link.remote(endpoint_name, backend_tag)
