import asyncio
import json
import os
import sys
import time
import logging

import uvicorn

import ray
from ray.experimental.async_api import _async_init, as_future, init
from ray.experimental.named_actors import get_actor, register_actor
from serve.utils import BytesEncoder, logger


class Response:
    def __init__(self, content=None, status_code=200):
        self.body = self.render(content)
        self.status_code = status_code
        self.raw_headers = [[b"content-type", b"application/json"]]

    def render(self, content):
        if content is None:
            return b""
        if isinstance(content, bytes):
            return content
        if isinstance(content, (dict, list)):
            return json.dumps(content, cls=BytesEncoder, indent=2).encode()
        return content.encode()

    async def __call__(self, scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": self.raw_headers,
            }
        )
        await send({"type": "http.response.body", "body": self.body})


class HTTPProxy:
    def __init__(self):
        ray.init(redis_address=os.environ["RAY_ADDRESS"])
        self.admin_actor = get_actor(os.environ["RAY_SERVE_ADMIN_NAME"])
        self.router = get_actor(os.environ["RAY_ROUTER_NAME"])

    async def route_checker(self, interval):
        while True:
            try:
                self.svcs = await as_future(self.admin_actor.list_service.remote())
            except ray.exceptions.RayletError:  # Handle termination
                return
            # logger.debug("Updated Routing Table: %s", self.svcs)
            await asyncio.sleep(interval)

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            await _async_init()
            asyncio.ensure_future(self.route_checker(interval=2))
            return

        if scope["path"] == "/":
            await Response(self.svcs)(scope, receive, send)
        elif scope["path"] in self.svcs:
            endpoint_name = self.svcs[scope["path"]]
            result_oid_bytes = await as_future(
                self.router.produce.remote(endpoint_name, scope)
            )
            result = await as_future(ray.ObjectID(result_oid_bytes))
            await Response({"result": result})(scope, receive, send)
        else:
            await Response({"error": "path not found"})(scope, receive, send)


app = HTTPProxy()
