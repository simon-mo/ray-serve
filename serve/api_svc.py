import ray


class RouteServer:
    def __init__(self):
        self.kvs = dict()
        self.request_count = 0

    def register_service(self, route: str, svc: str):
        self.kvs[route] = svc

    def list_service(self):
        self.request_count += 1
        return self.kvs

    def get_request_count(self):
        return self.request_count


@ray.remote
class RouteServerActor(RouteServer):
    pass
