from ray.experimental import get_actor


class RayServeHandle:
    def __init__(self, router_name):
        self.router_handle = get_actor(router_name)
