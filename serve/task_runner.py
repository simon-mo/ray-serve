import traceback

import ray


class TaskRunner:
    def __init__(self, func_to_run):
        self.func = func_to_run

    def __call__(self, *args):
        return self.func(*args)


def wrap_to_ray_error(callable_obj, *args):
    try:
        return callable_obj(*args)
    except Exception:
        traceback_str = traceback.format_exc()
        return ray.exceptions.RayTaskError(str(callable_obj), traceback_str)

class RayServeMixin():
    self_handle = None
    router_handle = None
    setup_completed = False
    consumer_name = None

    def setup(self, my_name, router_name):
        self.consumer_name = my_name
        self.self_handle = ray.experimental.get_actor(my_name)
        self.router_handle = ray.experimental.get_actor(router_name)
        self.setup_completed = True

    def main_loop(self):
        assert self.setup_completed

        work_token = ray.get(self.router_handle.consume.remote(self.consumer_name))
        work_item = ray.get(ray.ObjectID(work_token))

        # TODO(simon): handle variadic arguments
        result = wrap_to_ray_error(self.__call__, work_item.request_body)
        result_oid = work_item.result_oid
        ray.worker.global_worker.put_object(result_oid, result)

        # tail recursively schedule itself
        self.self_handle.main_loop.remote()

@ray.remote
class TaskRunnerActor(TaskRunner, RayServeMixin):
    pass
