import json
import logging
import ray


def _get_logger():
    logger = logging.getLogger("ray.serve")
    logger.setLevel(logging.DEBUG)
    return logger


logger = _get_logger()


class BytesEncoder(json.JSONEncoder):
    """
    Allow bytes to be part of the JSON document
    """

    def default(self, o):  # pylint: disable=E0202
        if isinstance(o, bytes):
            return o.decode()
        return super().default(o)


def get_custom_object_id():
    worker = ray.worker.global_worker
    object_id = ray._raylet.compute_put_id(
        worker.current_task_id, worker.task_context.put_index
    )
    worker.task_context.put_index += 1
    return object_id
