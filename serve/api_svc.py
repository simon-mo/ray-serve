import ray
from abc import ABC

import ray.experimental.internal_kv as ray_kv
import json
from serve.utils import logger


class NamespacedKVStore(ABC):
    def __init__(self, namespace):
        raise NotImplementedError()

    def get(self, key):
        raise NotImplementedError()

    def put(self, key, value):
        raise NotImplementedError()

    def as_dict(self):
        raise NotImplementedError()


class InMemoryKVStore(NamespacedKVStore):
    def __init__(self, namespace):
        self.data = dict()

        # namepsace is ignored because each namespace is backed by
        # one in memory dictionary
        self.namespace = namespace

    def get(self, key):
        return self.data[key]

    def put(self, key, value):
        self.data[key] = value

    def as_dict(self):
        return self.data.copy()


class RayInternalKVStore(NamespacedKVStore):
    def __init__(self, namespace):
        assert ray_kv._internal_kv_initialized()
        self.index_key = "RAY_SERVE_INDEX"
        self.namespace = namespace
        self._put(self.index_key, [])


    def _format_key(self, key):
        return "{ns}-{key}".format(ns=self.namespace, key=key)

    def _remove_format_key(self, formatted_key):
        return formatted_key.replace(self.namespace + "-", "")

    def _serialize(self, obj):
        return json.dumps(obj)

    def _deserialize(self, buffer):
        return json.loads(buffer)

    def _put(self, key, value):
        ray_kv._internal_kv_put(
            self._format_key(self._serialize(key)),
            self._serialize(value),
            overwrite=True,
        )

    def _get(self, key):
        return self._deserialize(
            ray_kv._internal_kv_get(self._format_key(self._serialize(key)))
        )

    def get(self, key):
        return self._get(key)

    def put(self, key, value):
        assert isinstance(key, str), "Key must be string"

        self._put(key, value)

        all_keys = set(self._get(self.index_key))
        all_keys.add(key)
        self._put(self.index_key, list(all_keys))

    def as_dict(self):
        data = {}
        all_keys = self._get(self.index_key)
        for key in all_keys:
            data[self._remove_format_key(key)] = self._get(key)
        return data


class RouteServer:
    _KV_CLASS = InMemoryKVStore

    def __init__(self):
        self.routing_table = self._KV_CLASS(namespace="routes")
        self.request_count = 0

    def register_service(self, route: str, svc: str):
        logger.info("[KV] Registering route %s to svc %s", route, svc)
        self.routing_table.put(route, svc)

    def list_service(self):
        self.request_count += 1
        table = self.routing_table.as_dict()
        # logger.info("[KV] list_service %s", table)
        return table

    def get_request_count(self):
        return self.request_count


@ray.remote
class RouteServerActor(RouteServer):
    _KV_CLASS = RayInternalKVStore
