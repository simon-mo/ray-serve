import time

import requests
from flaky import flaky

import ray
import serve as srv


def delay_rerun(*args):
    time.sleep(1)
    return True


# flaky test because the routing table might not be populated
@flaky(rerun_filter=delay_rerun)
def test_e2e(serve_instance):
    srv.create_endpoint("endpoint", "/api")
    result = ray.get(srv.global_state.api_handle.list_service.remote())
    assert result == {"/api": "endpoint"}

    assert requests.get("http://127.0.0.1:8000/").json() == result
    echo = lambda i: i

    srv.create_backend(echo, "echo:v1")
    srv.link("endpoint", "echo:v1")

    resp = requests.get("http://127.0.0.1:8000/api").json()["result"]
    assert resp["path"] == "/api"
    assert resp["method"] == "GET"
