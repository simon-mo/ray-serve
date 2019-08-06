import pytest

import ray
import serve as srv


@pytest.fixture(scope="session")
def serve_instance():
    srv.init()
    srv.global_state.wait_until_http_ready()
    yield
    srv.global_state.shutdown()
