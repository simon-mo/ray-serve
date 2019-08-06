import time

import requests

import serve as srv


def echo(context):
    return context


srv.init(blocking=True)

srv.create_endpoint("my_endpoint", "/echo")
srv.create_backend(echo, "echo:v1")
srv.link("my_endpoint", "echo:v1")

while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(resp)

    print("...")
    time.sleep(2)
