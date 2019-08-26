# Ray Serve

`Ray Serve` is a library that helps you deploy your functions and actors to the outside world.

It offers:
- Seamless version upgrade and rollback.
- Flexible traffic policy.
- First class deadline
- Call from http and python

```python
import serve as srv
import requests

def echo(context):
    return context

srv.init(blocking=True)

srv.create_endpoint("my_endpoint", "/echo")
srv.create_backend(echo, "echo:v1")
srv.link("my_endpoint", "echo:v1")

while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(resp)
```

```
# starting server process
INFO: Started server process [87477]
INFO: Waiting for application startup.
INFO: Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)

# routing table is populated in the background
Updated Routing Table:  {}
INFO: ('127.0.0.1', 61851) - "GET /echo HTTP/1.1" 200
{'error': 'path not found'}
...

# second try works!
Updated Routing Table:  {'/echo': 'my_endpoint'}
INFO: ('127.0.0.1', 61853) - "GET /echo HTTP/1.1" 200

{'result': {'type': 'http', 'http_version': '1.1', 'server': ['127.0.0.1', 8000], 'client': ['127.0.0.1', 61853], 'scheme': 'http', 'method': 'GET', 'root_path': '', 'path': '/echo', 'raw_path': '/echo', 'query_string': '', 'headers': [['host', '127.0.0.1:8000'], ['user-agent', 'python-requests/2.12.4'], ['accept-encoding', 'gzip, deflate'], ['accept', '*/*'], ['connection', 'keep-alive']]}}
...
```

