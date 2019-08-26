import time
from pprint import pprint

import requests

import serve as srv


@srv.route("/infer_image", methods=["POST"])
def infer(context):
    img = get_image(context['body'])
    return img

infer.remote({'body': image_bytes})


@srv.route("/infer_image", methods=["POST"])
def infer(image_bytes: http.Body):
    img = get_image(image_bytes)
    return img

infer.remote(image_bytes)


@srv.route("/echo/{my_name}")
def echo(my_name, my_image:http.Image.toPIL):
    context['body']
    version = context["http_version"]
    return context

echo.remote(context={'http_version': '1.1'}, my_name='Simon')
echo.remote(my_name='Simon')

srv.init(blocking=True)

srv.create_endpoint("my_endpoint", "/echo")
srv.create_backend(echo, "echo:v1")
srv.link("my_endpoint", "echo:v1")

while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    pprint(resp)

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
