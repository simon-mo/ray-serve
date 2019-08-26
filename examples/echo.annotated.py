import time
from pprint import pprint

import requests

import serve as srv
#TODO: don't use abbreviation 


def echo(context):
    return context


# Emphasize continously running system
srv.init(blocking=True)

#TODO: get rid of "my_endpoint" string, seems obselete
srv.create_endpoint("my_endpoint", "/echo", blocking=True)
srv.create_backend(echo, "echo:v1")
srv.link("my_endpoint", "echo:v1")

while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    pprint(resp)

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
