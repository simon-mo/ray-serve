from bowler import Query

query = (
    Query('.')
    .select_module("serve")
    .rename("ray.experimental.serve")
    .diff(interactive=True)
)
