import json


class BytesEncoder(json.JSONEncoder):
    """
    Allow bytes to be part of the JSON document
    """

    def default(self, o):  # pylint: disable=E0202
        if isinstance(o, bytes):
            return o.decode()
        return super().default(o)
