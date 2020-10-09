import json

from collections import OrderedDict
from utils.dateParser import current_milli_time


class Request:

    header_objects = None
    payload = None

    def __init__(self, **kwargs):
        self.header_objects = [
            'request_id',
            'request_dt',
            'request_ts',
            'request_src',
            'tags',
            'payload',
        ]
        self.build(**kwargs)

    def build(self, **kwargs):
        self.dictionary = OrderedDict()
        for obj in self.header_objects:
            self.dictionary[obj] = kwargs.get(obj)

        for k in kwargs:
            try:
                if k in self.header_objects:
                    self.dictionary[k] = kwargs.get(k)
                elif not k.startswith("payload_"):
                    raise KeyError
            except KeyError:
                print(
                    f"'{k}' is not a valid request object, must be one of the following: "
                    f"[{', '.join(self.header_objects)}] or object beginning with 'payload_'"
                )
                raise

        self.dictionary['payload'] = self.build_payload(**kwargs)

        if self.dictionary['request_ts'] is None:
            self.dictionary['request_ts'] = current_milli_time()

        return self

    def build_payload(self, **kwargs) -> type(dict):
        header_keys = self.header_objects
        payload_keys = [] if self.payload is None else list('payload_' + i for i in self.payload.keys())
        for k, v in kwargs.items():
            try:
                if k not in payload_keys and k not in header_keys:
                    raise KeyError
            except KeyError:
                print(
                    f"'{k}' is not a valid object, must be one of the following: "
                    f"[{', '.join(header_keys + payload_keys)}]"
                )
                raise

        return self.payload

    def as_json_object(self):
        return json.loads(self.__str__())

    def __str__(self):
        return json.dumps(self.dictionary)

