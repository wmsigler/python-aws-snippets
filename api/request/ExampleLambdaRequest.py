from collections import OrderedDict

from api.request.request import Request


class ExampleLambdaRequest(Request):

    def __init__(self, **kwargs):
        self.payload = kwargs.get('payload', OrderedDict([
            ('name', kwargs.get('payload_name')),
            ('quest', kwargs.get('payload_quest')),
            ('favorite-color', kwargs.get('payload_favorite-color')),
        ]))
        super().__init__(**kwargs)
