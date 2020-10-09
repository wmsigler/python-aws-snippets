import json
import traceback

from clients.aws.awsClient import AwsClient


class LambdaClient(AwsClient):

    CLIENT_TYPE = 'lambda'

    def __init__(selfself, logger, config=None):
        super().__init__(logger, config)

    def call_lambda_function(self, function_name, request):
        response_payload = None

        try:
            response = self.client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(request)
            )
            response_payload = json.load(response['Payload'])
        except Exception:
            self.logger.error(traceback.format_exc())

        return response_payload
