import subprocess
from pathlib import Path

from api.request.ExampleLambdaRequest import ExampleLambdaRequest
from clients.aws.lambdaClient import LambdaClient
from helpers.configHelper import ConfigHelper


def main():
    config = ConfigHelper.setup_config()


def example_call_lambda(logger, config):

    lambda_client = LambdaClient(logger, config)
    lambda_name = 'my-lambda-name'

    header_kwargs = {
        'request_id': 1234,
        'request_dt': '2020-01-01',
        'request_src': 'example-source',
        'tags': []
    }

    payload = {
        'name': 'Sir Lancelot of Camelot',
        'quest': 'To seek the Holy Grail',
        'favorite-color': 'blue'
    }

    lambda_request = ExampleLambdaRequest(
        **{
            **header_kwargs,
            'payload': payload
        }
    )

    try:
        lambda_response = lambda_client.call_lambda_function(
            lambda_name,
            lambda_request.as_json_object()
        )
        return lambda_response
    except Exception:
        logger.error(f"Error while executing lambda function '{lambda_name}' with request:\n{str(lambda_request)}")

def example_execute_jar(jar_path, request_file_path, request_data, logger):
    """
    Call JAR file with request file input
    :param request_file_path:
    :param request_data:
    :param logger:
    :return:
    """
    jar_file = str(Path(jar_path).resolve())
    args = request_file_path

    success = False
    out, err = None, None
    try:
        command = ' '.join(['java', '-jar', jar_file, args])
        p = subprocess.Popen(command, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
        out, err = p.communicate()
        logger.info(f"Output from JAR: {str(out)}")
        success = True
    except Exception:
        logger.error(f"Error occurred while executing JAR:\nRequest: {request_data}\nOutput: {out}\nError: {err}")

    return success


if __name__ == '__main__':
    main()