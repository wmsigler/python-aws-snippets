import boto3


# TODO: refactor
class KinesisClient:

    def __init__(self, logger, config):
        self.logger = logger
        self.kinesis_client = boto3.client('kinesis', region_name=config.get('AWS', 'region_name'))

    def publish_event(self, stream_name, key, body):
        try:
            self.logger.debug(f"Sending message to Kinesis stream '{stream_name}' with key '{key}' and message body:\n{body}")
            put_response = self.kinesis_client.put_record(
                StreamName=stream_name,
                Data=body,
                PartitionKey=key
            )
            return put_response
        except Exception:
            self.logger.debug(f"Error while sending message to Kinesis stream '{stream_name}' with key '{key}' and message body:\n{body}")
