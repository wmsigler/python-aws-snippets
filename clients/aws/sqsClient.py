import json
import traceback
import uuid

import boto3


# TODO: refactor
class SqsClient:

    def __init__(self, logger, config):
        self.config = config
        self.logger = logger
        self.visibility_timeout = int(config.get('AWS', 'sqs_visibility_timeout'))
        self.sqs_client = boto3.client(
            'sqs',
            region_name=config.get('AWS', 'region_name')
        )

    def poll_messages(self, queue_url):
        messages = []
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=queue_url,
                AttributeNames=['All'],
                MaxNumberOfMessages=int(self.config.get('AWS', 'sqs_max_messages')),
                WaitTimeSeconds=int(self.config.get('AWS', 'sqs_wait_time_seconds'))
            )

            if 'Messages' in response:
                self.logger.debug(f"Polled messages count = {len(response['Messages'])}")
                for m in response['Messages']:
                    # receipt_handle = m['ReceiptHandle']
                    # self.sqs_client.change_message_visibility(
                    #     QueueUrl=queue_url,
                    #     ReceiptHandle=receipt_handle,
                    #     VisibilityTimeout=self.visibility_timeout
                    # )
                    messages.append(m)

            return self.sqs_client, messages
        except Exception:
            self.logger.error(traceback.format_exc())
            pass

    def delete_message(self, queue_url, message):
        try:
            self.logger.debug(f"Deleting message from queue '{queue_url}':\n{message}")
            self.sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
        except Exception:
            self.logger.error(traceback.format_exc())
            pass

    def publish_message_fifo_queue(self, queue_url, msg, msg_attr, group_id, dedup_id=str(uuid.uuid4())):
        """
        Helper method to publish messages to FIFO SQS queue
        :param queue_url:
        :param msg:
        :param msg_attr:
        :param group_id:
        :param dedup_id:
        :return:
        """
        try:
            self.logger.debug(f"Sending logs via SQS message: {str(msg)}")
            response = self.sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(msg),
                MessageAttributes=msg_attr,
                MessageGroupId=group_id,
                MessageDeduplicationId=dedup_id
            )
            return response
        except Exception:
            self.logger.error(traceback.format_exc())
            pass
