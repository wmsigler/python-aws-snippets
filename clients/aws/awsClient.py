from abc import ABC, abstractmethod

import boto3

from helpers.configHelper import ConfigHelper


class AwsClient(ABC):
    client = None
    config = None

    @property
    @abstractmethod
    def CLIENT_TYPE(self):
        ...

    def __init__(self, logger, config=None):
        self.config = self.__setup_config() if config is None else config
        self.logger = logger
        self.client = self.__setup_client(config)
        super().__init__()

    def __setup_config(self):
        return ConfigHelper.setup_config()

    def __setup_client(self, config):
        return boto3.client(
            self.CLIENT_TYPE,
            region_name=config.get('AWS', 'region_name')
        )
