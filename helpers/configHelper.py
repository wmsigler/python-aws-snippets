from configparser import ConfigParser
from pathlib import Path


class ConfigHelper:
    @staticmethod
    def setup_config() -> type(ConfigParser):
        """
        Reads base and env-specific config files and returns object
        :return: config object
        """
        # read base config
        config = ConfigParser()
        base_config = Path('config', 'base_config.ini')
        config.read(base_config)

        # read execution env config
        env = config.get('APPLICATION', 'env')
        env_config = Path('config', f'{env}.ini')
        config.read(env_config)

        return config
