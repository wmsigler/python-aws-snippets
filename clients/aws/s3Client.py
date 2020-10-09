import os
import traceback
from pathlib import Path

from clients.aws.awsClient import AwsClient


class S3Client(AwsClient):

    CLIENT_TYPE = 's3'

    def __init__(self, logger, config=None):
        super().__init__(logger, config)

    def download_file(self, file_name, bucket_name, prefix, destination_path):
        success = False

        try:
            self._create_directory_if_not_exists(destination_path)

            s3_full_file_path = '/'.join([prefix, file_name])
            destination_full_file_path = os.path.join(destination_path, file_name)

            with open(destination_full_file_path, 'wb') as f:
                self.client.download_fileobj(bucket_name, s3_full_file_path, f)

            self.logger.info(f"Downloaded file {'/'.join([str(bucket_name), str(s3_full_file_path)])} to {str(destination_path)}")
            success = True
        except Exception:
            self.logger.error(traceback.format_exc())

        return success

    def download_files_with_prefix(self, bucket_name, prefix, destination_path):
        success = False

        dest_pathname = None
        try:
            keys = {}
            next_token = ''
            base_kwargs = {
                'Bucket': bucket_name,
                'Prefix': prefix
            }
            self.logger.debug(f"Creating directory {str(os.path.dirname(destination_path))}/")
            self._create_directory_if_not_exists(os.path.dirname(destination_path + "/"))

            while next_token is not None:
                kwargs = base_kwargs.copy()

                if next_token != '':
                    kwargs.update({'ContinuationToken': next_token})

                results = self.client.list_objects_v2(**kwargs)
                contents = results.get('Contents')

                for i in contents:
                    key = i.get('Key')
                    if key[-1] != '/':
                        keys[key] = str(key.split('/')[-1])
                next_token = results.get('NextContinuationToken')

            for key, file_name in keys.items():
                dest_pathname = os.path.join(destination_path, file_name)
                self._create_directory_if_not_exists(os.path.dirname(destination_path))

                with open(dest_pathname, 'wb') as f:
                    self.client.download_fileobj(bucket_name, key, f)

            success = True
        except Exception:
            self.logger.error(traceback.format_exc())

        return success, dest_pathname

    def upload_directory_files(self, bucket_name, prefix, directory):
        success = False

        current_file_s3_name = None
        current_file_local_name = None
        try:
            for path, subdirs, files in os.walk(directory):
                self.logger.info(f"Uploading files in '{str(Path(path))}' to S3 bucket '{bucket_name}'...")
                path = path.replace('\\', '/')
                directory_name = path.replace(directory, '')

                for file in files:
                    current_file_s3_name = '/'.join([prefix, directory_name, file])
                    current_file_local_name = Path(directory, directory_name, file).resolve()
                    with open(current_file_local_name, 'rb') as f:
                        self.client.upload_fileobj(f, bucket_name, current_file_s3_name)

                self.logger.info(f"Successfully uploaded files in '{str(Path(path))}' to S3 bucket '{bucket_name}'.")

            success = True
        except Exception:
            self.logger.error(f"Exception occurred when uploading file '{current_file_local_name}' to S3 bucket '{bucket_name}' as '{current_file_s3_name}':\n{traceback.format_exc()}")

        return success

    def _create_directory_if_not_exists(self, path):
        Path(path).mkdir(parents=True, exist_ok=True)
