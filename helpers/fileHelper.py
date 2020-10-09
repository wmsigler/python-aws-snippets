import traceback

from pathlib import Path


class FileHelper:
    @staticmethod
    def write_to_file(data, output_path, filename, logger):

        try:
            resolved_path = Path(output_path).resolve()
            resolved_path.mkdir(parents=True, exist_ok=True)

            resolved_filename = resolved_path.joinpath(filename)
            with open(resolved_filename, 'w') as f:
                f.write(str(data))
        except Exception:
            logger.error(traceback.format_exc())

    @staticmethod
    def get_files_under_path(file_path, file_name_pattern):
        return list(Path(file_path).glob(file_name_pattern))
