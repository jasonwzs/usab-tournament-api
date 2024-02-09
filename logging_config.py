# logging_config.py
import logging

def setup_logging(log_file_path):
    logger = logging.getLogger()
    logger.setLevel(logging.WARNING)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.WARNING)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
