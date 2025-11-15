# logging_config.py
import logging
from config.config import LOG_FILE


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s — %(levelname)s — %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )
