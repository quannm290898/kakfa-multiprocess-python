import os
from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import config as cf

def get_log_file_handler(log_folder=cf.log_folder):
    os.makedirs(log_folder, exist_ok=True)
    log_file = os.path.join(log_folder, "kafka_process.log")
    log_file_handler = TimedRotatingFileHandler(
        log_file, when="midnight", interval=1, backupCount=7
    )
    log_file_handler.suffix = "%Y-%m-%d"
    log_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    return log_file_handler