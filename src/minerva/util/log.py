# -*- coding: utf-8 -*-
import os
import sys
import subprocess
import logging
from logging.handlers import RotatingFileHandler

from minerva.util.config import parse_size


def parse_loglevel(level_str):
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL}

    level_str = level_str.strip().upper()
    if level_str not in level_map.keys():
        return logging.INFO
    else:
        return level_map[level_str]


def setup_logging(
        verbose=True, level="INFO", directory=None, filename=None,
        rotation_size="10MB"):
    """
    Setup logging.
    """

    root_logger = logging.getLogger("")

    for handler in root_logger.handlers:
        root_logger.removeHandler(handler)

    if verbose:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(parse_loglevel(level))
        root_logger.addHandler(handler)

    if not(directory is None and filename is None):
        max_log_size = parse_size(rotation_size)

        file_path = os.path.join(directory, filename)

        handler = RotatingFileHandler(
            file_path, maxBytes=max_log_size, backupCount=5
        )

        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        )

        root_logger.setLevel(parse_loglevel(level))
        root_logger.addHandler(handler)

    return logging


def subprocess_with_logging(command):
    process = subprocess.Popen(
        command.split(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )

    stdoutdata, stderrdata = process.communicate()
    if stdoutdata is not None:
        logging.info("%s", stdoutdata)
    if stderrdata is not None:
        logging.error("%s", stderrdata)
