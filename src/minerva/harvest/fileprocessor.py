#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Provides the function process_file for processing a single file.
"""
import os
import threading
from pathlib import Path

import time
import codecs

from operator import not_

from minerva.harvest.plugin_api_trend import HarvestParserTrend
from minerva.util import compose

from minerva.harvest.error import DataError


class ParseError(Exception):
    pass


def process_file(
        file_path: Path, parser: HarvestParserTrend, show_progress=False):
    """
    Process a single file with specified plugin.
    """
    if not file_path.exists():
        raise Exception("Could not find file '{0}'".format(file_path))

    with file_path.open(encoding='utf-8') as data_file:
        stop_event = threading.Event()
        condition = compose(not_, stop_event.is_set)

        if show_progress:
            start_progress_reporter(data_file, condition)

        try:
            for package in parser.load_packages(data_file, file_path.name):
                yield package
        except DataError as exc:
            raise exc
        finally:
            stop_event.set()


def start_progress_reporter(data_file, condition):
    """
    Start a daemon thread that reports about the progress (position in
    data_file).
    """
    data_file.seek(0, 2)
    size = data_file.tell()
    data_file.seek(0, 0)

    def progress_reporter():
        """
        Show progress in the file on the console using a progress bar.
        """
        while condition():
            position = data_file.tell()

            percentage = position / size * 100

            print('{}'.format(percentage))

            time.sleep(1.0)

    thread = threading.Thread(target=progress_reporter)
    thread.daemon = True
    thread.start()
    return thread
