# -*- coding: utf-8 -*-


class NoRecordError(Exception):
    """
    A rather generic exception to indicate that no record was found.
    """
    def __init__(self):
        super(NoRecordError, self).__init__()
