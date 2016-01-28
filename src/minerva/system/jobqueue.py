# -*- coding: utf-8 -*-
"""
Provides helper functions for interacting with the job queue in the database.
"""


class NoJobAvailable(Exception):
    pass


def get_job(cursor):
    cursor.callproc("system.get_job")

    result = cursor.fetchone()

    if result[0] is None:
        return None
    else:
        return result
