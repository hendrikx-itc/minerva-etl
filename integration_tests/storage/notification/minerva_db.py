# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"
from contextlib import closing


def clear_database(conn):
    with closing(conn.cursor()) as cursor:
        cursor.execute("DELETE FROM notification.notificationstore CASCADE")
