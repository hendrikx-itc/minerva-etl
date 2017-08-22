# -*- coding: utf-8 -*-
from minerva.storage.notification.types import NotificationStore, \
        Record, Attribute

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2017 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class NotificationPlugin(object):
    Record = staticmethod(Record)
    Attribute = staticmethod(Attribute)
    NotificationStore = staticmethod(NotificationStore)

    def get_notificationstore(self, datasource, entitytype):
        def f(cursor):
            return NotificationStore.load(cursor, datasource, entitytype)

        return f
