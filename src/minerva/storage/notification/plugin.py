# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"
from minerva.storage.notification.types import NotificationStore, \
    Record, Attribute


class NotificationPlugin(object):
    Record = staticmethod(Record)
    Attribute = staticmethod(Attribute)
    NotificationStore = staticmethod(NotificationStore)

    def get_notificationstore(self, datasource, entitytype):
        def f(cursor):
            return NotificationStore.load(cursor, datasource, entitytype)

        return f
