"""
Provides access and a location for storage class logic like 'trend',
'attribute', etc..
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


def load_plugins():
    from minerva.storage import trend, attribute, geospatial, notification, delta
    """
    Load and return a dictionary with plugins by their names.
    """
    return {
        'attribute': attribute.create,
        'trend': trend.create,
        'notification': notification.NotificationPlugin,
        'geospatial': geospatial.create
    }


def get_plugin(name):
    """
    Return storage plugin with name `name`.
    """
    return load_plugins().get(name)
