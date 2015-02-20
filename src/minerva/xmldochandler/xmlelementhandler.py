# -*- coding: utf-8 -*-

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.util.proxy import Proxy


class XmlElementHandlerRef(Proxy):

    def __init__(self, ref):
        # The subject is filled in later, based on the reference
        Proxy.__init__(self, None)
        self.ref = ref
        self.name = ref.localname
