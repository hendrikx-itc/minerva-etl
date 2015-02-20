# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class Proxy():
    def __init__(self, subject=None):
        self.__subject = subject

    def setsubject(self, subject):
        self.__subject = subject

    def __getattr__(self, name):
        return getattr(self.__subject, name)
