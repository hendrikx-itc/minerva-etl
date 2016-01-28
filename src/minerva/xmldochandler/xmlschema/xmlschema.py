# -*- coding: utf-8 -*-

import os.path
import sys

parentPath = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(parentPath)

from xmlnamespace import XmlNamespace

from xmlschema_string import XmlSchema_string


class XmlSchema(XmlNamespace):

    def __init__(self):
        XmlNamespace.__init__(self, u'http://www.w3.org/2001/XMLSchema')

        self.addnamedtype(XmlSchema_string())
