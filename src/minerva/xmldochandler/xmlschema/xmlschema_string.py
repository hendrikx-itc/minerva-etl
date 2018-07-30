# -*- coding: utf-8 -*-
import os.path
import sys
from minerva.xmldochandler.xmlelementtype import XmlElementType

parentPath = os.path.join(os.path.dirname(__file__), '..')
sys.path.append(parentPath)


class XmlSchema_string(XmlElementType):

    class XmlElementHandler(XmlElementType.XmlElementHandler):

        def __init__(self, name, elementtype):
            XmlElementType.XmlElementHandler.__init__(self, name, elementtype)

            self.on_valueread = None

        @staticmethod
        def create_localcontext():
            return list()

        def end_element(self, local_context):
            if self.on_valueread:
                self.on_valueread("".join(local_context))

            if self.on_element_end:
                self.on_element_end()

        def characters(self, local_context, content):
            return local_context.append(content)

    def __init__(self):
        XmlElementType.__init__(self, u'string', None)

    def create_elementhandler(self, name):
        return XmlSchema_string.XmlElementHandler(name, self)
