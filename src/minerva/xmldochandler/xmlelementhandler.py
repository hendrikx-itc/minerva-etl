# -*- coding: utf-8 -*-

from minerva.util.proxy import Proxy


class XmlElementHandlerRef(Proxy):

    def __init__(self, ref):
        # The subject is filled in later, based on the reference
        Proxy.__init__(self, None)
        self.ref = ref
        self.name = ref.localname
