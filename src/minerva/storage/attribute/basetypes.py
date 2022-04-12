# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"


class AttributeTag(object):
    def __init__(self, id, name):
        self.id = id
        self.name = name

    def __repr__(self):
        return "<AttributeTag({0})>".format(self.name)

    def __str__(self):
        return self.name
