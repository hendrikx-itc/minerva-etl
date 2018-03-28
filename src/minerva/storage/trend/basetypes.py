# -*- coding: utf-8 -*-


class TrendTag(object):
    def __init__(self, id, name):
        self.id = id
        self.name = name

    def __repr__(self):
        return "<TrendTag({0})>".format(self.name)

    def __str__(self):
        return self.name
