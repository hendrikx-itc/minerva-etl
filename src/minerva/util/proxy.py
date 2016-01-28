# -*- coding: utf-8 -*-


class Proxy:
    def __init__(self, subject=None):
        self.__subject = subject

    def setsubject(self, subject):
        self.__subject = subject

    def __getattr__(self, name):
        return getattr(self.__subject, name)
