# -*- coding: utf-8 -*-
"""Provides the Struct class."""


class Struct(dict):

    """Wraps a dictionary and makes its values accessible as attributes."""

    def __getattribute__(self, name):
        value = self[name]

        if isinstance(value, dict):
            return Struct(value)
        else:
            return value
