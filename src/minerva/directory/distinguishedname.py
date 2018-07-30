# -*- coding: utf-8 -*-
"""
Provice basic functions for manipulating distinguished names.
"""
import re

explode_regex = re.compile("([^,]+)=([^,]+)")


class InvalidDistinguishedNameError(Exception):
    pass


def explode(distinguished_name):
    return explode_regex.findall(distinguished_name)


def implode(parts):
    return ",".join(
        "{0}={1}".format(type_name, name)
        for type_name, name in parts
    )


def split_parts(distinguished_name):
    """
    Split the parts of a distinguished name into a list.
    """
    regex = re.compile(r"(?<!\\),")

    return regex.split(distinguished_name)


def escape(part):
    """
    Escape reserved characters in the name part.
    """
    part = part.replace(",", "\\,")

    return part


def entity_type_name_from_dn(dn):
    """
    Return type of last component of distinguished name
    """
    return explode(dn)[-1][0]


class DistinguishedName:
    def __init__(self, parts):
        self.parts = parts

    @staticmethod
    def from_str(dn_str):
        """Return new DistinguishedName instance constructed from `dn_str`"""
        return DistinguishedName(explode(dn_str))

    def to_str(self):
        return implode(self.parts)

    def entity_type_name(self):
        return self.parts[-1][0]
