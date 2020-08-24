# -*- coding: utf-8 -*-
"""
Provides plugin loading functionality.
"""
import pkg_resources

from minerva.loading import csv

ENTRY_POINT = "minerva.harvest.plugins"


builtin_types = {
    'csv': csv.Plugin()
}


def iter_entry_points():
    return pkg_resources.iter_entry_points(group=ENTRY_POINT)


def list_plugins():
    return list(builtin_types.keys()) + [
        entry_point.name for entry_point in iter_entry_points()
    ]


def load_plugins():
    """
    Load and return a dictionary with plugins by their names.
    """
    return {
        entry_point.name: entry_point.load()()
        for entry_point in iter_entry_points()
    }


def get_plugin(name):
    if name in builtin_types:
        return builtin_types[name]

    try:
        return next(
            entry_point.load()()
            for entry_point in iter_entry_points()
            if entry_point.name == name
        )
    except StopIteration:
        return None
