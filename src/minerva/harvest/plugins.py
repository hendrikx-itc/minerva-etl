# -*- coding: utf-8 -*-
"""
Provides plugin loading functionality.
"""
import pkg_resources

ENTRY_POINT = "minerva.harvest.plugins"


def iter_entry_points():
    return pkg_resources.iter_entry_points(group=ENTRY_POINT)


def load_plugins():
    """
    Load and return a dictionary with plugins by their names.
    """
    return {
        entry_point.name: entry_point.load()()
        for entry_point in iter_entry_points()
    }


def get_plugin(name):
    try:
        return next(
            entry_point.load()()
            for entry_point in iter_entry_points()
            if entry_point.name == name
        )
    except StopIteration:
        return None
