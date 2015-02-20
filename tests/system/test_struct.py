# -*- coding: utf-8 -*-
"""Unit tests for the Struct proxy class."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.system.struct import Struct


def test_constructor():
    config = Struct({
        "uri": "/data/test/",
        "job_config": {
            "datasource": "test-source"
        }
    })

    assert config.uri == '/data/test/'
    assert config['uri'] == '/data/test/'

    assert config.job_config.datasource == 'test-source'
    assert config.job_config['datasource'] == 'test-source'
