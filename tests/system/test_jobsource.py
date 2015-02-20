# -*- coding: utf-8 -*-
"""Unit tests for the JobSource class."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.system.jobsource import JobSource


def test_constructor():
    config = """{
    "uri": "/data/test/",
    "job_config": {
        "datasource": "test-source"
    }
}"""

    jobsource = JobSource(42, 'some-test-source', 'dummy', config)

    assert jobsource.id == 42
    assert jobsource.name == 'some-test-source'
    assert jobsource.job_type == 'dummy'

    assert jobsource.config.uri == '/data/test/'
    assert jobsource.config['uri'] == '/data/test/'

    assert jobsource.config.job_config.datasource == 'test-source'
    assert jobsource.config.job_config['datasource'] == 'test-source'
