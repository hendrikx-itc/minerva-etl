# -*- coding: utf-8 -*-
"""Unit tests for the Struct proxy class."""
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
