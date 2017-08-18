# -*- coding: utf-8 -*-
"""
Unit tests for the system helper functions.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import mock

from minerva.system.jobqueue import enqueue_job, finish_job


def test_add_job():
    """
    Test if jobs are successfully created in the database.
    """
    datasource_id = 33
    dispatcher_id = 17

    mock_conn = mock.Mock()
    mock_cursor = mock.Mock()
    mock_cursor.fetchone.return_value = (42, )
    mock_conn.cursor.return_value = mock_cursor

    enqueue_job(
        mock_conn, "/share/file.xml", 23675, datasource_id, dispatcher_id)


def test_finish_job():
    """
    Test if jobs are successfully updated in the database.
    """
    job_id = 42

    mock_conn = mock.Mock()
    mock_cursor = mock.Mock()
    mock_conn.cursor.return_value = mock_cursor

    finish_job(mock_conn, job_id)
