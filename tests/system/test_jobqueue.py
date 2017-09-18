# -*- coding: utf-8 -*-
"""
Unit tests for the system helper functions.
"""
import unittest
from unittest.mock import MagicMock

from minerva.system.jobqueue import enqueue_job, finish_job


class TestJobQueue(unittest.TestCase):
    def test_add_job(self):
        """
        Test if jobs are successfully created in the database.
        """
        data_source_id = 33
        dispatcher_id = 17

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (42, )
        mock_conn.cursor.return_value = mock_cursor

        enqueue_job(
            mock_conn, "/share/file.xml", 23675, data_source_id, dispatcher_id
        )

    def test_finish_job(self):
        """
        Test if jobs are successfully updated in the database.
        """
        job_id = 42

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        finish_job(mock_conn, job_id)
