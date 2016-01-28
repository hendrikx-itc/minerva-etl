# -*- coding: utf-8 -*-
"""Provides the Job class."""
import json


class JobDescriptor:
    def __init__(self, job_type, description, size, job_source_id):
        self.job_type = job_type
        self.description = description
        self.size = size
        self.job_source_id = job_source_id


class Job:
    """
    Represents a Minerva job for processing by a node.
    """
    def __init__(
            self, id_, type_, description, size, created, started,
            finished, job_source_id, state):
        self.id = id_
        self.type = type_
        self.description = description
        self.size = size
        self.created = created
        self.started = started
        self.finished = finished
        self.job_source_id = job_source_id
        self.state = state

    @staticmethod
    def create(descriptor):
        def f(cursor):
            cursor.callproc(
                "system.create_job",
                (
                    descriptor.job_type,
                    json.dumps(descriptor.description),
                    descriptor.size,
                    descriptor.job_source_id
                )
            )

            Job.from_record(cursor.fetchone())

        return f

    @staticmethod
    def from_record(record):
        (
            id_, job_type, description, size, created, started, finished,
            job_source_id, state
        ) = record

        return Job(
            id_, job_type, description, size, created, started, finished,
            job_source_id, state
        )

    def finish(self, cursor):
        finish_job(self.id)(cursor)

    def fail(self, message):
        return fail_job(self.id, message)


def finish_job(job_id):
    def f(cursor):
        cursor.callproc("system.finish_job", (job_id,))

    return f


def fail_job(job_id, message):
    def f(cursor):
        cursor.callproc("system.fail_job", (job_id, message,))

    return f
