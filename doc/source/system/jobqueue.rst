:mod:`minerva.system.jobqueue` --- jobqueue Module
==================================================

Module Contents
---------------

.. automodule:: minerva.system.jobqueue
   :members:
   :undoc-members:

.. function:: enqueue_job(conn, type, description, size, job_source_id)

   Add a new job to the job queue.

.. function:: finish_job(conn, job_id)

   Mark job with Id `job_id` as successfully finished.

.. function:: fail_job(conn, job_id)

   Mark job with Id `job_id` as failed.
