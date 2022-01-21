import psycopg2


def start_job(conn, description: dict) -> int:
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT logging.start_job(%s)",
            (psycopg2.extras.Json(description),)
        )

        job_id = cursor.fetchone()[0]

    return job_id


def end_job(conn, job_id: int):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT logging.end_job(%s)",
            (job_id,)
        )
