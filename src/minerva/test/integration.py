import time
import os

from minerva.test import connect


def start_db_container(docker_client):
    container = docker_client.containers.run(
        "minerva_db",
        detach=True,
        environment={"POSTGRES_PASSWORD": "password"},
        ports={5432: 5432}
    )

    os.environ['PGHOST'] = 'localhost'
    os.environ['PGPASSWORD'] = 'password'
    os.environ['PGUSER'] = 'postgres'
    os.environ['PGDATABASE'] = 'minerva'

    connected = False

    while not connected:
        try:
            connect()
            connected = True
            print('connected to db')
        except Exception:
            print('error connecting to db')
            connected = False
            time.sleep(1)

    return container
