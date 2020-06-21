import time
import os

import pytest
import docker

from minerva.test import connect


@pytest.fixture(scope="session")
def start_db_container(request):
    print('\n----------- session start ---------------')
    docker_client = docker.from_env()

    port = 5432

    container = docker_client.containers.run(
        "minerva_db",
        remove=True,
        detach=True,
        environment={"POSTGRES_PASSWORD": "password"},
        ports={port: 5432}
    )

    def stop_container():
        print("stopping container")
        container.stop()
        print('----------- session done ---------------')

    request.addfinalizer(stop_container)

    os.environ['PGHOST'] = 'localhost'
    os.environ['PGPASSWORD'] = 'password'
    os.environ['PGUSER'] = 'postgres'
    os.environ['PGDATABASE'] = 'minerva'
    os.environ['PGPORT'] = str(port)

    connected = False

    while not connected:
        try:
            conn = connect()
            connected = True
            print('connected to db')
        except Exception as exc:
            #print('error connecting to db: {}'.format(exc))
            connected = False
            time.sleep(1)

    return conn
