import time
import os

import pytest
import docker

from minerva.test import connect


@pytest.fixture(scope="session")
def start_db_container(request):
    print('\n----------- session start ---------------')
    docker_network = os.environ.get("TEST_DOCKER_NETWORK")

    docker_client = docker.from_env()

    container = docker_client.containers.run(
        "hendrikxitc/minerva",
        remove=True,
        detach=True,
        environment={"POSTGRES_PASSWORD": "password"},
        publish_all_ports=(docker_network is None),
        network=docker_network,
    )

    container.reload()

    def stop_container():
        print("stopping container")
        container.stop()
        print('----------- session done ---------------')

    request.addfinalizer(stop_container)

    if docker_network is None:
        os.environ['PGHOST'] = 'localhost'

        mapped_ports = container.ports['5432/tcp']

        # We assume there will be one mapping for the standard PostgreSQL port, so
        # we just take the first.
        first_mapped_port = mapped_ports[0]

        # Get the port on the host, so we know where to connect to
        host_port = first_mapped_port['HostPort']
    else:
        os.environ['PGHOST'] = container.name

        host_port = '5432'

    os.environ['PGPASSWORD'] = 'password'
    os.environ['PGUSER'] = 'postgres'
    os.environ['PGDATABASE'] = 'minerva'
    os.environ['PGPORT'] = host_port

    connected = False

    while not connected:
        try:
            conn = connect()
            connected = True
            print('connected to db')
        except Exception as exc:
            connected = False
            time.sleep(1)

    return conn
