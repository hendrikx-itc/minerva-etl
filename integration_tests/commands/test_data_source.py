import subprocess


def test_create_data_source(start_db_container):
    proc = subprocess.run(['minerva', 'data-source', 'create', 'test'])

    assert proc.returncode == 0

    proc = subprocess.run(['minerva', 'data-source', 'delete', 'test'])

    assert proc.returncode == 0
