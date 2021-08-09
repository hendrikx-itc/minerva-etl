import subprocess
import tempfile

from minerva.util.yaml import ordered_yaml_dump

trend_store_dict = {
    'data_source': 'test',
    'entity_type': 'Cell',
    'granularity': '1 day',
    'partition_size': '86400s',
    'parts': [
        {
            'name': 'first_part',
            'trends': [
            ]
        },
        {
            'name': 'second_part',
            'trends': [
            ]
        }
    ]
}


def test_create_data_source(start_db_container):
    proc = subprocess.run(['minerva', 'data-source', 'create', 'test'])

    assert proc.returncode == 0

    with tempfile.NamedTemporaryFile('wt', suffix=".yaml") as yaml_tmp_file:
        ordered_yaml_dump(trend_store_dict, yaml_tmp_file)
        yaml_tmp_file.flush()

        proc = subprocess.run(['minerva', 'trend-store', 'create', yaml_tmp_file.name])

    assert proc.returncode == 0

    with tempfile.NamedTemporaryFile("wt") as tmp_file:
        tmp_file.write("a,b,c\n")
        tmp_file.write("1,2,3\n")
        tmp_file.flush()

        proc = subprocess.run(
            ['minerva', 'load-data', '--data-source', 'test', '--type', 'csv', tmp_file.name]
        )

    assert proc.returncode == 0

    proc = subprocess.run(['minerva', 'data-source', 'delete', 'test'])

    assert proc.returncode == 0
