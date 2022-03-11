import subprocess
import tempfile
import json

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

    with tempfile.NamedTemporaryFile("wt") as csv_file, tempfile.NamedTemporaryFile("wt") as config_file:
        parser_config = {
            "timestamp": "timestamp",
            "identifier": "entity",
            "delimiter": ",",
            "chunk_size": 5000,
            "columns": [],
            "entity_type": "node",
            "granularity": "1d",
        }

        json.dump(parser_config, config_file)
        config_file.flush()

        csv_file.write("timestamp,entity,a,b,c\n")
        csv_file.write("2022-03-11T00:00:00,node_1,1,2,3\n")
        csv_file.flush()

        proc = subprocess.run(
            ['minerva', 'load-data', '--data-source', 'test', '--parser-config', config_file.name, '--type', 'csv', csv_file.name]
        )

    assert proc.returncode == 0

    proc = subprocess.run(['minerva', 'data-source', 'delete', 'test'])

    assert proc.returncode == 0
