"""Test load-data sub-command."""
import subprocess  # nosec
import tempfile
import json

from minerva.util.yaml import ordered_yaml_dump

trend_store_dict = {
    "data_source": "test",
    "entity_type": "Cell",
    "granularity": "1 day",
    "partition_size": "86400s",
    "parts": [
        {"name": "first_part", "trends": []},
        {"name": "second_part", "trends": []},
    ],
}


def test_load_csv_file(start_db_container):
    """Test loading of a CSV file."""
    conn = start_db_container
    proc = subprocess.run(  # nosec
        ["minerva", "data-source", "create", "test"], check=True
    )

    assert proc.returncode == 0

    with tempfile.NamedTemporaryFile("wt", suffix=".yaml") as yaml_tmp_file:
        ordered_yaml_dump(trend_store_dict, yaml_tmp_file)
        yaml_tmp_file.flush()

        proc = subprocess.run(  # nosec
            ["minerva", "trend-store", "create", yaml_tmp_file.name], check=True
        )

    assert proc.returncode == 0

    with tempfile.NamedTemporaryFile("wt") as csv_file, tempfile.NamedTemporaryFile(
        "wt"
    ) as config_file:
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

        proc = subprocess.run(  # nosec
            [
                "minerva",
                "load-data",
                "--data-source",
                "test",
                "--parser-config",
                config_file.name,
                "--type",
                "csv",
                csv_file.name,
            ],
            check=True,
        )

    assert proc.returncode == 0

    with conn.cursor() as cursor:
        cursor.execute('SELECT c FROM trend."test_node_1d"')

        (c_value,) = cursor.fetchone()

        assert c_value == 3

    proc = subprocess.run(  # nosec
        ["minerva", "data-source", "delete", "test"], check=True
    )

    assert proc.returncode == 0
