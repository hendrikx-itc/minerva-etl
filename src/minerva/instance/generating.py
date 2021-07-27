"""
Provides common definition generating logic
"""
import re


def translate_entity_aggregation_part_name(name: str, target_entity_type: str, prefix='') -> str:
    """
    Translate a part name with standard naming convention
    <data_source>_<entity_type>_<granularity> to
    <data_source_<target_entity_type>_<granularity>.
    """
    m = re.match("^([^_]+)_([^_]+)_(.*)$", name)

    if m is None:
        raise ValueError(f"Could not translate part name {name}")

    data_source = m.group(1)
    _entity_type = m.group(2)
    tail = m.group(3)

    return f"{data_source}_{target_entity_type}_{prefix}{tail}"
