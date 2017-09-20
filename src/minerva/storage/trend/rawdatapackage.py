# -*- coding: utf-8 -*-
from minerva.storage.trend.datapackage import DataPackageBase
from minerva.directory.distinguishedname import explode
from minerva.util import grouped_by


class RawDataPackage(DataPackageBase):
    def __init__(self, granularity, timestamp, trend_names, rows):
        self.granularity = granularity
        self.timestamp = timestamp
        self.trend_names = trend_names
        self.rows = rows

    def get_entity_type_name(self):
        if self.rows:
            first_dn = self.rows[0][0]

            return entity_type_name_from_dn(first_dn)

    def get_key(self):
        return self.timestamp, self.get_entity_type_name(), self.granularity

    @staticmethod
    def merge_packages(packages):
        result = []

        for k, group in grouped_by(packages, RawDataPackage.get_key):
            l = list(group)
            result.append(package_group(k, l))

        return result

    @classmethod
    def entity_ref_type(cls):
        raise NotImplementedError()

    def entity_type_name(self):
        raise NotImplementedError()


def package_group(key, packages):
    timestamp_str, _entitytype_name, granularity = key

    all_field_names = set()
    dict_rows_by_dn = {}

    for p in packages:
        for dn, values in p.rows:
            value_dict = dict(zip(p.trend_names, values))

            dict_rows_by_dn.setdefault(dn, {}).update(value_dict)

        all_field_names.update(p.trend_names)

    field_names = list(all_field_names)

    rows = []
    for dn, value_dict in dict_rows_by_dn.items():
        values = [value_dict.get(f, "") for f in field_names]

        row = dn, values

        rows.append(row)

    return RawDataPackage(granularity, timestamp_str, field_names, rows)


def entity_type_name_from_dn(dn):
    """
    Return the entitytype name from a Distinguished Name
    """
    parts = explode(dn)

    return parts[-1][0]