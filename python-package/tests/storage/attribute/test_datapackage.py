from datetime import datetime

import pytz

from minerva.storage.attribute.datapackage import DataPackage


def test_deduce_datatypes():
    timestamp = pytz.utc.localize(datetime.utcnow())
    datapackage = DataPackage(
        timestamp=timestamp,
        attribute_names=('height', 'power', 'refs'),
        rows=[
            (10034, ['15.6', '68', ['r32', 'r44', 'r50']])
        ]
    )

    data_types = datapackage.deduce_data_types()

    assert data_types[0] == 'real'
    assert data_types[1] == 'smallint'
    assert data_types[2] == 'text'
