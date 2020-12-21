import os
import random

import jinja2

template_file_name = "sample.csv"

def generate(target_dir, timestamp, granularity):
    dir_path = os.path.dirname(os.path.abspath(__file__))

    template_file_path = os.path.join(dir_path, template_file_name)

    with open(template_file_path) as template_file:
        template_data = template_file.read()

        template = jinja2.Template(template_data)

    timestamp_str = timestamp_to_str(timestamp)

    out_file_name = 'pm_{}.csv'.format(
        timestamp_to_filename_str(granularity.decr(timestamp), timestamp)
    )

    output_file_path = os.path.join(target_dir, out_file_name)

    data = {
        "timestamp": timestamp_str,
        "power_kwh": [random.uniform(0, 80) for _ in range(2)],
    }

    with open(output_file_path, 'w') as out_file:
        out_file.write(template.render(**data))

    return output_file_path


def timestamp_to_str(timestamp):
    plusminus, offset_hours, offset_minutes = offset_tuple(timestamp)

    offset_str = "{}{:02d}:{:02d}".format(plusminus, offset_hours, offset_minutes)

    return timestamp.strftime('%Y-%m-%dT%H:%M:%S') + offset_str


def timestamp_to_filename_str(start, end):
    """
    20190910.0000+0200-0015+0200
    """
    start_offset_str = "{}{:02d}{:02d}".format(*offset_tuple(start))
    end_offset_str = "{}{:02d}{:02d}".format(*offset_tuple(end))

    # 2019-10-10T00:15:00+02:00
    return '{}{}-{}{}'.format(
        start.strftime('%Y%m%d.%H%M'),
        start_offset_str,
        end.strftime('%H%M'),
        end_offset_str
    )


def offset_tuple(timestamp):
    offset = timestamp.tzinfo.utcoffset(timestamp)

    offset_seconds = offset.total_seconds()

    if (offset_seconds >= 0):
        plusminus = '+'
    else:
        plusminus = '-'

    offset_hours = int(offset_seconds / 60 / 60)
    offset_minutes = int((offset_seconds - (offset_hours * 60 * 60)) / 60)

    return plusminus, offset_hours, offset_minutes
