# Minerva database interface library

The Python interface and administration tooling to the Minerva database.

[![License](https://img.shields.io/github/license/hendrikx-itc/minerva-etl)](LICENSE)
[![pypi](https://img.shields.io/pypi/v/minerva-etl.svg)](https://pypi.org/project/minerva-etl/)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/c5362e2ab5e64942af78dd09ab70afa7)](https://www.codacy.com/gh/hendrikx-itc/minerva-etl/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=hendrikx-itc/minerva-etl&amp;utm_campaign=Badge_Grade)

Minerva is an open-source ETL platform optimized for real-time big data
processing. It relies on the advanced functionality and performance of
PostgreSQL.

To access an existing Minerva instance from code, or to set up a new Minerva
instance, this is the component you need to use.

## Dependencies and requirements

Minerva depends on the following software:

* postgresql-libs (at least 9.1)
* python2 (at least 2.7)
* python2-setuptools
* python2-yaml
* python2-pytz
* python2-psycopg2 (at least 2.2.1)

The PostgreSQL database server version must be at least 9.1.

Run `python2 setup.py install` inside the `python-package` directory to install Minerva.

## License

Minerva is distributed under [AGPL-3.0-only](LICENSE).
