import datetime

import pytz
from nose.tools import eq_, assert_raises

from minerva_storage_trend.tables import make_table_name, get_table_names
from minerva.directory.basetypes import DataSource

GP_QTR = 900
GP_HR = 3600
GP_DAY = 86400
GP_WEEK = 604800

def create_datasource(timezone):
	return DataSource(1, name="DummySource", description="Dummy data source",
			timezone="Europe/Amsterdam", storagetype="trend")


ENTITYTYPE_NAME = "DummyType"
DATASOURCE = create_datasource("Europe/Amsterdam")


def test_qtr_local():
	timestamp = DATASOURCE.tzinfo.localize(datetime.datetime(2008, 12, 3, 0, 15, 0))

	for ts in (timestamp + i * datetime.timedelta(0, GP_QTR) for i in range(96)):
		table_name = make_table_name(DATASOURCE, GP_QTR, ENTITYTYPE_NAME, ts)
		eq_(table_name, "dummysource_dummytype_qtr_20081203")


def test_qtr_local_dst():
	timestamp = DATASOURCE.tzinfo.localize(datetime.datetime(2023, 6, 10, 0, 15, 0))

	for ts in (timestamp + i * datetime.timedelta(0, GP_QTR) for i in range(96)):
		table_name = make_table_name(DATASOURCE, GP_QTR, ENTITYTYPE_NAME, ts)
		eq_(table_name, "dummysource_dummytype_qtr_20230610")


def test_qtr_local_at_midnight():
	timestamp1 = DATASOURCE.tzinfo.localize(datetime.datetime(2008, 12, 4, 0, 0, 0))
	table_name1 = make_table_name(DATASOURCE, GP_QTR, ENTITYTYPE_NAME, timestamp1)
	eq_(table_name1, "dummysource_dummytype_qtr_20081203")

	timestamp2 = DATASOURCE.tzinfo.localize(datetime.datetime(2009, 12, 4, 0, 0, 0))
	table_name2 = make_table_name(DATASOURCE, GP_QTR, ENTITYTYPE_NAME, timestamp2)
	eq_(table_name2, "dummysource_dummytype_qtr_20091203")


def test_qtr_local_after_midnight():
	timestamp = DATASOURCE.tzinfo.localize(datetime.datetime(2009, 12, 4, 0, 15, 0))
	table_name = make_table_name(DATASOURCE, GP_QTR, ENTITYTYPE_NAME, timestamp)
	eq_(table_name, "dummysource_dummytype_qtr_20091204")

	timestamp = DATASOURCE.tzinfo.localize(datetime.datetime(2008, 12, 4, 0, 15, 0))
	table_name = make_table_name(DATASOURCE, GP_QTR, ENTITYTYPE_NAME, timestamp)
	eq_(table_name, "dummysource_dummytype_qtr_20081204")


def test_qtr_utc():
	timestamp = pytz.UTC.localize(datetime.datetime(2008, 12, 3, 15, 0, 0))
	table_name = make_table_name(DATASOURCE, GP_QTR, ENTITYTYPE_NAME, timestamp)
	eq_(table_name, "dummysource_dummytype_qtr_20081203")


def test_qtr_utc_at_midnight():
	timestamp = pytz.UTC.localize(datetime.datetime(2008, 12, 3, 23, 0, 0))
	table_name = make_table_name(DATASOURCE, GP_QTR, ENTITYTYPE_NAME, timestamp)
	eq_(table_name, "dummysource_dummytype_qtr_20081203")


def test_qtr_utc_after_midnight():
	timestamp = pytz.UTC.localize(datetime.datetime(2008, 12, 4, 0, 15, 0))
	table_name = make_table_name(DATASOURCE, GP_QTR, ENTITYTYPE_NAME, timestamp)
	eq_(table_name, "dummysource_dummytype_qtr_20081204")


def test_hr_local():
	timestamp = DATASOURCE.tzinfo.localize(datetime.datetime(2012, 2, 24, 1, 0, 0))

	for ts in [timestamp + i * datetime.timedelta(0, GP_HR) for i in range(7 * 24)]:
		table_name = make_table_name(DATASOURCE, GP_HR, ENTITYTYPE_NAME, ts)
		eq_(table_name, "dummysource_dummytype_hr_20120224")

	timestamp = DATASOURCE.tzinfo.localize(datetime.datetime(2012, 3, 2, 1, 0, 0))

	for ts in [timestamp + i * datetime.timedelta(0, GP_HR) for i in range(7 * 24)]:
		table_name = make_table_name(DATASOURCE, GP_HR, ENTITYTYPE_NAME, ts)
		eq_(table_name, "dummysource_dummytype_hr_20120302")


def test_hr_local_dst():
	timestamp = DATASOURCE.tzinfo.localize(datetime.datetime(2011, 6, 3, 1, 0, 0))

	for ts in [timestamp + i * datetime.timedelta(0, GP_HR) for i in range(7 * 24)]:
		table_name = make_table_name(DATASOURCE, GP_HR, ENTITYTYPE_NAME, ts)
		eq_(table_name, "dummysource_dummytype_hr_20110603")

#def test_hr_pre_epoch_dst():
#	timestamp = DATASOURCE.tzinfo.localize(datetime.datetime(1969, 7, 11, 1, 0, 0))
#
#	for ts in [timestamp + i * datetime.timedelta(0, GP_HR) for i in range(7 * 24)]:
#		table_name = make_table_name(DATASOURCE, GP_HR, ENTITYTYPE_NAME, ts)
#		eq_(table_name, "dummysource_dummytype_hr_19690711")

	# Check old pre unix time timestamps within an interval of 7 days
#	timestamp = DATASOURCE.tzinfo.localize(datetime.datetime(1065, 12, 29, 1, 0, 0))
#
#	for ts in [timestamp + i * datetime.timedelta(0, GP_HR) for i in range(7 * 24)]:
#		table_name = make_table_name(DATASOURCE, GP_HR, ENTITYTYPE_NAME, ts)
#		eq_(table_name, "dummysource_dummytype_hr_10651229")


def test_day():
	timestamp = DATASOURCE.tzinfo.localize(datetime.datetime(2012, 2, 22, 0, 0, 0))

	for ts in (timestamp + i * datetime.timedelta(0, GP_DAY) for i in range(30)):
		table_name = make_table_name(DATASOURCE, GP_DAY, ENTITYTYPE_NAME, ts)
		eq_(table_name, "dummysource_dummytype_day_20120221")


def test_day_dst():
	timestamp = DATASOURCE.tzinfo.localize(datetime.datetime(2008, 5, 13, 0, 0, 0))

	for ts in (timestamp + i * datetime.timedelta(0, GP_DAY) for i in range(30)):
		table_name = make_table_name(DATASOURCE, GP_DAY, ENTITYTYPE_NAME, ts)
		eq_(table_name, "dummysource_dummytype_day_20080512")


def test_day_local_dst():
	timestamp = DATASOURCE.tzinfo.localize(datetime.datetime(2013, 7, 16, 0, 0, 0))

	for ts in (timestamp + i * datetime.timedelta(0, GP_DAY) for i in range(30)):
		table_name = make_table_name(DATASOURCE, GP_DAY, ENTITYTYPE_NAME, ts)
		eq_(table_name, "dummysource_dummytype_day_20130715")


def test_week():
	timestamp = DATASOURCE.tzinfo.localize(datetime.datetime(2011, 12, 31, 0, 0, 0))

	for ts in (timestamp + i * datetime.timedelta(0, GP_WEEK) for i in range(30)):
		table_name = make_table_name(DATASOURCE, GP_WEEK, ENTITYTYPE_NAME, ts)
		eq_(table_name, "dummysource_dummytype_wk_20111223")


def test_naive_timestamp_exception():
	naive_ts = datetime.datetime(2011, 12, 7, 11, 15)
	non_naive_ts = DATASOURCE.tzinfo.localize(datetime.datetime(2011, 12, 7, 11, 15))

	assert_raises(TypeError, make_table_name, DATASOURCE, GP_QTR, ENTITYTYPE_NAME, naive_ts)
	assert_raises(TypeError, get_table_names, DATASOURCE, GP_QTR, ENTITYTYPE_NAME,
		naive_ts, naive_ts + datetime.timedelta(1))
	assert_raises(TypeError, get_table_names, DATASOURCE, GP_QTR, ENTITYTYPE_NAME,
		non_naive_ts, naive_ts + datetime.timedelta(1))
	assert_raises(TypeError, get_table_names, DATASOURCE, GP_QTR, ENTITYTYPE_NAME,
		naive_ts, non_naive_ts + datetime.timedelta(1))

