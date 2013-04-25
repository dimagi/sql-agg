import unittest
from fixture import DataTestCase, SQLAlchemyFixture
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from datetime import date

from models import metadata, UserTable, RegionTable
from fixtures import RegionData, UserData
from sqlagg import *
from sqlagg.columns import *

engine = create_engine('sqlite:///:memory:')
metadata.bind = engine
metadata.create_all()

db_fixture = SQLAlchemyFixture(
    engine=metadata.bind,
    env={"UserData": UserTable, "RegionData": RegionTable})


class TestSqlAgg(DataTestCase, unittest.TestCase):
    fixture = db_fixture
    datasets = [UserData, RegionData]

    @classmethod
    def setUpClass(cls):
        Session = scoped_session(sessionmaker(bind=metadata.bind, autoflush=True))
        cls.session = Session()

    def test_single_group(self):
        data = self.get_user_data(None, None)

        self.assertEqual(data['user1']['indicator_a'], 4)
        self.assertEqual(data['user1']['indicator_b'], 2)
        self.assertEqual(data['user2']['indicator_a'], 2)
        self.assertEqual(data['user2']['indicator_b'], 2)

    def test_filters(self):
        filters = ["date < :enddate"]
        filter_values = {"enddate": date(2013, 02, 01)}

        data = self.get_user_data(filter_values, filters)
        self.assertEqual(data['user1']['indicator_a'], 1)
        self.assertEqual(data['user1']['indicator_b'], 1)
        self.assertEqual(data['user2']['indicator_a'], 0)
        self.assertEqual(data['user2']['indicator_b'], 1)

    def test_filters_multiple(self):
        filters = ["date > :startdate", "date < :enddate"]
        filter_values = {"startdate": date(2013, 02, 20), "enddate": date(2013, 03, 05)}

        data = self.get_user_data(filter_values, filters)
        self.assertNotIn('user1', data)
        self.assertEqual(data['user2']['indicator_a'], 2)
        self.assertEqual(data['user2']['indicator_b'], 1)

    def test_multiple_groups(self):
        data = self.get_region_data()

        region1 = data['region1']
        region1_a = region1['region1_a']
        self.assertEqual(region1_a['indicator_a'], 1)
        self.assertEqual(region1_a['indicator_b'], 2)

        region1_b = region1['region1_b']
        self.assertEqual(region1_b['indicator_a'], 4)
        self.assertEqual(region1_b['indicator_b'], 2)

        region2 = data['region2']
        region2_a = region2['region2_a']
        self.assertEqual(region2_a['indicator_a'], 2)
        self.assertEqual(region2_a['indicator_b'], 1)

    def test_different_filters(self):
        filters = ["date < :enddate"]
        filter_values = {"enddate": date(2013, 02, 01)}
        vc = QueryContext("user_table", filters=filters, group_by=["user"])
        user = SimpleColumn("user")
        i_a = SumColumn("indicator_a")
        i_b = SumColumn("indicator_b", filters=["date > :enddate"])
        vc.append_column(user)
        vc.append_column(i_a)
        vc.append_column(i_b)
        data = vc.resolve(self.session.connection(), filter_values)

        self.assertEqual(data['user1']['indicator_a'], 1)
        self.assertNotIn('indicator_b', data['user1'])
        self.assertEqual(data['user2']['indicator_a'], 0)
        self.assertEqual(data['user2']['indicator_b'], 1)

    def test_alias(self):
        filters = ["date < :enddate"]
        filter_values = {"enddate": date(2013, 04, 01)}
        vc = QueryContext("user_table", filters=filters, group_by=["user"])
        user = SimpleColumn("user")
        i_sum_a = SumColumn("indicator_a", alias="sum_a")
        i_count_a = CountColumn("indicator_a", alias="count_a")
        vc.append_column(user)
        vc.append_column(i_sum_a)
        vc.append_column(i_count_a)
        data = vc.resolve(self.session.connection(), filter_values)

        self.assertEqual(data['user1']['sum_a'], 4)
        self.assertEqual(data['user1']['count_a'], 2)
        self.assertEqual(data['user2']['sum_a'], 2)
        self.assertEqual(data['user2']['count_a'], 2)

    def test_custom_view(self):
        from sqlalchemy import func
        vc = QueryContext("user_table", filters=None, group_by=[])

        class CustomColumn(BaseColumnColumn):
            aggregate_fn = lambda view, col: func.avg(col) / func.sum(col)

        agg_view = CustomColumn("indicator_a")
        vc.append_column(agg_view)
        data = vc.resolve(self.session.connection(), None)

        self.assertEqual(data["indicator_a"], 0.25)

    def test_multiple_tables(self):
        filters = ["date < :enddate"]
        filter_values = {"enddate": date(2013, 04, 01)}
        vc = QueryContext("user_table", filters=filters, group_by=["user"])
        user = SimpleColumn("user")
        i_a = SumColumn("indicator_a")

        region = SimpleColumn("region", table_name="region_table", group_by=["region"])
        i_a_r = SumColumn("indicator_a", table_name="region_table", group_by=["region"])
        vc.append_column(user)
        vc.append_column(i_a)

        vc.append_column(region)
        vc.append_column(i_a_r)
        data = vc.resolve(self.session.connection(), filter_values)

        self.assertEqual(data['user1']['indicator_a'], 4)
        self.assertEqual(data['user2']['indicator_a'], 2)
        self.assertEqual(data['region1']['indicator_a'], 5)
        self.assertEqual(data['region2']['indicator_a'], 2)

    def get_user_data(self, filter_values, filters):
        vc = QueryContext("user_table", filters=filters, group_by=["user"])
        user = SimpleColumn("user")
        i_a = SumColumn("indicator_a")
        i_b = CountColumn("indicator_b")
        vc.append_column(user)
        vc.append_column(i_a)
        vc.append_column(i_b)
        return vc.resolve(self.session.connection(), filter_values)

    def get_region_data(self):
        vc = QueryContext("region_table", filters=None, group_by=["region", "sub_region"])
        region = SimpleColumn("region")
        sub_region = SimpleColumn("sub_region")
        i_a = SumColumn("indicator_a")
        i_b = CountColumn("indicator_b")
        vc.append_column(region)
        vc.append_column(sub_region)
        vc.append_column(i_a)
        vc.append_column(i_b)
        return vc.resolve(self.session.connection(), None)