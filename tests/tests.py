import unittest
from fixture import DataTestCase, SQLAlchemyFixture
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from datetime import date

from models import metadata, UserTable, RegionTable
from fixtures import RegionData, UserData
from sqlagg import *
from sqlagg.views import *

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
        filters = ["date < '{enddate}'"]
        filter_values = {"enddate": date(2013, 02, 01)}

        data = self.get_user_data(filter_values, filters)
        self.assertEqual(data['user1']['indicator_a'], 1)
        self.assertEqual(data['user1']['indicator_b'], 1)
        self.assertEqual(data['user2']['indicator_a'], 0)
        self.assertEqual(data['user2']['indicator_b'], 1)

    def test_filters_multiple(self):
        filters = ["date > '{startdate}'", "date < '{enddate}'"]
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

    def get_user_data(self, filter_values, filters):
        vc = ViewContext("user_table", filters=filters, group_by=["user"])
        user = SimpleView("user")
        i_a = SumView("indicator_a")
        i_b = CountView("indicator_b")
        vc.append_view(user)
        vc.append_view(i_a)
        vc.append_view(i_b)
        vc.resolve(self.session.connection(), filter_values)
        data = vc.data
        return data

    def get_region_data(self):
        vc = ViewContext("region_table", filters=None, group_by=["region", "sub_region"])
        region = SimpleView("region")
        sub_region = SimpleView("sub_region")
        i_a = SumView("indicator_a")
        i_b = CountView("indicator_b")
        vc.append_view(region)
        vc.append_view(sub_region)
        vc.append_view(i_a)
        vc.append_view(i_b)
        vc.resolve(self.session.connection(), None)
        data = vc.data
        return data