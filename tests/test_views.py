import unittest
from fixture import DataTestCase, SQLAlchemyFixture
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from models import metadata, UserTable, RegionTable
from fixtures import RegionData, UserData
from sqlagg import *

engine = create_engine('sqlite:///:memory:')
metadata.bind = engine
metadata.create_all()

db_fixture = SQLAlchemyFixture(
    engine=metadata.bind,
    env={"UserData": UserTable, "RegionData": RegionTable})


class TestSqlAggViews(DataTestCase, unittest.TestCase):
    fixture = db_fixture
    datasets = [UserData, RegionData]

    @classmethod
    def setUpClass(cls):
        Session = scoped_session(sessionmaker(bind=metadata.bind, autoflush=True))
        cls.session = Session()

    def test_sum(self):
        self._test_view(SumView("indicator_a"), 6)

    def test_count(self):
        self._test_view(CountView("indicator_c"), 2)

    def test_max(self):
        self._test_view(MaxView("indicator_a"), 3)

    def test_min(self):
        self._test_view(MinView("indicator_a"), 0)

    def test_mean(self):
        self._test_view(MeanView("indicator_a"), 1.5)

    def test_unique(self):
        self._test_view(UniqueView("user"), 2)

    def test_unique_2(self):
        self._test_view(UniqueView("sub_region", table_name="region_table"), 3)

    def _test_view(self, view, expected):
        data = self._get_view_data(view)
        self.assertEqual(data[view.key], expected)

    def _get_view_data(self, view):
        vc = ViewContext("user_table")
        vc.append_view(view)
        vc.resolve(self.session.connection())
        return vc.data

    def test_median(self):
        self._test_view(MedianView("indicator_a"), 1.5)

    def test_median_group(self):
        data = self._get_view_data(MedianView("indicator_a", group_by=["user"]))
        self.assertEqual(data["user1"]["indicator_a"], 2)
        self.assertEqual(data["user2"]["indicator_a"], 1)