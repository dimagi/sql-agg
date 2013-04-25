import unittest
from . import BaseTest
from sqlalchemy.orm import scoped_session, sessionmaker

from sqlagg import *


class TestSqlAggViews(BaseTest, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        Session = scoped_session(sessionmaker(bind=cls.metadata().bind, autoflush=True))
        cls.session = Session()

    def test_sum(self):
        self._test_view(SumColumn("indicator_a"), 6)

    def test_count(self):
        self._test_view(CountColumn("indicator_c"), 2)

    def test_max(self):
        self._test_view(MaxColumn("indicator_a"), 3)

    def test_min(self):
        self._test_view(MinColumn("indicator_a"), 0)

    def test_mean(self):
        self._test_view(MeanColumn("indicator_a"), 1.5)

    def test_unique(self):
        self._test_view(UniqueColumn("user"), 2)

    def test_unique_2(self):
        self._test_view(UniqueColumn("sub_region", table_name="region_table"), 3)

    def test_median(self):
        self._test_view(MedianColumn("indicator_a"), 1.5)

    def test_median_group(self):
        data = self._get_view_data(MedianColumn("indicator_a", group_by=["user"]))
        self.assertEqual(data["user1"]["indicator_a"], 2)
        self.assertEqual(data["user2"]["indicator_a"], 1)

    def _test_view(self, view, expected):
        data = self._get_view_data(view)
        self.assertEqual(view.get_value(data), expected)

    def _get_view_data(self, view):
        vc = QueryContext("user_table")
        vc.append_column(view)
        return vc.resolve(self.session.connection())