from unittest2 import TestCase
from . import BaseTest, engine
from sqlalchemy.orm import sessionmaker

from sqlagg import *

Session = sessionmaker()


class TestSqlAggViews(BaseTest, TestCase):
    def setUp(self):
        self.connection = engine.connect()
        self.trans = self.connection.begin()
        self.session = Session(bind=self.connection)
        super(TestSqlAggViews, self).setUp()

    def tearDown(self):
        super(TestSqlAggViews, self).tearDown()
        self.trans.commit()
        self.session.close()
        self.connection.close()

    def test_missing_data(self):
        self.assertIsNone(SumColumn("not there").get_value({}))

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

    def test_alias_column(self):
        vc = QueryContext("user_table")
        i_a = SumColumn("indicator_a")
        i_a2 = AliasColumn("indicator_a")
        vc.append_column(i_a)
        vc.append_column(i_a2)
        data = vc.resolve(self.session.connection())
        self.assertEqual(i_a.get_value(data), 6)
        self.assertEqual(i_a2.get_value(data), 6)

    def test_alias_column_with_aliases(self):
        vc = QueryContext("user_table")
        i_a = SumColumn("indicator_a", alias="sum_a")
        i_a2 = AliasColumn("sum_a")
        vc.append_column(i_a)
        vc.append_column(i_a2)
        data = vc.resolve(self.session.connection())
        self.assertEqual(i_a.get_value(data), 6)
        self.assertEqual(i_a2.get_value(data), 6)

    def test_aggregate_column(self):
        col = AggregateColumn(lambda x, y: x + y,
                              SumColumn("indicator_a"),
                              SumColumn("indicator_c"))
        self._test_view(col, 9)

    def test_conditional_column_simple(self):
        # sum(case user when 'user1' then 1 when 'user2' then 3 else 0)
        col = SumWhen('user', whens={'user1': 1, 'user2': 3}, else_=0)
        self._test_view(col, 8)

    def test_conditional_column_complex(self):
        # sum(case when indicator_a < 1 OR indicator_a > 2 then 1 else 0)
        col = SumWhen(whens={'user_table.indicator_a < 1 OR user_table.indicator_a > 2': 1}, alias='a')
        self._test_view(col, 2)

        # sum(case when indicator_a between 1 and 2 then 0 else 1)
        col = SumWhen(whens={'user_table.indicator_a between 1 and 2': 0}, else_=1, alias='a')
        self._test_view(col, 2)

    def _test_view(self, view, expected):
        data = self._get_view_data(view)
        value = view.get_value(data)
        self.assertAlmostEqual(float(value), float(expected))

    def _get_view_data(self, view):
        vc = QueryContext("user_table")
        vc.append_column(view)
        return vc.resolve(self.session.connection())
