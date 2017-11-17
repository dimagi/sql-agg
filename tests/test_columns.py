from __future__ import absolute_import
from unittest2 import TestCase
from . import BaseTest, engine
from sqlalchemy.orm import sessionmaker

from sqlagg import *
from sqlagg.columns import MonthColumn, DayColumn, YearColumn, WeekColumn, CountUniqueColumn, DayOfWeekColumn, DayOfYearColumn, YearQuarterColumn

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
        self._test_view(CountUniqueColumn("user"), 2)

    def test_unique_2(self):
        self._test_view(CountUniqueColumn("sub_region", table_name="region_table"), 3)

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

    def test_conditional_column_multi(self):
        # sum(case user when 'user1' then indicator_a else 0)
        col = SumWhen(whens={"user_table.user = 'user1'": 'indicator_a'}, else_=0, alias='a')
        self._test_view(col, 4)

    def test_month(self):
        vc = QueryContext("user_table", group_by=['month'])
        vc.append_column(MonthColumn('date', alias='month'))
        result = vc.resolve(self.session.connection())
        self.assertEquals(result, {1.0: {'month': 1.0}, 2.0: {'month': 2.0}, 3.0: {'month': 3.0}})

    def test_day(self):
        vc = QueryContext("user_table", group_by=['day'])
        vc.append_column(DayColumn('date', alias='day'))
        result = vc.resolve(self.session.connection())
        self.assertEquals(result, {1.0: {'day': 1.0}})

    def test_year(self):
        vc = QueryContext("user_table", group_by=['year'])
        vc.append_column(YearColumn('date', alias='year'))
        result = vc.resolve(self.session.connection())
        self.assertEquals(result, {2013.0: {'year': 2013.0}})

    def test_week(self):
        vc = QueryContext("user_table", group_by=['week'])
        vc.append_column(WeekColumn('date', alias='week'))
        result = vc.resolve(self.session.connection())
        self.assertEquals(result, {1.0: {u'week': 1.0}, 5.0: {u'week': 5.0}, 9.0: {u'week': 9.0}})

    def test_day_of_week(self):
        vc = QueryContext("user_table", group_by=['dow'])
        vc.append_column(DayOfWeekColumn('date', alias='dow'))
        result = vc.resolve(self.session.connection())
        self.assertEquals(result, {2.0: {u'dow': 2.0}, 5.0: {u'dow': 5.0}})

    def test_day_of_year(self):
        vc = QueryContext("user_table", group_by=['doy'])
        vc.append_column(DayOfYearColumn('date', alias='doy'))
        result = vc.resolve(self.session.connection())
        self.assertEquals(result, {1.0: {u'doy': 1.0}, 32.0: {u'doy': 32.0}, 60.0: {u'doy': 60.0}})

    def test_quarter(self):
        vc = QueryContext("user_table", group_by=['q'])
        vc.append_column(YearQuarterColumn('date', alias='q'))
        result = vc.resolve(self.session.connection())
        self.assertEquals(result, {1.0: {u'q': 1.0}})

    def _test_view(self, view, expected):
        data = self._get_view_data(view)
        value = view.get_value(data)
        self.assertAlmostEqual(float(value), float(expected))

    def _get_view_data(self, view):
        vc = QueryContext("user_table")
        vc.append_column(view)
        return vc.resolve(self.session.connection())
