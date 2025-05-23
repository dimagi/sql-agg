import datetime

from sqlagg import (
    AggregateColumn,
    AliasColumn,
    CountColumn,
    MaxColumn,
    MeanColumn,
    MinColumn,
    QueryContext,
    SumColumn,
    SumWhen,
)
from sqlagg.columns import (
    ArrayAggColumn,
    ConditionalAggregation,
    CountUniqueColumn,
    DayColumn,
    DayOfWeekColumn,
    DayOfYearColumn,
    MonthColumn,
    NonzeroSumColumn,
    SimpleColumn,
    WeekColumn,
    YearColumn,
    YearQuarterColumn,
)
from sqlagg.filters import EQ
from sqlagg.sorting import OrderBy

from . import DataTestCase


class TestSqlAggViews(DataTestCase):

    def test_column_key(self):
        self.assertEqual(hash(SumColumn("").column_key), hash(SumColumn("").column_key))
        self.assertNotEqual(
            hash(SumColumn("", table_name='a').column_key),
            hash(SumColumn("", table_name='b').column_key)
        )
        self.assertEqual(
            hash(SumColumn("", filters=[EQ('a', 'b')]).column_key),
            hash(SumColumn("", filters=[EQ('a', 'b')]).column_key)
        )
        # different filter order doesn't matter
        self.assertEqual(
            hash(SumColumn("", filters=[EQ('a', 'b'), EQ('c', 'd')]).column_key),
            hash(SumColumn("", filters=[EQ('c', 'd'), EQ('a', 'b')]).column_key)
        )
        self.assertEqual(
            hash(SumColumn("", filters=[EQ('a', 'b')], group_by=['month', 'day']).column_key),
            hash(SumColumn("", filters=[EQ('a', 'b')], group_by=['month', 'day']).column_key)
        )
        # different group order does matter
        self.assertNotEqual(
            hash(SumColumn("", filters=[EQ('a', 'b')], group_by=['month', 'day']).column_key),
            hash(SumColumn("", filters=[EQ('a', 'b')], group_by=['day', 'month']).column_key)
        )

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

    def test_nonzero_sum(self):
        self._test_view(NonzeroSumColumn("indicator_a"), 1)
        self._test_view(NonzeroSumColumn("indicator_b"), 1)
        self._test_view(NonzeroSumColumn("indicator_c"), 1)
        self._test_view(NonzeroSumColumn("indicator_d"), 0)

    def test_alias_column(self):
        vc = QueryContext("user_table")
        i_a = SumColumn("indicator_a")
        i_a2 = AliasColumn("indicator_a")
        vc.append_column(i_a)
        vc.append_column(i_a2)
        data = vc.resolve(self.session.connection())
        self.assertEqual(len(data), 1)
        self.assertEqual(i_a.get_value(data[0]), 6)
        self.assertEqual(i_a2.get_value(data[0]), 6)

    def test_alias_column_with_aliases(self):
        vc = QueryContext("user_table")
        i_a = SumColumn("indicator_a", alias="sum_a")
        i_a2 = AliasColumn("sum_a")
        vc.append_column(i_a)
        vc.append_column(i_a2)
        data = vc.resolve(self.session.connection())
        self.assertEqual(len(data), 1)
        self.assertEqual(i_a.get_value(data[0]), 6)
        self.assertEqual(i_a2.get_value(data[0]), 6)

    def test_aggregate_column(self):
        col = AggregateColumn(lambda x, y: x + y,
                              SumColumn("indicator_a"),
                              SumColumn("indicator_c"))
        self._test_view(col, 9)

    def test_conditional_column_simple(self):
        # sum(case user when 'user1' then 1 when 'user2' then 3 else 0)
        col = SumWhen('user', whens=[['user1', 1], ['user2', 3]], else_=0)
        self._test_view(col, 8)

    def test_conditional_column_complex(self):
        # sum(case when indicator_a < 1 OR indicator_a > 2 then 1 else 0)
        col = SumWhen(whens=[['user_table.indicator_a < 1 OR user_table.indicator_a > 2', 1]], alias='a')
        self._test_view(col, 2)

        # sum(case when indicator_a between 1 and 2 then 0 else 1)
        col = SumWhen(whens=[['user_table.indicator_a between 1 and 2', 0]], else_=1, alias='a')
        self._test_view(col, 2)

        # with binds: sum(case when indicator_a between 1 and 2 then 0 else 1)
        col = SumWhen(whens=[['user_table.indicator_a between ? and ?', 1, 2, 0]], else_=1, alias='a')
        self._test_view(col, 2)

    def test_conditional_column_multi(self):
        # sum(case user when 'user1' then indicator_a else 0)
        col = SumWhen(whens=[["user_table.user = 'user1'", 'indicator_a']], else_=0, alias='a')
        self._test_view(col, 4)

    def test_group_by_conditional(self):
        vc = QueryContext("user_table", group_by=['bucket'])
        vc.append_column(ConditionalAggregation(whens=[
            ["indicator_a between 0 and 1", "'0-1'"],
            ["indicator_a between 2 and 2", "'2'"],
        ], else_='3+', alias='bucket'))
        vc.append_column(CountColumn('user'))
        result = vc.resolve(self.session.connection())
        self.assertEqual(result, {
            '0-1': {'bucket': '0-1', 'user': 2},
            '2': {'bucket': '2', 'user': 1},
            '3+': {'bucket': '3+', 'user': 1},
        })

    def test_array_agg_with_order(self):
        vc = QueryContext("region_table", group_by=['region', 'sub_region'])
        vc.append_column(AliasColumn('region'))
        array_agg_column = ArrayAggColumn('indicator_a', 'date')
        vc.append_column(array_agg_column)
        result = vc.resolve(self.session.connection())
        self.assertEqual(result, {
            (u'region1', u'region1_a'): {'indicator_a': [0, 1], 'region': 'region1', 'sub_region': 'region1_a'},
            (u'region1', u'region1_b'): {'indicator_a': [3, 1], 'region': 'region1', 'sub_region': 'region1_b'},
            (u'region2', u'region2_a'): {'indicator_a': [2], 'region': 'region2', 'sub_region': 'region2_a'},
        })

    def test_array_agg_without_order(self):
        vc = QueryContext("region_table", group_by=['region', 'sub_region'])
        vc.append_column(AliasColumn('region'))
        array_agg_column = ArrayAggColumn('indicator_a')
        vc.append_column(array_agg_column)
        result = vc.resolve(self.session.connection())
        self.assertEqual(result, {
            (u'region1', u'region1_a'): {'indicator_a': [1, 0], 'region': 'region1', 'sub_region': 'region1_a'},
            (u'region1', u'region1_b'): {'indicator_a': [3, 1], 'region': 'region1', 'sub_region': 'region1_b'},
            (u'region2', u'region2_a'): {'indicator_a': [2], 'region': 'region2', 'sub_region': 'region2_a'},
        })

    def test_month(self):
        vc = QueryContext("user_table", group_by=['month'])
        vc.append_column(MonthColumn('date', alias='month'))
        result = vc.resolve(self.session.connection())
        self.assertEqual(result, {1.0: {'month': 1.0}, 2.0: {'month': 2.0}, 3.0: {'month': 3.0}})

    def test_day(self):
        vc = QueryContext("user_table", group_by=['day'])
        vc.append_column(DayColumn('date', alias='day'))
        result = vc.resolve(self.session.connection())
        self.assertEqual(result, {1.0: {'day': 1.0}})

    def test_year(self):
        vc = QueryContext("user_table", group_by=['year'])
        vc.append_column(YearColumn('date', alias='year'))
        result = vc.resolve(self.session.connection())
        self.assertEqual(result, {2013.0: {'year': 2013.0}})

    def test_week(self):
        vc = QueryContext("user_table", group_by=['week'])
        vc.append_column(WeekColumn('date', alias='week'))
        result = vc.resolve(self.session.connection())
        self.assertEqual(result, {1.0: {'week': 1.0}, 5.0: {'week': 5.0}, 9.0: {'week': 9.0}})

    def test_day_of_week(self):
        vc = QueryContext("user_table", group_by=['dow'])
        vc.append_column(DayOfWeekColumn('date', alias='dow'))
        result = vc.resolve(self.session.connection())
        self.assertEqual(result, {2.0: {'dow': 2.0}, 5.0: {'dow': 5.0}})

    def test_day_of_year(self):
        vc = QueryContext("user_table", group_by=['doy'])
        vc.append_column(DayOfYearColumn('date', alias='doy'))
        result = vc.resolve(self.session.connection())
        self.assertEqual(result, {1.0: {'doy': 1.0}, 32.0: {'doy': 32.0}, 60.0: {'doy': 60.0}})

    def test_quarter(self):
        vc = QueryContext("user_table", group_by=['q'])
        vc.append_column(YearQuarterColumn('date', alias='q'))
        result = vc.resolve(self.session.connection())
        self.assertEqual(result, {1.0: {'q': 1.0}})

    def _test_view(self, view, expected):
        data = self._get_view_data(view)
        self.assertEqual(len(data), 1)
        value = view.get_value(data[0])
        self.assertAlmostEqual(float(value), float(expected))

    def _get_view_data(self, view):
        vc = QueryContext("user_table")
        vc.append_column(view)
        return vc.resolve(self.session.connection())

    def test_distinct_on(self):
        vc = QueryContext(
            "user_table",
            distinct_on=['user', 'year'],
            order_by=[OrderBy('user'), OrderBy('year'), OrderBy('date', is_ascending=False)],
            group_by=['user', 'date']
        )
        vc.append_column(SimpleColumn('user'))
        vc.append_column(YearColumn('date', alias='year'))
        vc.append_column(SimpleColumn('indicator_a'))
        result = vc.resolve(self.session.connection())
        self.assertEqual(result, {
            ('user1', datetime.date(2013, 2, 1)): {'user': 'user1', 'year': 2013.0, 'indicator_a': 3,
                                                   'date': datetime.date(2013, 2, 1)},
            ('user2', datetime.date(2013, 3, 1)): {'user': 'user2', 'year': 2013.0, 'indicator_a': 2,
                                                   'date': datetime.date(2013, 3, 1)}
        })
