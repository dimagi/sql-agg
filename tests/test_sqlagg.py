from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

from datetime import date

from sqlalchemy.exc import ProgrammingError

from sqlagg import QueryContext, BaseColumn, DuplicateColumnsException, AggregateColumn
from sqlagg.columns import SimpleColumn, SumColumn, CountColumn, MeanColumn
from sqlagg.filters import LT, GTE, GT, AND, EQ
from sqlagg.sorting import OrderBy
from . import DataTestCase


class TestSqlAgg(DataTestCase):

    def test_single_group(self):
        data = self._get_user_data(None, None)

        self.assertEqual(data['user1']['indicator_a'], 4)
        self.assertEqual(data['user1']['indicator_b'], 2)
        self.assertEqual(data['user2']['indicator_a'], 2)
        self.assertEqual(data['user2']['indicator_b'], 2)

    def test_filters(self):
        filters = [LT('date', 'enddate')]
        filter_values = {"enddate": date(2013, 2, 1)}

        data = self._get_user_data(filter_values, filters)
        self.assertEqual(data['user1']['indicator_a'], 1)
        self.assertEqual(data['user1']['indicator_b'], 1)
        self.assertEqual(data['user2']['indicator_a'], 0)
        self.assertEqual(data['user2']['indicator_b'], 1)

    def test_filters_multiple(self):
        def test(filters):
            filter_values = {"startdate": date(2013, 2, 20), "enddate": date(2013, 3, 5)}

            data = self._get_user_data(filter_values, filters)
            self.assertNotIn('user1', data)
            self.assertEqual(data['user2']['indicator_a'], 2)
            self.assertEqual(data['user2']['indicator_b'], 1)

        test([GTE('date', 'startdate'), LT('date', 'enddate')])
        test([AND([GTE('date', 'startdate'), LT('date', 'enddate')])])

    def test_multiple_groups(self):
        data = self._get_region_data()

        r1_a = ('region1', 'region1_a')
        self.assertEqual(data[r1_a]['indicator_a'], 1)
        self.assertEqual(data[r1_a]['indicator_b'], 2)

        r1_b = ('region1', 'region1_b')
        self.assertEqual(data[r1_b]['indicator_a'], 4)
        self.assertEqual(data[r1_b]['indicator_b'], 2)

        r2_a = ('region2', 'region2_a')
        self.assertEqual(data[r2_a]['indicator_a'], 2)
        self.assertEqual(data[r2_a]['indicator_b'], 1)

    def test_different_filters(self):
        filters = [LT('date', 'enddate')]
        filter_values = {"enddate": date(2013, 2, 1)}
        vc = QueryContext("user_table", filters=filters, group_by=["user"])
        user = SimpleColumn("user")
        i_a = SumColumn("indicator_a")
        i_b = SumColumn("indicator_b", filters=[GT('date', 'enddate')])
        vc.append_column(user)
        vc.append_column(i_a)
        vc.append_column(i_b)
        self.assertEqual(2, len(vc.get_query_strings(self.session.connection())))
        data = vc.resolve(self.session.connection(), filter_values)
        self.assertEqual(data['user1']['indicator_a'], 1)
        self.assertNotIn('indicator_b', data['user1'])
        self.assertEqual(data['user2']['indicator_a'], 0)
        self.assertEqual(data['user2']['indicator_b'], 1)

    def test_alias(self):
        filters = [LT('date', 'enddate')]
        filter_values = {"enddate": date(2013, 4, 1)}
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

    def test_group_by_missing_column(self):
        vc = QueryContext("user_table", group_by=["user"])
        i_a = SumColumn("indicator_a")
        vc.append_column(i_a)
        data = vc.resolve(self.session.connection())

        self.assertEqual(data['user1']['indicator_a'], 4)
        self.assertEqual(data['user2']['indicator_a'], 2)

    def test_group_by_aliased_column(self):
        vc = QueryContext("user_table", group_by=["user"])
        user = SimpleColumn("user", alias="aliased_column")
        i_a = SumColumn("indicator_a")
        vc.append_column(user)
        vc.append_column(i_a)
        data = vc.resolve(self.session.connection())

        self.assertEqual(data['user1']['indicator_a'], 4)
        self.assertEqual(data['user2']['indicator_a'], 2)

    def test_custom_view(self):
        from sqlalchemy import func
        vc = QueryContext("user_table", filters=None, group_by=[])

        class CustomColumn(BaseColumn):
            aggregate_fn = lambda view, col: func.avg(col) / func.sum(col)

        agg_view = CustomColumn("indicator_a")
        vc.append_column(agg_view)
        data = vc.resolve(self.session.connection(), None)

        self.assertEqual(len(data), 1)
        self.assertAlmostEqual(float(data[0]["indicator_a"]), float(0.25))

    def test_multiple_tables(self):
        filters = [LT('date', 'enddate')]
        filter_values = {"enddate": date(2013, 4, 1)}
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

    def test_missing_table(self):
        vc = QueryContext("missing_table", group_by=["user"])
        user = SimpleColumn("user")
        i_a = SumColumn("indicator_a")
        vc.append_column(user)
        vc.append_column(i_a)

        with self.assertRaises(ProgrammingError):
            vc.resolve(self.session.connection())

    def test_missing_column(self):
        vc = QueryContext("user_table", group_by=["user"])
        user = SimpleColumn("user_missing")
        i_a = SumColumn("indicator_a")
        vc.append_column(user)
        vc.append_column(i_a)

        with self.assertRaises(ProgrammingError):
            vc.resolve(self.session.connection())

    def test_totals_no_filter(self):
        vc = QueryContext(
            "user_table",
            group_by=["user"],
        )

        vc.append_column(MeanColumn('indicator_a'))
        vc.append_column(SumColumn('indicator_b'))
        vc.append_column(SumColumn('indicator_c'))

        self.assertEqual(
            vc.totals(
                self.session.connection(),
                [
                    'indicator_a',
                    'indicator_b',
                    'indicator_c',
                ],
            ),
            {
                'indicator_a': 3,
                'indicator_b': 5,
                'indicator_c': 3,
            },
        )

    def test_totals_with_filter(self):
        vc = QueryContext(
            "user_table",
            filters=[EQ('user', 'username')],
            group_by=["user"],
        )

        for column_name in [
            'indicator_a',
            'indicator_b',
            'indicator_c',
        ]:
            vc.append_column(SumColumn(column_name))

        self.assertEqual(
            vc.totals(
                self.session.connection(),
                [
                    'indicator_a',
                    'indicator_b',
                    'indicator_c',
                ],
                {'username': 'user1'},
            ),
            {
                'indicator_a': 4,
                'indicator_b': 1,
                'indicator_c': 1,
            },
        )

    def test_count_group_by(self):
        vc = QueryContext(
            "user_table",
            group_by=["user"],
            order_by=[OrderBy("user", is_ascending=True)]
        )

        vc.append_column(SumColumn('indicator_a'))
        self.assertEqual(2, vc.count(self.session.connection()))

    def test_count_with_filter(self):
        vc = QueryContext(
            "user_table",
            filters=[EQ('user', 'username')],
            group_by=["user"],
        )

        vc.append_column(SumColumn('indicator_a'))
        self.assertEqual(1, vc.count(self.session.connection(), {'username': 'user1'},))

    def test_count_no_grouping(self):
        vc = QueryContext(
            "user_table",
            filters=[EQ('user', 'username')],
        )

        vc.append_column(SimpleColumn('indicator_a'))
        self.assertEqual(2, vc.count(self.session.connection(), {'username': 'user1'},))

    def test_count_group_no_agg(self):
        # due to past idiosyncrasy of sqlagg CommCare was adding a group by on
        # primary keys as follows:
        #
        #   SELECT username_ea02198f AS column_0, doc_id AS doc_id FROM ucr_table_x GROUP BY doc_id
        #
        # When doing a count of this query we have to do a subquery

        vc = QueryContext(
            "user_table",
            group_by=['indicator_a']
        )

        vc.append_column(SimpleColumn('indicator_a'))
        self.assertEqual(4, vc.count(self.session.connection(),))

    def test_count_with_nulls(self):
        vc = QueryContext(
            "user_table",
        )

        self.assertEqual(4, vc.count(self.session.connection(),))

    def test_user_view_data(self):
        data = self._get_user_view_data(None, None)

        self.assertEqual(data['user1']['indicator_a'], 4)
        self.assertEqual(data['user1']['indicator_b'], 2)
        self.assertEqual(data['user2']['indicator_a'], 2)
        self.assertEqual(data['user2']['indicator_b'], 2)

    def test_duplicate_columns(self):
        vc = QueryContext("user_table")
        vc.append_column(SimpleColumn("user"))
        vc.append_column(SumColumn("indicator_a"))
        vc.append_column(AggregateColumn(
            sum, SumColumn("indicator_a"), CountColumn("indicator_b")
        ))
        with self.assertRaises(DuplicateColumnsException):
            vc.resolve(self.session.connection())

    def test_order_by_non_existant_column(self):
        vc = QueryContext(
            "user_table",
            group_by=["user"],
            order_by=[OrderBy('lol')],
        )
        vc.append_column(SumColumn("indicator_a"))

        with self.assertRaises(ProgrammingError):
            vc.resolve(self.session.connection())

    def test_order_by_sql_injextion(self):
        vc = QueryContext(
            "user_table",
            group_by=["region"],
            order_by=[OrderBy('user; DROP TABLE user_table CASCADE; SELECT * FROM region_table ORDER BY region')],
        )
        vc.append_column(SumColumn("indicator_a"))

        with self.assertRaises(ProgrammingError):
            vc.resolve(self.session.connection())

    def _get_user_data(self, filter_values, filters):
        vc = QueryContext("user_table", filters=filters, group_by=["user"])
        user = SimpleColumn("user")
        i_a = SumColumn("indicator_a")
        i_b = CountColumn("indicator_b")
        vc.append_column(user)
        vc.append_column(i_a)
        vc.append_column(i_b)
        return vc.resolve(self.session.connection(), filter_values)

    def _get_region_data(self):
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

    def _get_user_view_data(self, filter_values, filters):
        vc = QueryContext("user_view", filters=filters, group_by=["user"])
        user = SimpleColumn("user")
        i_a = SumColumn("indicator_a")
        i_b = CountColumn("indicator_b")
        vc.append_column(user)
        vc.append_column(i_a)
        vc.append_column(i_b)
        return vc.resolve(self.session.connection(), filter_values)
