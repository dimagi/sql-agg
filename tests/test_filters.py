from unittest2 import TestCase

from sqlalchemy import Column, String

from sqlagg.filters import *


class TestSqlAggViews(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.column_name = 'my_column'
        cls.column_dict = {
            'column_name': cls.column_name,
        }

        class MockTable(object):
            pass

        cls.mock_table = MockTable()
        setattr(cls.mock_table, 'c', [Column(cls.column_name, type_=String)])

    def test_raw(self):
        a = RAW('fancy stuff')
        self.assertEqual(str(a.build_expression(self.mock_table)), 'fancy stuff')

    def test_between(self):
        a = BETWEEN(self.column_name, 'start', 'end')
        self.assertEqual(
            str(a.build_expression(self.mock_table)),
            '%(column_name)s BETWEEN :start AND :end' % self.column_dict
        )

    def test_is_null(self):
        a = ISNULL(self.column_name)
        self.assertEqual(
            str(a.build_expression(self.mock_table)),
            '%(column_name)s IS NULL' % self.column_dict
        )

    def test_not_null(self):
        a = NOTNULL(self.column_name)
        self.assertEqual(
            str(a.build_expression(self.mock_table)),
            '%(column_name)s IS NOT NULL' % self.column_dict
        )

    def test_not(self):
        a = NOT(RAW('fancy'))
        self.assertEqual(str(a.build_expression(self.mock_table)), 'NOT fancy')

    def test_and(self):
        a = AND([RAW('fancy'), RAW('good')])
        self.assertEqual(str(a.build_expression(self.mock_table)), 'fancy AND good')

    def test_or(self):
        a = OR([RAW('fancy'), RAW('good')])
        self.assertEqual(str(a.build_expression(self.mock_table)), 'fancy OR good')

    def test_complex(self):
        filter = AND([
            AND([
                RAW('jack'), RAW('jill')
            ]),
            OR([
                RAW('water'), RAW('hill')
            ])])
        self.assertEqual(str(filter.build_expression(self.mock_table)), 'jack AND jill AND (water OR hill)')

    def test_in(self):
        a = INFilter(self.column_name, ('option_1', 'option_2'))
        self.assertEqual(
            str(a.build_expression(self.mock_table)),
            '%(column_name)s IN (:option_1, :option_2)' % self.column_dict
        )
