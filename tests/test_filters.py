from __future__ import absolute_import
from __future__ import unicode_literals
from unittest import TestCase

from sqlalchemy import Column, String

from sqlagg.filters import *


class TestSqlAggViews(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.column_name = 'my_column'
        cls.column_dict = {
            'column_name': cls.column_name,
        }

    def test_raw(self):
        a = RAW('fancy stuff')
        self.assertEqual(str(a.build_expression()), 'fancy stuff')
        self._test_equality(a, RAW('fancy stuff'), RAW('other'))

    def test_between(self):
        a = BETWEEN(self.column_name, 'start', 'end')
        self.assertEqual(
            str(a.build_expression()),
            '%(column_name)s BETWEEN :start AND :end' % self.column_dict
        )
        self._test_equality(a, BETWEEN(self.column_name, 'start', 'end'), BETWEEN(self.column_name, 'start', 'other'))

    def test_is_null(self):
        a = ISNULL(self.column_name)
        self.assertEqual(
            str(a.build_expression()),
            '%(column_name)s IS NULL' % self.column_dict
        )
        self._test_equality(a, ISNULL(self.column_name), ISNULL('ohter'))

    def test_not_null(self):
        a = NOTNULL(self.column_name)
        self.assertEqual(
            str(a.build_expression()),
            '%(column_name)s IS NOT NULL' % self.column_dict
        )
        self._test_equality(a, NOTNULL(self.column_name), NOTNULL('other'))

    def test_not(self):
        a = NOT(RAW('fancy'))
        self.assertEqual(str(a.build_expression()), 'NOT fancy')
        self._test_equality(a, NOT(RAW('fancy')), NOT(RAW('other')))

    def test_and(self):
        a = AND([RAW('fancy'), RAW('good')])
        self.assertEqual(str(a.build_expression()), 'fancy AND good')
        self._test_equality(a, AND([RAW('fancy'), RAW('good')]), AND([RAW('fancy'), RAW('other')]))
        self.assertNotEqual(a, AND([RAW('fancy'), RAW('good'), RAW('other')]))

        # test ordering
        self.assertEqual(
            AND([OR([RAW('a'), EQFilter('b', 'c')]), NOT(BETWEEN('d', 'e', 'f'))]),
            AND([NOT(BETWEEN('d', 'e', 'f')), OR([EQFilter('b', 'c'), RAW('a')])])
        )

    def test_or(self):
        a = OR([RAW('fancy'), RAW('good')])
        self.assertEqual(str(a.build_expression()), 'fancy OR good')
        self._test_equality(a, OR([RAW('fancy'), RAW('good')]), OR([RAW('fancy'), RAW('other')]))
        self.assertNotEqual(a, OR([RAW('fancy'), RAW('good'), RAW('other')]))

        # test ordering
        self.assertEqual(
            OR([AND([RAW('a'), EQFilter('b', 'c')]), NOT(BETWEEN('d', 'e', 'f'))]),
            OR([NOT(BETWEEN('d', 'e', 'f')), AND([EQFilter('b', 'c'), RAW('a')])])
        )

    def test_complex(self):
        filter = AND([
            AND([
                RAW('jack'), RAW('jill')
            ]),
            OR([
                RAW('water'), RAW('hill')
            ])])
        self.assertEqual(str(filter.build_expression()), 'jack AND jill AND (water OR hill)')

    def test_in(self):
        a = INFilter(self.column_name, ('option_1', 'option_2'))
        self.assertEqual(
            str(a.build_expression()),
            '%(column_name)s IN (:option_1, :option_2)' % self.column_dict
        )
        self._test_equality(
            a,
            INFilter(self.column_name, ('option_1', 'option_2')),
            INFilter(self.column_name, ('option_1', 'option_3'))
        )
        self.assertEqual(a, INFilter(self.column_name, ('option_2', 'option_1')))

    def _test_equality(self, filterA, filterB, filterC):
        self.assertEqual(hash(filterA), hash(filterB))
        self.assertEqual(filterA, filterB)
        self.assertNotEqual(filterA, filterC)
        self.assertNotEqual(hash(filterA), hash(filterC))
        self.assertNotEqual(filterB, filterC)
        self.assertNotEqual(hash(filterB), hash(filterC))
