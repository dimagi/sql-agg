from unittest2 import TestCase
from sqlagg.filters import *


class TestSqlAggViews(TestCase):

    def test_raw(self):
        a = RAW('fancy stuff')
        self.assertEqual(a.build_expression(), 'fancy stuff')

    def test_between(self):
        a = BETWEEN('date', 'start', 'end')
        self.assertEqual(a.build_expression(), '"date" between :start and :end')

    def test_operator(self):
        class TestOP(BasicFilter):
            operator = '<>'

        a = TestOP('date', 'start')
        self.assertEqual(a.build_expression(), '"date" <> :start')

    def test_is_null(self):
        a = ISNULL('date')
        self.assertEqual(a.build_expression(), '"date" IS NULL')

    def test_not_null(self):
        a = NOTNULL('date')
        self.assertEqual(a.build_expression(), '"date" IS NOT NULL')

    def test_not(self):
        a = NOT(RAW('fancy'))
        self.assertEqual(a.build_expression(), 'NOT fancy')

    def test_and(self):
        a = AND([RAW('fancy'), RAW('good')])
        self.assertEqual(a.build_expression(), '(fancy AND good)')

    def test_or(self):
        a = OR([RAW('fancy'), RAW('good')])
        self.assertEqual(a.build_expression(), '(fancy OR good)')

    def test_complex(self):
        filter = AND([
            AND([
                RAW('jack'), RAW('jill')
            ]),
            OR([
                RAW('water'), RAW('hill')
            ])])
        self.assertEqual(filter.build_expression(), '((jack AND jill) AND (water OR hill))')