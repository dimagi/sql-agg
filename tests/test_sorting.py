from __future__ import absolute_import
from unittest import TestCase

from sqlagg.sorting import OrderBy


class TestSorting(TestCase):
    def test_order_by(self):
        self.assertEqual(
            str(OrderBy('column_1').build_expression()),
            'column_1 ASC'
        )
        self.assertEqual(
            str(OrderBy('column_1', is_ascending=True).build_expression()),
            'column_1 ASC'
        )
        self.assertEqual(
            str(OrderBy('column_1', is_ascending=False).build_expression()),
            'column_1 DESC'
        )
