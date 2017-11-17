from __future__ import absolute_import
import collections

from sqlalchemy import bindparam, text
from sqlalchemy.sql import operators, and_, or_, not_

from sqlagg.exceptions import ColumnNotFoundException


def get_column(table, column_name):
    for column in table.c:
        if column.name == column_name:
            return column
    raise ColumnNotFoundException('column with name "%s" not found' % column_name)


class SqlFilter(object):
    def build_expression(self, table):
        raise NotImplementedError()

    def __str__(self):
        return 'SqlFilter(%s)' % self.build_expression()


class RawFilter(SqlFilter):
    def __init__(self, expression):
        self.expression = expression

    def build_expression(self, table):
        return text(self.expression)


class BasicFilter(SqlFilter):
    operator = None

    def __init__(self, column_name, parameter, operator=None):
        self.column_name = column_name
        self.parameter = parameter
        if operator:
            self.operator = operator

    def build_expression(self, table):
        if not self.operator:
            raise 'Operator missing'

        return self.operator(get_column(table, self.column_name), bindparam(self.parameter))


class BetweenFilter(SqlFilter):
    def __init__(self, column_name, lower_param, upper_param):
        self.column_name = column_name
        self.lower_param = lower_param
        self.upper_param = upper_param

    def build_expression(self, table):
        return get_column(table, self.column_name).between(
            bindparam(self.lower_param), bindparam(self.upper_param)
        )


class GTFilter(BasicFilter):
    operator = operators.gt


class GTEFilter(BasicFilter):
    operator = operators.ge


class LTFilter(BasicFilter):
    operator = operators.lt


class LTEFilter(BasicFilter):
    operator = operators.le


class EQFilter(BasicFilter):
    operator = operators.eq


class NOTEQFilter(BasicFilter):
    operator = operators.ne


class INFilter(BasicFilter):
    """
    This filter requires that the parameter value be a tuple.
    """
    def build_expression(self, table):
        assert isinstance(self.parameter, collections.Iterable)
        return operators.in_op(
            get_column(table, self.column_name),
            tuple(bindparam(param) for param in self.parameter)
        )

class ISNULLFilter(SqlFilter):
    def __init__(self, column_name):
        self.column_name = column_name

    def build_expression(self, table):
        return get_column(table, self.column_name).is_(None)


class NOTNULLFilter(SqlFilter):
    def __init__(self, column_name):
        self.column_name = column_name

    def build_expression(self, table):
        return get_column(table, self.column_name).isnot(None)


class NOTFilter(SqlFilter):
    def __init__(self, filter):
        self.filter = filter

    def build_expression(self, table):
        return not_(self.filter.build_expression(table))


class ANDFilter(SqlFilter):
    """
    Lets you construct AND operations on filters.
    """
    def __init__(self, filters):
        self.filters = filters
        assert len(self.filters) > 1

    def build_expression(self, table):
        return and_(*[f.build_expression(table) for f in self.filters])


class ORFilter(SqlFilter):
    """
    Lets you construct OR operations on filters.
    """
    def __init__(self, filters):
        self.filters = filters
        assert len(self.filters) > 1

    def build_expression(self, table):
        return or_(*[f.build_expression(table) for f in self.filters])


RAW = RawFilter
BETWEEN = BetweenFilter
GT = GTFilter
GTE = GTEFilter
LT = LTFilter
LTE = LTEFilter
EQ = EQFilter
NOTEQ = NOTEQFilter
IN = INFilter
ISNULL = ISNULLFilter
NOTNULL = NOTNULLFilter
NOT = NOTFilter
AND = ANDFilter
OR = ORFilter
