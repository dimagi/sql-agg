from __future__ import absolute_import
from __future__ import unicode_literals
import collections
from functools import total_ordering

from sqlalchemy import bindparam, text, column
from sqlalchemy.sql import operators, and_, or_, not_

from sqlagg.exceptions import SqlAggException


class NotEqMixin(object):
    def __ne__(self, other):
        return not self.__eq__(other)


@total_ordering
class SqlFilter(NotEqMixin):
    def build_expression(self):
        raise NotImplementedError()

    def __lt__(self, other):
        """Ordering is required for consistent sorting when creating column keys"""
        return hash(self) < hash(other)


class RawFilter(SqlFilter):
    def __init__(self, expression):
        self.expression = expression

    def build_expression(self):
        return text(self.expression)

    def __eq__(self, other):
        return isinstance(other, RawFilter) and self.expression == other.expression

    def __hash__(self):
        return hash((RawFilter, self.expression))

    def __repr__(self):
        return "SQL({})".format(self.expression)


class BasicFilter(SqlFilter):
    operator = None
    operator_string = None

    def __init__(self, column_name, parameter, operator=None):
        self.column_name = column_name
        self.parameter = parameter
        if operator:
            self.operator = operator

    def build_expression(self):
        if not self.operator:
            raise SqlAggException('Operator missing')

        return self.operator(column(self.column_name), bindparam(self.parameter))

    def __eq__(self, other):
        return (
            isinstance(other, BasicFilter)
            and self.column_name == other.column_name
            and self.parameter == other.parameter
            and self.operator == other.operator
        )

    def __hash__(self):
        return hash((type(self), self.column_name, self.parameter, self.operator))

    def __repr__(self):
        return "SQL({} {} {})".format(self.column_name, self.operator_string, self.parameter)


class BetweenFilter(SqlFilter):
    def __init__(self, column_name, lower_param, upper_param):
        self.column_name = column_name
        self.lower_param = lower_param
        self.upper_param = upper_param

    def build_expression(self):
        return column(self.column_name).between(
            bindparam(self.lower_param), bindparam(self.upper_param)
        )

    def __eq__(self, other):
        return (
            isinstance(other, BetweenFilter)
            and self.column_name == other.column_name
            and self.lower_param == other.lower_param
            and self.upper_param == other.upper_param
        )
    def __hash__(self):
        return hash((BetweenFilter, self.column_name, self.lower_param, self.upper_param))


class GTFilter(BasicFilter):
    operator = operators.gt
    operator_string = '>'


class GTEFilter(BasicFilter):
    operator = operators.ge
    operator_string = '>='


class LTFilter(BasicFilter):
    operator = operators.lt
    operator_string = '<'


class LTEFilter(BasicFilter):
    operator = operators.le
    operator_string = '<='


class EQFilter(BasicFilter):
    operator = operators.eq
    operator_string = '='


class NOTEQFilter(BasicFilter):
    operator = operators.ne
    operator_string = '!='


class INFilter(BasicFilter):
    """
    This filter requires that the parameter value be a tuple.
    """
    operator_string = 'in'
    def build_expression(self):
        assert isinstance(self.parameter, collections.Iterable)
        return operators.in_op(
            column(self.column_name),
            tuple(bindparam(param) for param in self.parameter)
        )

    def __eq__(self, other):
        return (
            isinstance(other, INFilter)
            and self.column_name == other.column_name
            and self.operator == other.operator
            and sorted(self.parameter) == sorted(other.parameter)
        )

    def __hash__(self):
        return hash((type(self), self.column_name, self.operator, tuple(sorted(self.parameter))))


class ISNULLFilter(SqlFilter):
    def __init__(self, column_name):
        self.column_name = column_name

    def build_expression(self):
        return column(self.column_name).is_(None)

    def __eq__(self, other):
        return isinstance(other, ISNULLFilter) and self.column_name == other.column_name

    def __hash__(self):
        return hash((ISNULLFilter, self.column_name))

    def __repr__(self):
        return "SQL({} IS NULL)"


class NOTNULLFilter(SqlFilter):
    def __init__(self, column_name):
        self.column_name = column_name

    def build_expression(self):
        return column(self.column_name).isnot(None)

    def __eq__(self, other):
        return isinstance(other, NOTNULLFilter) and self.column_name == other.column_name

    def __hash__(self):
        return hash((NOTNULLFilter, self.column_name))

    def __repr__(self):
        return "SQL({} NOT NULL)"


class NOTFilter(SqlFilter):
    def __init__(self, filter):
        self.filter = filter

    def build_expression(self):
        return not_(self.filter.build_expression())

    def __eq__(self, other):
        return isinstance(other, NOTFilter) and self.filter == other.filter

    def __hash__(self):
        return hash((NOTFilter, self.filter))


class ANDFilter(SqlFilter):
    """
    Lets you construct AND operations on filters.
    """
    def __init__(self, filters):
        self.filters = filters
        assert len(self.filters) > 1

    def build_expression(self):
        return and_(*[f.build_expression() for f in self.filters])

    def __eq__(self, other):
        return isinstance(other, ANDFilter) and set(self.filters) == set(other.filters)

    def __hash__(self):
        return hash((NOTFilter,) + tuple(sorted(self.filters)))

    def __repr__(self):
        return "SQL({})".format(' AND '.join(self.filters))


class ORFilter(SqlFilter):
    """
    Lets you construct OR operations on filters.
    """
    def __init__(self, filters):
        self.filters = filters
        assert len(self.filters) > 1

    def build_expression(self):
        return or_(*[f.build_expression() for f in self.filters])

    def __eq__(self, other):
        return isinstance(other, ORFilter) and set(self.filters) == set(other.filters)

    def __hash__(self):
        return hash((ORFilter,) + tuple(sorted(self.filters)))

    def __repr__(self):
        return "SQL({})".format(' OR '.join(self.filters))


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
