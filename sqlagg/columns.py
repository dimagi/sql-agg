from __future__ import absolute_import
from __future__ import unicode_literals
from sqlalchemy import func, distinct, case, text, cast, Integer, column
from sqlalchemy.dialects.postgresql import aggregate_order_by
from .base import BaseColumn, CustomQueryColumn, SqlColumn
import six
import uuid


class SimpleColumn(BaseColumn):
    pass


class YearColumn(BaseColumn):
    aggregate_fn = lambda _, y: func.extract('YEAR', y)


class MonthColumn(BaseColumn):
    aggregate_fn = lambda _, y: func.extract('MONTH', y)


class WeekColumn(BaseColumn):
    aggregate_fn = lambda _, y: func.extract('WEEK', y)


class DayColumn(BaseColumn):
    aggregate_fn = lambda _, y: func.extract('DAY', y)


class YearQuarterColumn(BaseColumn):
    aggregate_fn = lambda _, y: func.extract('QUARTER', y)


class DayOfWeekColumn(BaseColumn):
    aggregate_fn = lambda _, y: func.extract('DOW', y)


class DayOfYearColumn(BaseColumn):
    aggregate_fn = lambda _, y: func.extract('DOY', y)


class SumColumn(BaseColumn):
    aggregate_fn = func.sum


class CountColumn(BaseColumn):
    aggregate_fn = func.count


class MaxColumn(BaseColumn):
    aggregate_fn = func.max


class MinColumn(BaseColumn):
    aggregate_fn = func.min


class MeanColumn(BaseColumn):
    aggregate_fn = func.avg

    def get_value(self, row):
        value = super(MeanColumn, self).get_value(row)
        if value is not None:
            return float(value)


class NonzeroSumColumn(BaseColumn):
    aggregate_fn = lambda _, column: cast(func.sum(column) > 0, Integer)


class CountUniqueColumn(BaseColumn):
    aggregate_fn = lambda _, column: func.count(distinct(column))


class ConditionalAggregation(BaseColumn):
    def __init__(self, key=None, whens=None, else_=None, *args, **kwargs):
        super(ConditionalAggregation, self).__init__(key, *args, **kwargs)
        self.whens = whens or []
        self.else_ = else_

        assert self.key or self.alias, "Column must have either a key or an alias"

    @property
    def sql_column(self):
        return ConditionalColumn(self.key, self.whens, self.else_, self.aggregate_fn, self.alias)


class ArrayAggColumn(BaseColumn):
    """
    Perform array aggregation on a column
    Pass order_by_col to sort by another column within the group
    Example: array generated for a column col1, ordered by col2 with select clause like
    Select ARRAY_AGG(col1 ORDER BY col2), col3 ..
    """

    def __init__(self, key, order_by_col=None, *args, **kwargs):
        super(ArrayAggColumn, self).__init__(key, *args, **kwargs)
        self.order_by_col = order_by_col

    @property
    def sql_column(self):
        return ArrayAggSQLColumn(self.key, self.order_by_col, self.alias)


class SumWhen(ConditionalAggregation):
    """
    Without binds:
    SumWhen("vehicle", whens=[["unicycle", 1], ["bicycle", 2], ["car", 4]], else_=0, alias="num_wheels")

    With binds:
    SumWhen("age_cohort",
            whens=[
                ["age_in_months < ?", 6, 1],
                ["age_in_months < ?", 12, 2],
            ],
            else_=0,
            alias="half_years")
    """
    aggregate_fn = func.sum


class ConditionalColumn(SqlColumn):
    """
    Without binds:
    ConditionalColumn("vehicle",
                      whens=[["unicycle", 1], ["bicycle": 2], ["car": 4]],
                      else_=0,
                      aggregation_fn=func.sum,
                      alias="num_wheels")

    Or with binds:
    ConditionalColumn("age_cohort",
                      whens=[
                        ["age_in_months < ?", 6, 1],
                        ["age_in_months < ?", 12, 2],
                      ],
                      else_=0,
                      aggregation_fn=func.sum)
    """
    def __init__(self, column_name, whens, else_, aggregate_fn, alias):
        self.aggregate_fn = aggregate_fn
        self.column_name = column_name
        self.whens = whens
        self.else_ = else_
        self.alias = alias

    @property
    def label(self):
        return self.alias or self.column_name

    def build_column(self):
        if self.column_name:
            expr = case(value=column(self.column_name), whens=self.whens, else_=self.else_)
        else:
            expr = case(whens=self._build_whens(), else_=self.else_)

        if self.aggregate_fn:
            expr = self.aggregate_fn(expr)
        return expr.label(self.label)

    def _build_whens(self):
        whens = []
        for item in self.whens:
            when, *binds, then = item
            if binds:
                binds = list(reversed(binds))
                named_binds = {}
                when_with_named_binds = ''
                for letter in when:
                    if letter != '?':
                        when_with_named_binds += letter
                    else:
                        bind_name = 'p' + uuid.uuid4().hex
                        when_with_named_binds += ':' + bind_name
                        named_binds[bind_name] = binds.pop()
                when = text(when_with_named_binds).bindparams(**named_binds)
            else:
                when = text(when)
            then = text(then) if isinstance(then, six.string_types) else then
            whens.append((when, then))
        return whens


class ArrayAggSQLColumn(SqlColumn):
    def __init__(self, column_name, order_by_col, alias=None):
        self.column_name = column_name
        self.order_by_col = order_by_col
        self.alias = alias

    @property
    def label(self):
        return self.alias or self.column_name

    def build_column(self):
        table_column = column(self.column_name)
        if self.order_by_col:
            order_by_column = column(self.order_by_col)
            return func.array_agg(aggregate_order_by(table_column, order_by_column.asc())).label(self.label)
        return func.array_agg(table_column).label(self.label)
