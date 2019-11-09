from __future__ import absolute_import
from __future__ import unicode_literals
from sqlalchemy import func, distinct, case, text, cast, Integer, column
from .base import BaseColumn, CustomQueryColumn, SqlColumn
import six


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


class SumWhen(ConditionalAggregation):
    """
    SumWhen("vehicle", whens=[["unicycle", 1], ["bicycle", 2], ["car", 4]], else_=0, alias="num_wheels")
    """
    aggregate_fn = func.sum


class SumWhenWithBinds(SumWhen):
    """
    SumWhen("age_cohort",
            whens=[
                ["when": "age < :ten", "then": 1],
                ["when": "age < :thirty", "then": 2],
            binds={
                "ten": 10,
                "thirty": 30,
            },
            else_=0)
    """
    def __init__(self, key=None, whens=None, binds=None, else_=None, *args, **kwargs):
        super(SumWhenWithBinds, self).__init__(key, whens, else_, *args, **kwargs)
        self.binds = binds or {}

    @property
    def sql_column(self):
        return ConditionalTemplateColumn(self.key, self.whens, self.binds, self.else_,
                                         self.aggregate_fn, self.alias)


class ConditionalColumn(SqlColumn):
    """
    ConditionalColumn("vehicle",
                      whens=[["unicycle", 1], ["bicycle": 2], ["car": 4]],
                      else_=0,
                      aggregation_fn=func.sum,
                      alias="num_wheels")
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
            when, then = item
            when = text(when)
            then = text(then) if isinstance(then, six.string_types) else then
            whens.append((when, then))
        return whens


class ConditionalTemplateColumn(ConditionalColumn):
    """
    ConditionalTemplateColumn("age_cohort",
                              whens=[
                                ["when": "age < :ten", "then": 1],
                                ["when": "age < :thirty", "then": 2],
                              ],
                              binds={
                                "ten": 10,
                                "thirty": 30,
                              },
                              else_=0,
                              aggregation_fn=func.sum)
    """
    def __init__(self, column_name, whens, binds, else_, aggregate_fn, alias):
        super(ConditionalTemplateColumn, self).__init__(column_name, whens, else_, aggregate_fn, alias)
        self.binds = binds or {}

    def _build_whens(self):
        whens = []
        for item in self.whens:
            when, then = item
            params = {key: value for key, value in self.binds.items() if ':' + key in when}
            when = text(when).bindparams(**params)
            then = text(then) if isinstance(then, six.string_types) else then
            whens.append((when, then))
        return whens
