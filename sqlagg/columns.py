from sqlalchemy import func, distinct, case, text
from queries import MedianQueryMeta
from .base import BaseColumn, CustomQueryColumn, SqlColumn


class SimpleColumn(BaseColumn):
    pass


class YearColumn(BaseColumn):
    aggregate_fn = lambda _, y: func.extract('YEAR', y)


class MonthColumn(BaseColumn):
    aggregate_fn = lambda _, y: func.extract('MONTH', y)


class DayColumn(BaseColumn):
    aggregate_fn = lambda _, y: func.extract('DAY', y)


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


class UniqueColumn(BaseColumn):
    aggregate_fn = lambda view, column: func.count(distinct(column))


class MedianColumn(CustomQueryColumn):
    query_cls = MedianQueryMeta
    name = "median"


class ConditionalAggregation(BaseColumn):
    def __init__(self, key=None, whens={}, else_=None, *args, **kwargs):
        super(ConditionalAggregation, self).__init__(key, *args, **kwargs)
        self.whens = whens
        self.else_ = else_

        assert self.key or self.alias, "Column must have either a key or an alias"

    @property
    def sql_column(self):
        return ConditionalColumn(self.key, self.whens, self.else_, self.aggregate_fn, self.alias)


class SumWhen(ConditionalAggregation):
    """
    SumWhen("vehicle", whens={"unicycle": 1, "bicycle": 2, "car": 4}, else_=0, alias="num_wheels")
    """
    aggregate_fn = func.sum


class ConditionalColumn(SqlColumn):
    """
    ConditionalColumn("vehicle",
                      whens={"unicycle": 1, "bicycle": 2, "car": 4},
                      else_=0,
                      aggregation_fn=func.sum,
                      alias="num_wheels")
    """
    def __init__(self, column_name, whens, else_, aggregate_fn, alias):
        self.aggregate_fn = aggregate_fn
        self.column_name = column_name
        self.whens = whens
        self.else_ = else_
        self.alias = alias or column_name

    def build_column(self, sql_table):
        if self.column_name:
            expr = case(value=sql_table.c[self.column_name], whens=self.whens, else_=self.else_)
        else:
            whens = {}
            for when, then in self.whens.items():
                if isinstance(then, basestring):
                    whens[text(when)] = text(then)
                else:
                    whens[text(when)] = then

            expr = case(whens=whens, else_=self.else_)

        return self.aggregate_fn(expr).label(self.alias)
