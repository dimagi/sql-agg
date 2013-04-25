from sqlalchemy import func, distinct
from .base import QueryColumn
from queries import MedianQueryMeta


class BaseColumnColumn(object):
    aggregate_fn = None

    def __init__(self, key, as_name=None, table_name=None, filters=None, group_by=None):
        self.key = key
        self.as_name = as_name
        self.table_name = table_name
        self.filters = filters
        self.group_by = group_by

        #TODO: allow 'having' e.g. count(x) having x > 4

    @property
    def view_key(self):
        return self.table_name, str(self.filters), str(self.group_by)


class CustomQueryColumn(BaseColumnColumn, QueryColumn):
    query_cls = None
    name = None

    def get_query_meta(self, default_table_name, default_filters, default_group_by):
        table_name = self.table_name or default_table_name
        filters = self.filters or default_filters
        group_by = self.group_by or default_group_by
        return self.query_cls(table_name, filters, group_by)

    @property
    def view_key(self):
        return self.name, self.key, self.table_name, str(self.filters), str(self.group_by)


class SimpleColumn(BaseColumnColumn):
    pass


class SumColumn(BaseColumnColumn):
    aggregate_fn = func.sum


class CountColumn(BaseColumnColumn):
    aggregate_fn = func.count


class MaxColumn(BaseColumnColumn):
    aggregate_fn = func.max


class MinColumn(BaseColumnColumn):
    aggregate_fn = func.min


class MeanColumn(BaseColumnColumn):
    aggregate_fn = func.avg


class UniqueColumn(BaseColumnColumn):
    aggregate_fn = lambda view, column: func.count(distinct(column))


class MedianColumn(CustomQueryColumn):
    query_cls = MedianQueryMeta
    name = "median"