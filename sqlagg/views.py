from sqlalchemy import func, distinct
from sqlagg import QueryView
from queries import MedianQueryMeta


class BaseColumnView(object):
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


class CustomQueryView(BaseColumnView, QueryView):
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


class SimpleView(BaseColumnView):
    pass


class SumView(BaseColumnView):
    aggregate_fn = func.sum


class CountView(BaseColumnView):
    aggregate_fn = func.count


class MaxView(BaseColumnView):
    aggregate_fn = func.max


class MinView(BaseColumnView):
    aggregate_fn = func.min


class MeanView(BaseColumnView):
    aggregate_fn = func.avg


class UniqueView(BaseColumnView):
    aggregate_fn = lambda view, column: func.count(distinct(column))


class MedianView(CustomQueryView):
    query_cls = MedianQueryMeta
    name = "median"