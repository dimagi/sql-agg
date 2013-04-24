from sqlalchemy import func
from sqlagg import QueryView
from queries import MedianQueryMeta


class BaseColumnView(object):
    def __init__(self, key, as_name=None, aggregate_fn=None, table_name=None, filters=None, group_by=None):
        self.key = key
        self.as_name = as_name
        self.aggregate_fn = aggregate_fn
        self.table_name = table_name
        self.filters = filters
        self.group_by = group_by

        #TODO: allow 'having' e.g. count(x) having x > 4

    @property
    def view_key(self):
        return self.table_name, str(self.filters), str(self.group_by)


class SimpleView(BaseColumnView):
    def __init__(self, *args, **kwargs):
        super(SimpleView, self).__init__(*args, **kwargs)


class SumView(BaseColumnView):
    def __init__(self, *args, **kwargs):
        kwargs["aggregate_fn"] = func.sum
        super(SumView, self).__init__(*args, **kwargs)


class CountView(BaseColumnView):
    def __init__(self, *args, **kwargs):
        kwargs["aggregate_fn"] = func.count
        super(CountView, self).__init__(*args, **kwargs)


class MaxView(BaseColumnView):
    def __init__(self, *args, **kwargs):
        kwargs["aggregate_fn"] = func.max
        super(MaxView, self).__init__(*args, **kwargs)


class MinView(BaseColumnView):
    def __init__(self, *args, **kwargs):
        kwargs["aggregate_fn"] = func.min
        super(MinView, self).__init__(*args, **kwargs)


class MeanView(BaseColumnView):
    def __init__(self, *args, **kwargs):
        kwargs["aggregate_fn"] = func.avg
        super(MeanView, self).__init__(*args, **kwargs)


class UniqueView(BaseColumnView):
    def __init__(self, *args, **kwargs):
        from sqlalchemy import distinct

        kwargs["aggregate_fn"] = lambda c: func.count(distinct(c))
        super(UniqueView, self).__init__(*args, **kwargs)


class MedianView(QueryView):
    def __init__(self, key, as_name=None, table_name=None, filters=None, group_by=None):
        self.key = key
        self.as_name = as_name
        self.table_name = table_name
        self.filters = filters
        self.group_by = group_by

    def get_query_meta(self, default_table_name, default_filters, default_group_by):
        table_name = self.table_name or default_table_name
        filters = self.filters or default_filters
        group_by = self.group_by or default_group_by
        return MedianQueryMeta(table_name, filters, group_by)

    @property
    def view_key(self):
        return "median", self.key, self.table_name, str(self.filters), str(self.group_by)