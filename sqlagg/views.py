import sqlalchemy


class BaseColumnView(object):
    def __init__(self, key, aggregate_fn=None, table_name=None, filters=None, group_by=None):
        self.key = key
        self.aggregate_fn = aggregate_fn
        self.table_name = table_name
        self.filters = filters
        self.group_by = group_by

    @property
    def view_key(self):
        return self.table_name, str(self.filters), str(self.group_by)

    def get_value(self, row):
        return row.get(self.key, None) if row else None


class SimpleView(BaseColumnView):
    def __init__(self, *args, **kwargs):
        super(SimpleView, self).__init__(*args, **kwargs)


class SumView(BaseColumnView):
    def __init__(self, *args, **kwargs):
        kwargs["aggregate_fn"] = sqlalchemy.func.sum
        super(SumView, self).__init__(*args, **kwargs)


class CountView(BaseColumnView):
    def __init__(self, *args, **kwargs):
        kwargs["aggregate_fn"] = sqlalchemy.func.count
        super(CountView, self).__init__(*args, **kwargs)