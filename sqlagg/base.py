# -*- coding: utf-8 -*-
import sqlalchemy
import logging

logger = logging.getLogger(__name__)


class SqlColumn(object):
    """
    Simple representation of a column with a name and an aggregation function which can be None.
    """
    def __init__(self, column_name, aggregate_fn=None):
        self.column_name = column_name
        self.aggregate_fn = aggregate_fn

    def __repr__(self):
        return "SqlColumn(column_name=%s)" % (self.column_name)


class QueryMeta(object):
    """
    Metadata about a query including the table being queried, list of columns, filters and group by columns.
    """
    def __init__(self):
        self.columns = []
        self.filters = []
        self.group_by = []
        self.table_name = None

    def append_column(self, key, aggregate_fn):
        aggregate_fn = aggregate_fn or sqlalchemy.func.sum
        self.columns.append(SqlColumn(key, aggregate_fn if not key in self.group_by else None))

    def check(self):
        groups = list(self.group_by)
        for c in self.columns:
            if c.column_name in groups:
                groups.remove(c.column_name)

        for g in groups:
            self.append_column(g, None)

    def __repr__(self):
        return "Querymeta(columns=%s, filters=%s, group_by=%s, table=%s)" % \
               (self.columns, self.filters, self.group_by, self.table_name)


class ViewContext(object):
    def __init__(self, table, filters=None, group_by=None):
        self.table_name = table
        self.filters = filters
        self.group_by = group_by
        self.query_meta = {}
        self.data = {}

    def append_view(self, view):
        query_key = view.view_key
        query = self.query_meta.setdefault(query_key, self._new_query_meta(view))
        query.append_column(view.key, view.aggregate_fn)

    def _new_query_meta(self, view):
        qm = QueryMeta()
        qm.table_name = view.table_name or self.table_name
        qm.filters = view.filters or self.filters
        qm.group_by = view.group_by or self.group_by
        return qm

    @property
    def table(self):
        if not hasattr(self, '_table'):
            self._table = self.metadata.tables[self.table_name]

        return self._table

    @property
    def metadata(self):
        if not hasattr(self, '_metadata'):
            self._metadata = sqlalchemy.MetaData()
            self._metadata.bind = self.connection
            self._metadata.reflect()

        return self._metadata

    def resolve(self, connection, filter_values):
        self.connection = connection

        for qm in self.query_meta.values():
            qm.check()
            query = sqlalchemy.select()
            for group_key in qm.group_by:
                query.append_group_by(self.table.c[group_key])

            for c in qm.columns:
                col = self.table.c[c.column_name]
                sql_col = c.aggregate_fn(col) if c.aggregate_fn else col
                query.append_column(sql_col.label(c.column_name))

            if qm.filters:
                for filter in qm.filters:
                    query.append_whereclause(filter.format(**filter_values))

            logger.debug("%s", query)
            result = self.connection.execute(query).fetchall()
            for r in result:
                row = self.data
                for group in qm.group_by:
                    row = row.setdefault(r[group], {})
                row.update(kvp for kvp in r.items())

    def __str__(self):
        return str(self.query_meta)