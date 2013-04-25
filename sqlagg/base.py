# -*- coding: utf-8 -*-
import sqlalchemy
import logging

query_logger = logging.getLogger("sqlagg.queries")


class SqlColumn(object):
    """
    Simple representation of a column with a name and an aggregation function which can be None.
    """
    def __init__(self, column_name, aggregate_fn=None, alias=None):
        self.column_name = column_name
        self.alias = alias or column_name
        self.aggregate_fn = aggregate_fn

    def build_column(self, sql_table):
        table_column = sql_table.c[self.column_name]
        sql_col = self.aggregate_fn(table_column) if self.aggregate_fn else table_column
        return sql_col.label(self.alias)

    def __repr__(self):
        return "SqlColumn(column_name=%s)" % self.column_name


class QueryMeta(object):
    def __init__(self, table_name, filters, group_by):
        self.filters = filters
        self.group_by = group_by
        self.table_name = table_name

    def append_column(self, view):
        pass

    def execute(self, metadata, connection, filter_values):
        raise NotImplementedError()


class SimpleQueryMeta(QueryMeta):
    """
    Metadata about a query including the table being queried, list of columns, filters and group by columns.
    """
    def __init__(self, table_name, filters, group_by):
        super(SimpleQueryMeta, self).__init__(table_name, filters, group_by)
        self.columns = []

    def append_column(self, column):
        self.columns.append(SqlColumn(column.key, column.aggregate_fn, column.alias))

    def _check(self):
        groups = list(self.group_by)
        for c in self.columns:
            if c.column_name in groups:
                groups.remove(c.column_name)

        for g in groups:
            self.columns.append(SqlColumn(g, aggregate_fn=None, alias=g))

    def execute(self, metadata, connection, filter_values):
        query = self._build_query(metadata)
        query_logger.debug("SimpleQuery:\n%s", query)
        return connection.execute(query, **filter_values).fetchall()

    def _build_query(self, metadata):
        self._check()
        table = metadata.tables[self.table_name]
        query = sqlalchemy.select()
        for group_key in self.group_by:
            query.append_group_by(table.c[group_key])

        for c in self.columns:
            query.append_column(c.build_column(table))

        if self.filters:
            for filter in self.filters:
                query.append_whereclause(filter)

        return query

    def __repr__(self):
        return "Querymeta(columns=%s, filters=%s, group_by=%s, table=%s)" % \
               (self.columns, self.filters, self.group_by, self.table_name)


class QueryContext(object):
    def __init__(self, table, filters={}, group_by=[]):
        self.table_name = table
        self.filters = filters
        self.group_by = group_by
        self.query_meta = {}

    def append_column(self, column):
        query_key = column.column_key
        query = self.query_meta.setdefault(query_key, self._new_query_meta(column))
        query.append_column(column)

    def _new_query_meta(self, column):
        if isinstance(column, QueryColumn):
            return column.get_query_meta(self.table_name, self.filters, self.group_by)
        else:
            table_name = column.table_name or self.table_name
            filters = column.filters or self.filters
            group_by = column.group_by or self.group_by
            return SimpleQueryMeta(table_name, filters, group_by)

    @property
    def metadata(self):
        if not hasattr(self, '_metadata'):
            self._metadata = sqlalchemy.MetaData()
            self._metadata.bind = self.connection
            self._metadata.reflect()

        return self._metadata

    def resolve(self, connection, filter_values=None):
        self.connection = connection

        data = {}
        for qm in self.query_meta.values():
            result = qm.execute(self.metadata, self.connection, filter_values or {})

            for r in result:
                row = data
                for group in qm.group_by:
                    row = row.setdefault(r[group], {})
                row.update(kvp for kvp in r.items())

        return data

    def __str__(self):
        return str(self.query_meta)


class QueryColumn(object):
    @property
    def column_key(self):
        raise NotImplementedError()

    def get_query_meta(self):
        raise NotImplementedError()