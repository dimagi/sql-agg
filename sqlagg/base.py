# -*- coding: utf-8 -*-
import sqlalchemy
from sqlagg.filters import SqlFilter


class TableNotFoundException(Exception):
    pass


class ColumnNotFoundException(Exception):
    pass


class SqlColumn(object):
    def build_column(self, sql_table):
        raise NotImplementedError()


class SimpleSqlColumn(SqlColumn):
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
        self.columns.append(column.sql_column)

    def _check(self):
        if self.group_by:
            groups = list(self.group_by)
            for c in self.columns:
                if c.column_name in groups:
                    groups.remove(c.column_name)
                elif c.alias in groups:
                    groups.remove(c.alias)

            for g in groups:
                self.columns.append(SimpleSqlColumn(g, aggregate_fn=None, alias=g))

    def execute(self, metadata, connection, filter_values):
        query = self._build_query(metadata)
        return connection.execute(query, **filter_values).fetchall()

    def _build_query(self, metadata):
        self._check()
        try:
            table = metadata.tables[self.table_name]
        except KeyError:
            raise TableNotFoundException("Unable to query table, table not found: %s" % self.table_name)

        try:
            query = sqlalchemy.select()
            if self.group_by:
                cols = [c.column_name for c in self.columns]
                alias = [c.alias for c in self.columns]
                for group_key in self.group_by:
                    if group_key in cols:
                        query.append_group_by(table.c[group_key])
                    elif group_key in alias:
                        query.append_group_by(group_key)

            for c in self.columns:
                query.append_column(c.build_column(table))
        except KeyError as e:
            raise ColumnNotFoundException("Missing column in table (%s): %s" % (self.table_name, e))

        if self.filters:
            for filter in self.filters:
                query.append_whereclause(filter.build_expression())

        if not query.froms:
            query = query.select_from(table)

        return query

    def __repr__(self):
        return "Querymeta(columns=%s, filters=%s, group_by=%s, table=%s)" % \
               (self.columns, self.filters, self.group_by, self.table_name)


class QueryContext(object):
    def __init__(self, table, filters=[], group_by=[]):
        self.table_name = table
        self.filters = filters
        self.group_by = group_by
        self.query_meta = {}

        if self.filters:
            assert all(isinstance(f, SqlFilter) for f in self.filters)

    def append_column(self, column):
        if isinstance(column, AliasColumn):
            return
        elif isinstance(column, AggregateColumn):
            for c in column.columns:
                self.append_column(c)
            return

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

            tables = [qm.table_name for qm in self.query_meta.values()]
            def table_filter(table_name, metadata):
                return table_name in tables

            self._metadata.reflect(only=table_filter)

        return self._metadata

    def resolve(self, connection, filter_values=None):
        """
        Returns a dict containing the data of the following format:
        * If group_by == [] or None
            return a dict mapping column names to values: {'col_a': 1....}
        * If len(group_by) == 1
            return a dict mapping groupings to that group levels data
            e.g. {
                    'user1': {'user': 'user1', 'col_a': 1...},
                    'user2': {...}
                }
        * If len(group_by) > 1
            return a dict mapping tuple(groupsings) to that group levels data
            e.g. {
                    ('region1', 'subregion1'): {'region': 'region1', 'sub_region': 'subregion1', 'col_a': 1}
                    ('region1', 'subregion2'): {...}
                 }
        """
        self.connection = connection

        data = {}
        for qm in self.query_meta.values():
            result = qm.execute(self.metadata, self.connection, filter_values or {})

            for r in result:
                if not qm.group_by:
                    row_key = None
                elif len(qm.group_by) == 1:
                    row_key = r[qm.group_by[0]]
                elif len(qm.group_by) > 1:
                    row_key = tuple([r[group] for group in qm.group_by])

                if qm.group_by:
                    if row_key is None:
                        # null values coming out of the database wreak havoc elsewhere in the code
                        row_key = ''
                    row = data.setdefault(row_key, {})
                    row.update(kvp for kvp in r.items())
                else:
                    data.update(kvp for kvp in r.items())

        return data

    def __str__(self):
        return str(self.query_meta)


class SqlAggColumn(object):
    @property
    def name(self):
        raise NotImplementedError()

    @property
    def column_key(self):
        raise NotImplementedError()

    def get_value(self, row):
        raise NotImplementedError()


class QueryColumn(SqlAggColumn):
    def get_query_meta(self):
        raise NotImplementedError()


class BaseColumn(SqlAggColumn):
    aggregate_fn = None

    def __init__(self, key, alias=None, table_name=None, filters=None, group_by=None):
        self.key = key
        self.alias = alias
        self.table_name = table_name
        self.filters = filters
        self.group_by = group_by

        if self.filters:
            assert all(isinstance(f, SqlFilter) for f in self.filters)

        #TODO: allow 'having' e.g. count(x) having x > 4

    @property
    def name(self):
        return self.alias or self.key

    @property
    def column_key(self):
        return self.table_name, str(self.filters), str(self.group_by)

    @property
    def sql_column(self):
        return SimpleSqlColumn(self.key, self.aggregate_fn, self.alias)

    def get_value(self, row):
        row_key = self.alias or self.key
        return row.get(row_key, None) if row else None


class CustomQueryColumn(BaseColumn, QueryColumn):
    query_cls = None
    name = None

    def get_query_meta(self, default_table_name, default_filters, default_group_by):
        table_name = self.table_name or default_table_name
        filters = self.filters or default_filters
        group_by = self.group_by or default_group_by
        return self.query_cls(table_name, filters, group_by)

    @property
    def column_key(self):
        return self.name, self.key, self.table_name, str(self.filters), str(self.group_by)


class AliasColumn(SqlAggColumn):
    """
    An AliasColumn doesn't contribute to the query. It is used to reference the value of an existing column.
    e.g. In an AggregateColumn
    """
    column_key = None

    def __init__(self, key):
        self.key = key

    @property
    def name(self):
        return self.key

    def get_value(self, row):
        return row.get(self.key, None) if row else None


class AggregateColumn(SqlAggColumn):
    def __init__(self, aggregate_fn, *columns):
        self.aggregate_fn = aggregate_fn
        self.columns = columns

    @property
    def name(self):
        return '_'.join([c.name for c in self.columns])

    def get_value(self, row):
        values = [v.get_value(row) for v in self.columns]
        return self.aggregate_fn(*values)
