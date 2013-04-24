import time
from sqlalchemy import select, Table, Column, INT, and_, func
from sqlagg import QueryMeta
from .alchemy_extensions import InsertFromSelect


class MedianQueryMeta(QueryMeta):
    """
    Custom query for calculating the median over a group.

    See http://dev.mysql.com/doc/refman/5.0/en/group-by-functions.html comment by Paul Harris
    See http://mysql-udf.sourceforge.net/ for another possible option

    Strategy:
        Use temporary tables to sort the values for each grouping. Also add an index column to number the values.
        Create a second temporary table and populated it with the average index for each group (rounded up).

        Join the tables in a select to get the median for each group.
        Note that this method selects the upper median value for sets of even size i.e. median(0,1,2,3) = 2
    """
    ID_COL = "id"
    VAL_COL = "value"

    def append_view(self, view):
        self.key = view.key
        self.as_name = view.as_name or self.key

    def execute(self, metadata, connection, filter_values):
        median_table = self._build_median_table(metadata)
        self._populate_median_table(median_table, metadata, connection, filter_values)

        median_id_table = self._build_median_id_table(metadata)
        self._populate_median_id_table(median_table, median_id_table, connection)

        median_query = self._build_median_query(median_id_table, median_table)

        result = connection.execute(median_query).fetchall()
        return result

        """select * from temp_median

        create temp table temp_median (id serial PRIMARY KEY, user_name VARCHAR(50), value INT)
        insert into temp_median (user_name, value) (SELECT t.user, indicator_d FROM user_table t ORDER BY t.user, indicator_d)

        CREATE TEMPORARY TABLE temp_median_ids AS SELECT ROUND(AVG(id)) AS id FROM temp_median GROUP BY user_name;
        select * from temp_median_ids
        SELECT user_name, value FROM temp_median_ids LEFT JOIN temp_median USING (id);"""

    def _get_table_name(self, prefix):
        return "%s_%s_%s_%s" % (prefix, self.table_name, self.key, str(time.time()).replace(".", "_"))

    def _build_median_table(self, metadata):
        origin_table = metadata.tables[self.table_name]
        origin_column = origin_table.c[self.key]

        table_name = self._get_table_name("median")
        median_table = Table(table_name, metadata,
                             Column("id", INT, primary_key=True),
                             Column("value", origin_column.type),
                             prefixes=['TEMPORARY'])
        for group in self.group_by:
            column = origin_table.c[group]
            median_table.append_column(Column(group, column.type))

        median_table.create()

        return median_table

    def _populate_median_table(self, median_table, metadata, connection, filter_values):
        origin_table = metadata.tables[self.table_name]
        origin_column = origin_table.c[self.key]

        query = select([origin_column], and_(*self.filters))
        for group in self.group_by:
            column = origin_table.c[group]
            query.append_column(column)
            query.append_order_by(column)

        query.append_order_by(origin_table.c[self.key])
        from_select = InsertFromSelect(median_table, query, ["value"] + self.group_by)
        connection.execute(from_select, **filter_values)

    def _build_median_id_table(self, metadata):
        table_name = self._get_table_name("median_id")
        median_id_table = Table(table_name, metadata,
                                Column(self.ID_COL, INT, primary_key=True),
                                prefixes=["TEMPORARY"])

        median_id_table.create()
        return median_id_table

    def _populate_median_id_table(self, median_table, median_id_table, connection):
        query = select([func.round(func.avg(median_table.c[self.ID_COL]))])
        for group in self.group_by:
            query.append_group_by(median_table.c[group])
        connection.execute(InsertFromSelect(median_id_table, query, [self.ID_COL]))

    def _build_median_query(self, median_id_table, median_table):
        final_query = select(from_obj=median_id_table)
        for group in self.group_by:
            final_query.append_column(median_table.c[group])

        final_query.append_column(median_table.c[self.VAL_COL].label(self.as_name))
        final_query.append_whereclause(median_id_table.c[self.ID_COL] == median_table.c[self.ID_COL])
        return final_query
