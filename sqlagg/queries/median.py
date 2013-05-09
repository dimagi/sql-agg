import time
import logging
from sqlalchemy import select, Table, Column, INT, and_, func, alias
from sqlagg import QueryMeta
from .alchemy_extensions import InsertFromSelect, func_ext

logger = logging.getLogger("sqlagg")


class MedianQueryMeta(QueryMeta):
    """
    Custom query for calculating the median over a group.

    See http://dev.mysql.com/doc/refman/5.0/en/group-by-functions.html comment by Paul Harris
    See
        http://mysql-udf.sourceforge.net/
        http://stackoverflow.com/questions/996922/how-can-i-write-my-own-aggregate-functions-with-sqlalchemy
    for other possible options.

    Strategy:
        Use temporary tables to sort the values for each grouping. Also add an index column to number the values.
        Create a second temporary table and populated it with the average index for each group (rounded up).

        Join the tables in a select to get the median for each group.

    e.g.
        CREATE TABLE user_table (
            user_name VARCHAR(50),
            indicator_d INT
        )

        INSERT INTO user_table VALUES ('user1', 1), ('user1', 2), ('user1', 3);
        INSERT INTO user_table VALUES ('user2', 1), ('user2', 2), ('user2', 3), ('user2', 4);

        CREATE TEMP TABLE temp_median (id serial PRIMARY KEY, user_name VARCHAR(50), value INT);
        INSERT INTO temp_median (user_name, value) (
            SELECT t.user_name, indicator_d FROM user_table t ORDER BY t.user_name, indicator_d
        );

        CREATE TEMPORARY TABLE temp_median_ids (upper INT, lower INT);
        INSERT INTO temp_median_ids (upper, lower) (
            SELECT CEIL(AVG(id)) AS upper, FLOOR(AVG(id)) as lower FROM temp_median GROUP BY user_name
        );

        SELECT tu.user_name, (tu.value + tl.value) / 2.0 as value
        FROM temp_median_ids
        LEFT JOIN temp_median tu ON tu.id = temp_median_ids.upper
        LEFT JOIN temp_median tl ON tl.id = temp_median_ids.lower;

        -- user1: 2
        -- user2: 2.5
    """
    ID_COL = "id"
    VAL_COL = "value"

    def append_column(self, column):
        self.key = column.key
        self.alias = column.alias or self.key

    def execute(self, metadata, connection, filter_values):
        median_table = self._build_median_table(metadata)
        self._populate_median_table(median_table, metadata, connection, filter_values)

        median_id_table = self._build_median_id_table(metadata)
        self._populate_median_id_table(median_table, median_id_table, connection)

        median_query = self._build_median_query(median_id_table, median_table)

        result = connection.execute(median_query).fetchall()
        return result

    def _get_table_name(self, prefix):
        return "%s_%s_%s_%s" % (prefix, self.table_name, self.key, str(time.time()).replace(".", "_"))

    def _build_median_table(self, metadata):
        """
        CREATE TEMP TABLE temp_median (id serial PRIMARY KEY, user_name VARCHAR(50), value INT);
        """
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

        logger.debug("Building median table: %s", table_name)
        median_table.create()

        return median_table

    def _populate_median_table(self, median_table, metadata, connection, filter_values):
        """
        INSERT INTO temp_median (user_name, value) (
            SELECT t.user_name, indicator_d FROM user_table t ORDER BY t.user_name, indicator_d
        );
        """
        origin_table = metadata.tables[self.table_name]
        origin_column = origin_table.c[self.key]

        query = select([origin_column], and_(*self.filters))
        for group in self.group_by:
            column = origin_table.c[group]
            query.append_column(column)
            query.append_order_by(column)

        query.append_order_by(origin_table.c[self.key])

        # TODO: better way of escaping names
        columns = ["value"] + self.group_by
        for i, c in enumerate(columns):
            columns[i] = '"%s"' % c

        from_select = InsertFromSelect(median_table, query, columns)

        logger.debug("Populate median table")
        connection.execute(from_select, **filter_values)

    def _build_median_id_table(self, metadata):
        """
        CREATE TEMPORARY TABLE temp_median_ids (upper INT, lower INT);
        """
        table_name = self._get_table_name("median_id")
        median_id_table = Table(table_name, metadata,
                                Column("upper", INT),
                                Column("lower", INT),
                                prefixes=["TEMPORARY"])

        logger.debug("Building median ID table: %s", table_name)
        median_id_table.create()
        return median_id_table

    def _populate_median_id_table(self, median_table, median_id_table, connection):
        """
        INSERT INTO temp_median_ids (upper, lower) (
            SELECT CEIL(AVG(id)) AS upper, FLOOR(AVG(id)) as lower FROM temp_median GROUP BY user_name
        );
        """
        func_avg = func.avg(median_table.c[self.ID_COL])
        query = select([func_ext.ceil(func_avg).label("upper"), func_ext.floor(func_avg).label("lower")],
                       from_obj=median_table)
        for group in self.group_by:
            query.append_group_by(median_table.c[group])
        from_select = InsertFromSelect(median_id_table, query, ["upper", "lower"])

        logger.debug("Populate median ID table")
        connection.execute(from_select)

    def _build_median_query(self, median_id_table, median_table):
        """
        SELECT tu.user_name, (tu.value + tl.value) / 2.0 as value
        FROM temp_median_ids
        LEFT JOIN temp_median tu ON tu.id = temp_median_ids.upper
        LEFT JOIN temp_median tl ON tl.id = temp_median_ids.lower;
        """
        t_upper = alias(median_table, name="tup")
        t_lower = alias(median_table, name="tlo")

        final_query = select(from_obj=median_id_table)
        for group in self.group_by:
            final_query.append_column(t_upper.c[group])

        final_query.append_column(((t_upper.c[self.VAL_COL] + t_lower.c[self.VAL_COL]) / 2.0).label(self.alias))
        final_query.append_whereclause(median_id_table.c["upper"] == t_upper.c[self.ID_COL])
        final_query.append_whereclause(median_id_table.c["lower"] == t_lower.c[self.ID_COL])
        return final_query
