class SqlAggException(Exception):
    pass


class TableNotFoundException(Exception):
    pass


class ColumnNotFoundException(Exception):
    pass


class ColumnWithNameNotFoundException(SqlAggException):
    pass
