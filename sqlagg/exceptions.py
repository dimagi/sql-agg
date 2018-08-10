class SqlAggException(Exception):
    pass


class TableNotFoundException(SqlAggException):
    pass


class ColumnNotFoundException(SqlAggException):
    pass


class DuplicateColumnsException(SqlAggException):
    pass
