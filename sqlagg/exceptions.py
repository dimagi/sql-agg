class SqlAggException(Exception):
    pass


class ColumnNotFoundException(SqlAggException):
    pass


class DuplicateColumnsException(SqlAggException):
    pass
