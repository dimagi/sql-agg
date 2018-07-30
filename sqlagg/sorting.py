from __future__ import absolute_import
from __future__ import unicode_literals
from sqlalchemy import desc, asc


class OrderBy(object):
    def __init__(self, column_name, is_ascending=True):
        self.column_name = column_name
        self.is_ascending = is_ascending

    def build_expression(self):
        if self.is_ascending:
            return asc(self.column_name)
        return desc(self.column_name)

    def __str__(self):
        return (
            'OrderBy(column_name=%s, is_ascending=%s)'
            % (self.column_name, str(self.is_ascending))
        )
