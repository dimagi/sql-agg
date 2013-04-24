from sqlalchemy.sql.expression import Executable, ClauseElement
from sqlalchemy.ext.compiler import compiles


class InsertFromSelect(Executable, ClauseElement):
    def __init__(self, table, select, columns=None):
        self.table = table
        self.select = select
        self.columns = ("(%s)" % ",".join(columns)) if columns else ""


@compiles(InsertFromSelect)
def visit_insert_from_select(element, compiler, **kw):
    return "INSERT INTO %s %s (%s)" % (
        compiler.process(element.table, asfrom=True),
        element.columns,
        compiler.process(element.select)
    )


@compiles(InsertFromSelect, 'sqlite')
def visit_insert_from_select(element, compiler, **kw):
    return "INSERT INTO %s %s %s" % (
        compiler.process(element.table, asfrom=True),
        element.columns,
        compiler.process(element.select)
    )
