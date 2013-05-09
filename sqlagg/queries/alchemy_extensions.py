from sqlalchemy.sql.expression import Executable, ClauseElement, ColumnElement, FunctionElement
from sqlalchemy.ext.compiler import compiles
from sqlalchemy import INT


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


class func_ext(object):
    class ceil(ColumnElement):
        """
        Implement ceil() function as a custom function to allow use in sqlite for testing.
        """
        type = INT()

        def __init__(self, clauses):
            self.clauses = clauses

    @compiles(ceil)
    def _compile_ceil(element, compiler, **kw):
        return "ceil(%s)" % compiler.process(element.clauses)

    @compiles(ceil, 'sqlite')
    def _compile_ceil_sqlite(element, compiler, **kw):
        return """(case when {0} = cast({0} as int) then cast({0} as int)
                 else (1 + cast({0} as int))
            end)""".format(compiler.process(element.clauses))

    class floor(ColumnElement):
        """
        Implement floor() function as a custom function to allow use in sqlite for testing.
        """
        type = INT()

        def __init__(self, clauses):
            self.clauses = clauses

    @compiles(floor)
    def _compile_floor(element, compiler, **kw):
        return "floor(%s)" % compiler.process(element.clauses)

    @compiles(floor, 'sqlite')
    def _compile_floor_sqlite(element, compiler, **kw):
        return "cast(%s as int)" % compiler.process(element.clauses)
