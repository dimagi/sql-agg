class SqlFilter(object):
    def build_expression(self):
        raise NotImplementedError()

    def __str__(self):
        return 'SqlFilter(%s)' % self.build_expression()


class RawFilter(SqlFilter):
    def __init__(self, expression):
        self.expression = expression

    def build_expression(self):
        return self.expression


class BasicFilter(SqlFilter):
    operator = None

    def __init__(self, column_name, parameter):
        self.column_name = column_name
        self.parameter = parameter

    def build_expression(self):
        if not self.operator:
            raise 'Operator missing'

        return '"%s" %s :%s' % (self.column_name, self.operator, self.parameter)


class BetweenFilter(SqlFilter):
    def __init__(self, column_name, lower_param, upper_param):
        self.column_name = column_name
        self.lower_param = lower_param
        self.upper_param = upper_param

    def build_expression(self):
        return '"%s" between :%s and :%s' % (self.column_name, self.lower_param, self.upper_param)


class GTFilter(BasicFilter):
    operator = '>'


class GTEFilter(BasicFilter):
    operator = '>='


class LTFilter(BasicFilter):
    operator = '<'


class LTEFilter(BasicFilter):
    operator = '<='


class EQFilter(BasicFilter):
    operator = '='


class NOTEQFilter(BasicFilter):
    operator = '!='


class INFilter(BasicFilter):
    """
    This filter requires that the parameter value be a tuple.
    """
    operator = 'in'


class ISNULLFilter(SqlFilter):
    def __init__(self, column_name):
        self.column_name = column_name

    def build_expression(self):
        return '"%s" IS NULL' % self.column_name


class NOTNULLFilter(SqlFilter):
    def __init__(self, column_name):
        self.column_name = column_name

    def build_expression(self):
        return '"%s" IS NOT NULL' % self.column_name


class NOTFilter(SqlFilter):
    def __init__(self, filter):
        self.filter = filter

    def build_expression(self):
        return 'NOT %s' % self.filter.build_expression()


class ANDFilter(SqlFilter):
    """
    Lets you construct AND operations on filters.
    """
    def __init__(self, filters):
        self.filters = filters
        assert len(self.filters) > 1

    def build_expression(self):
        expressions = [f.build_expression() for f in self.filters]
        return '(%s)' % ' AND '.join(expressions)


class ORFilter(SqlFilter):
    """
    Lets you construct OR operations on filters.
    """
    def __init__(self, filters):
        self.filters = filters
        assert len(self.filters) > 1

    def build_expression(self):
        expressions = [f.build_expression() for f in self.filters]
        return '(%s)' % ' OR '.join(expressions)


RAW = RawFilter
BETWEEN = BetweenFilter
GT = GTFilter
GTE = GTEFilter
LT = LTFilter
LTE = LTEFilter
EQ = EQFilter
NOTEQ = NOTEQFilter
IN = INFilter
ISNULL = ISNULLFilter
NOTNULL = NOTNULLFilter
NOT = NOTFilter
AND = ANDFilter
OR = ORFilter