# sql-agg
[![Build Status](https://travis-ci.org/dimagi/sql-agg.png)](https://travis-ci.org/dimagi/sql-agg)
[![Test coverage](https://coveralls.io/repos/dimagi/sql-agg/badge.png?branch=master)](https://coveralls.io/r/dimagi/sql-agg)

## Basic usage
Assuming you have the following database table:

| user  |    date    | column_a    | column_b    |
|-------|------------|-------------|-------------|
| user1 | 2012-01-01 |      1      |      1      |
| user1 | 2012-01-06 |      2      |      0      |
| user2 | 2012-02-19 |      0      |      3      |

You can use sql-agg to extract aggregated data from the table as follows:

```python
from datetime import date
from sqlagg import *
from sqlagg.columns import *

# create the columns
user = SimpleColumn("user")
i_a = SumColumn("column_a")
i_b = CountColumn("column_b")

# initialise the query context and add the columns to it
vc = QueryContext("table_name",
    filters=[GT('date', 'startdate'), LT('date', 'enddate')],
    group_by=["user"])
vc.append_column(user)
vc.append_column(i_a)
vc.append_column(i_b)

filter_values={
    "startdate": date(2012, 1, 1),
    "enddate": date(2012, 3, 1)
    }

# resolve the query context with the filter values (connection is an SQLAlchemy connection)
vc.resolve(connection, filter_values=filter_values)
data = vc.data
```

The resultant `data` variable will be a dictionary as follows:
```python
{
    "user1": {
        "user": "user1",
        "column_a": 3,
        "column_b": 2
    },
    "user2": {
        "user": "user2"
        "column_a": 0,
        "column_b": 1
    }
}
```

## Multi-level grouping
Multi-level grouping can be done by adding multiple SimpleColumn's to the QueryContext as well as multiple column names in
the 'group_by' parameter of the QueryContext.

```python
region = SimpleColumn("region")
sub_region = SimpleColumn("sub_region")
column_a = SumColumn("column_a")

vc = QueryContext("table_name"
    filters=None,
    group_by=["region","sub_region"])
```

The resultant data would look as follows:
```python
{
    ("region1", "sub_region1"): {
        "region": "region1",
        "sub_region": "sub_region1",
        "column_a": 1
    },
    ("region1", "sub_region2"): {
        "region": "region1",
        "sub_region": "sub_region2",
        "column_a": 3
    },
    ("region2", "sub_region3"): {
        "region": "region2",
        "sub_region": "sub_region3",
        "column_a": 2
    }
}
```

## Columns in detail
For each column you can specify the `table`, `filters` and also `group_by` fields. Using these features you can supply
different filters per column or select data from different columns.

### Different filters
```python
column_a = SumColumn("column_a")
column_b = SumColumn("column_b", filters=[LT('date', 'enddate')])
```

In this case `column_a` will get the filters supplied to the `QueryContext` while `column_b` will be resolved with its own
filters. This will result in two queries being run on the database.

## Different tables
It is possible to select data from different tables by providing columns with different `table_name`s.

```python
column_a = SumColumn("column_a")
column_b = SumColumn("column_b", table_name="table_b", group_by=["user"])
```

Here `column_a` will be selected from the table configured in the QueryContext while `column_b` will be selected from
*table_name* and will be grouped by *user*. This will result in two queries being run on the database.

## As Name
It is possible to use the same column in multiple columns by specifying the `alias` argument of the column.

```python
sum_a = SumColumn("column_a", alias="sum_a")
count_a = CountColumn("column_a", alias="count_a")
```

The resulting data will use the `alias` keys to reference the values.

## Conditional / Case columns
*Simple*
```python
num_wheels = SumWhen("vehicle", whens={"unicycle": 1, "bicycle": 2, "car": 4}, else_=0, alias="num_wheels")
```

*Complex*
```python
num_children = SumWhen(whens={"users.age < 13": 1}, else_=0, alias="children")
```

## Alias and Aggregate columns
Useful if you want to use a column more than once but don't want to re-calculate its value.
```python
sum_a = SumColumn("column_a")

aggregate = AggregateColumn(lambda x, y: x / y,
                            AliasColumn("column_a"),
                            SumColumn("column_b")
```
TODO: custom queries

# Filtering
The `QueryContext` and most column classes accept a `filters` parameter which must be iterable.
Each element of this iterable must be a subclass of `sqlagg.filter.SqlFilter`. The elements of this
parameter are combined using the `AND` operator.

i.e.
`filters = [EQ('user', 'username'), EQ('role', 'admin')]`

is equivalent to:

```
filters = [AND([
    EQ('user', 'username'), EQ('role', 'admin')
])]
```


Any filter expression can be expressed using a RawFilter:

`RawFilter('"user" = :username AND "date" between :start and :end')`

In this case the same filter could be expressed as follows:

`AND([EQ('user', 'username'), BETWEEN('date', 'start', 'end'])`

# Development

To install dependencies run

`pip install .`

## Running Tests

First create an environment variable for the appropriate connection string:

```bash
export SQLAGG_TEST_CONNECTION_STRING='postgresql://user:pass@localhost:5432/sqlagg_test
```

Then run the following

```python
python setup.py test
```

Note: If you face issues with psycopg2 try replacing with `psycopg2-binary` in setup.py
