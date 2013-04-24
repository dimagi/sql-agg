# sql-agg

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
from sqlagg.views import *

# create the views
user = SimpleView("user")
i_a = SumView("column_a")
i_b = CountView("column_b")

# initialise the view context and add the views to it
vc = ViewContext("table_name",
    filters=["date > :startdate", "date < :enddate"],
    group_by=["user"])
vc.append_view(yuser)
vc.append_view(i_a)
vc.append_view(i_b)

filter_values={
    "startdate": date(2012, 01, 01),
    "enddate": date(2012, 03, 01)
    }

# resolve the view context with the filter values (connection is an SQLAlchemy connection)
vc.resolve(connection, filter_values=filter_values)
data = vc.data
```

The resultant `data` variable will be a dictionary as follows:
```python
{
    "user1": {
        "column_a": 3,
        "column_b": 2
    },
    "user2": {
        "column_a": 0,
        "column_b": 1
    }
}

## Multi-level grouping
Multi-level grouping can be done by adding multiple SimpleView's to the ViewContext as well as multiple column names in
the 'group_by' parameter of the ViewContext.

```python
region = SimpleView("region")
sub_region = SimpleView("sub_region")
column_a = SumView("column_a")

vc = ViewContext("table_name"
    filters=None,
    group_by=["region","sub_region"])
```

The resultant data would look as follows:
```python
{
    "region1": {
        "sub_region1": {
            "column_a": 1
        },
        "sub_region2": {
            "column_a": 3
        }
    },
    "region2": {
        "sub_region3": {
            "column_a": 2
        }
    }
}
```

## Views in detail
For each view you can specify the `table`, `filters` and also `group_by` fields. Using these features you can supply
different filters per column or select data from different columns.

### Different filters
```python
view_a = SumView("column_a")
view_b = SumView("column_b", filters="date < '{enddate}'")
```

In this case `view_a` will get the filters supplied to the `ViewContext` while `view_b` will be resolved with its own
filters. This will result in two queries being run on the database.

## Different tables
It is possible to select data from different tables by providing views with different `table_name`s.

```python
view_a = SumView("column_a")
view_b = SumView("column_b", table_name="table_b", group_by=["user"]
```

Here `view_a` will be selected from the table configured in the ViewContext while `view_b` will be selected from
*table_name* and will be grouped by *user*. This will result in two queries being run on the database.

## As Name
It is possible to use the same column in multiple views by specifying the `as_name` argument of the view.

```python
sum_a = SumView("column_a", as_name="sum_a")
count_a = CountView("column_a", as_name="count_a")
```

The resulting data will use the `as_name` keys to reference the values.



