sql-agg
=======

Assuming you have the following database table:

| user  |    date    | indicator_a | indicator_b |
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
i_a = SumView("indicator_a")
i_b = CountView("indicator_b")

# initialise the view context and add the views to it
vc = ViewContext("table_name",
    filters=["date > 'startdate'", "date < 'enddate'"],
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
        "indicator_a": 3,
        "indicator_b": 2
    },
    "user2": {
        "indicator_a": 0,
        "indicator_b": 1
    }
}

Multi-level grouping can be done by adding multiple SimpleView's to the ViewContext as well as multiple column names in
the 'group_by' parameter of the ViewContext.

```python
region = SimpleView("region")
sub_region = SimpleView("sub_region")
indicator_a = SumView("indicator_a")

vc = ViewContext("table_name"
    filters=None,
    group_by=["region","sub_region"])
```

The resultant data would look as follows:
```python
{
    "region1": {
        "sub_region1": {
            "indicator_a": 1
        },
        "sub_region2": {
            "indicator_a": 3
        }
    },
    "region2": {
        "sub_region3": {
            "indicator_a": 2
        }
    }
}
```