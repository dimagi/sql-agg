import sqlalchemy
from sqlalchemy import Table, Column, String, DATE, INT
from sqlalchemy.orm import mapper

metadata = sqlalchemy.MetaData()

user_table = Table("user_table",
                   metadata,
                   Column("user", String(50), primary_key=True, autoincrement=False),
                   Column("date", DATE, primary_key=True, autoincrement=False),
                   Column("indicator_a", INT),
                   Column("indicator_b", INT),
                   Column("indicator_c", INT),
                   Column("indicator_d", INT))


class UserTable(object):
    pass


region_table = Table("region_table",
                     metadata,
                     Column("region", String(50), primary_key=True, autoincrement=False),
                     Column("sub_region", String(50), primary_key=True, autoincrement=False),
                     Column("date", DATE, primary_key=True, autoincrement=False),
                     Column("indicator_a", INT),
                     Column("indicator_b", INT))


class RegionTable(object):
    pass


mapper(UserTable, user_table)
mapper(RegionTable, region_table)
