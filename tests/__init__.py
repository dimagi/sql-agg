from __future__ import absolute_import
from __future__ import unicode_literals
from fixture import DataTestCase, SQLAlchemyFixture
from sqlalchemy import create_engine, text

from .models import metadata, UserTable, RegionTable
from .fixtures import RegionData, UserData

# import logging
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

engine = create_engine('postgresql://postgres:@localhost/sqlagg_test')
metadata.bind = engine
metadata.create_all()

engine.execute(text('CREATE OR REPLACE VIEW "user_view" as SELECT * from user_table'))


class BaseTest(DataTestCase):
    datasets = [UserData, RegionData]
    fixture = SQLAlchemyFixture(
        engine=metadata.bind,
        env={"UserData": UserTable, "RegionData": RegionTable})

    @classmethod
    def metadata(cls):
        return metadata
