"""
The test database must be created with something like

    psql -h localhost postgres postgres -c 'CREATE DATABASE sqlagg_test;'
"""
from __future__ import absolute_import
from __future__ import unicode_literals

import os
from unittest import TestCase

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from unmagic import fixture

from .models import metadata, user_table, region_table
from .fixtures import region_data, user_data

SQLAGG_TEST_CONNECTION_STRING = os.environ.get('SQLAGG_TEST_CONNECTION_STRING',
                                               'postgresql://postgres@localhost/sqlagg_test')

Session = sessionmaker()


@fixture(scope='session')
def setup_db():
    engine = create_engine(SQLAGG_TEST_CONNECTION_STRING)
    cn = engine.connect()
    tx = cn.begin()
    metadata.bind = cn
    metadata.create_all()
    cn.execute(text('CREATE VIEW "user_view" as SELECT * from user_table'))
    _insert_rows(user_table, user_data, cn)
    _insert_rows(region_table, region_data, cn)
    yield cn
    tx.rollback()
    cn.close()


class DataTestCase(TestCase):

    def setUp(self):
        cn = setup_db()
        tx = cn.begin_nested()
        self.addCleanup(tx.rollback)
        self.session = Session(bind=cn)


def _insert_rows(table, rows, connection):
    for values in rows:
        connection.execute(table.insert().values(values))
