try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='sqlagg',
    version='0.1.4',
    description='SQL aggregation tool',
    author='Dimagi',
    author_email='dev@dimagi.com',
    url='http://github.com/dimagi/sql-agg',
    packages=['sqlagg', 'sqlagg.queries'],
    license='MIT',
    install_requires=[
        'SQLAlchemy>=0.8.0',
    ],
    tests_require=[
        'unittest2',
        'nose',
        'SQLAlchemy-Fixtures>=0.1.5',
        'fixture>=1.4',
        'psycopg2'
    ]
)
