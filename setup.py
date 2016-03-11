try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='sqlagg',
    version='0.6.0',
    description='SQL aggregation tool',
    author='Dimagi',
    author_email='dev@dimagi.com',
    url='http://github.com/dimagi/sql-agg',
    packages=['sqlagg', 'sqlagg.queries'],
    license='MIT',
    install_requires=[
        'SQLAlchemy>=1.0.9',
    ],
    tests_require=[
        'unittest2==0.5.1',
        'nose',
        'SQLAlchemy-Fixtures>=0.1.5',
        'fixture>=1.4',
        'psycopg2'
    ],
    setup_requires=['nose']
)
