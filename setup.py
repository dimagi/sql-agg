from __future__ import absolute_import
from __future__ import unicode_literals
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

tests_require=[
    'nose',
    'SQLAlchemy-Fixtures>=0.1.5',
    'fixture>=1.4',
    'psycopg2'
]


setup(
    name='sqlagg',
    version='0.16.0',
    description='SQL aggregation tool',
    author='Dimagi',
    author_email='dev@dimagi.com',
    url='http://github.com/dimagi/sql-agg',
    packages=['sqlagg'],
    license='MIT',
    install_requires=[
        'SQLAlchemy>=1.0.9',
    ],
    tests_require=tests_require,
    setup_requires=['nose'],
    extras_require={
        'test': tests_require,
    },
    classifiers=[
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ]
)
