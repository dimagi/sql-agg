try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='sqlagg',
    version='0.1.0',
    description='SQL aggregation tool',
    author='Simon Kelly',
    author_email='skelly@dimagi.com',
    url='http://github.com/dimagi/sql-agg',
    packages=['sqlagg'],
    license='MIT',
    install_requires=[
        'SQLAlchemy>=0.8.0',
        'SQLAlchemy-Fixtures>=0.1.5',
        'fixture>=1.4'
    ]
)