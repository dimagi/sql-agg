from sqlalchemy import func, distinct
from queries import MedianQueryMeta
from .base import BaseColumn, CustomQueryColumn


class SimpleColumn(BaseColumn):
    pass


class SumColumn(BaseColumn):
    aggregate_fn = func.sum


class CountColumn(BaseColumn):
    aggregate_fn = func.count


class MaxColumn(BaseColumn):
    aggregate_fn = func.max


class MinColumn(BaseColumn):
    aggregate_fn = func.min


class MeanColumn(BaseColumn):
    aggregate_fn = func.avg


class UniqueColumn(BaseColumn):
    aggregate_fn = lambda view, column: func.count(distinct(column))


class MedianColumn(CustomQueryColumn):
    query_cls = MedianQueryMeta
    name = "median"