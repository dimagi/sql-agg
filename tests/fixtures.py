from __future__ import absolute_import
from __future__ import unicode_literals
from fixture import DataSet
import datetime


class UserData(DataSet):
    class r1:
        user = "user1"
        date = datetime.date(2013, 1, 1)
        indicator_a = 1
        indicator_b = 1
        indicator_c = 1
        indicator_d = 0

    class r2:
        user = "user1"
        date = datetime.date(2013, 2, 1)
        indicator_a = 3
        indicator_b = 0

    class r3:
        user = "user2"
        date = datetime.date(2013, 1, 1)
        indicator_a = 0
        indicator_b = 3
        indicator_c = 2
        indicator_d = 0

    class r4:
        user = "user2"
        date = datetime.date(2013, 3, 1)
        indicator_a = 2
        indicator_b = 1



class RegionData(DataSet):
    class r1:
        region = "region1"
        sub_region = "region1_a"
        date = datetime.date(2013, 3, 1)
        indicator_a = 1
        indicator_b = 0

    class r2:
        region = "region1"
        sub_region = "region1_a"
        date = datetime.date(2013, 2, 1)
        indicator_a = 0
        indicator_b = 1

    class r3:
        region = "region1"
        sub_region = "region1_b"
        date = datetime.date(2013, 1, 1)
        indicator_a = 3
        indicator_b = 1

    class r4:
        region = "region1"
        sub_region = "region1_b"
        date = datetime.date(2013, 3, 1)
        indicator_a = 1
        indicator_b = 1

    class r5:
        region = "region2"
        sub_region = "region2_a"
        date = datetime.date(2013, 1, 1)
        indicator_a = 2
        indicator_b = 1
