#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2021-01-22 14:22
# @Author  : NingAnMe <ninganme@qq.com>
"""
从各省贫困村平均的太阳能资源距平来看（图16），
____个省份2019年太阳能资源偏高，
其中__省偏高幅度最大，为__%，
其他__个省份2019年太阳能资源均偏低，
其中__省偏低幅度最大，为__%。
"""
import argparse
import numpy as np
import pandas as pd
import json

from utils.model import Village
from utils.config import POOR_XLSX
from a04_data_statistics import num_point


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Schedule')
    # parser.add_argument('--dateStart', '-s', help='开始年份，YYYY(2019)', required=True)
    # parser.add_argument('--dateEnd', '-e', help='结束年份，YYYY(2019)', required=True)
    parser.add_argument('--dateChoice', '-o', help='距平年份，YYYY(2019)', required=True)
    parser.add_argument('--leftLongitude', '-l', help='经度或左上角经度，47.302235', required=True)
    parser.add_argument('--leftLatitude', '-a', help='纬度或左上角纬度，85.880519', required=True)
    args = parser.parse_args()

    date_choice = args.dateChoice
    date_start = str(int(date_choice) - 10)
    date_end = str(int(date_choice) - 1)

    longitude = float(args.leftLongitude)
    latitude = float(args.leftLatitude)

    data_type = 'GHI'
    task_choice = 'monthAnomaly'

    lons = [longitude]
    lats = [latitude]

    datas, _, _ = num_point(dataType=data_type,
                            taskChoice=task_choice,
                            dateStart=date_start,
                            dateEnd=date_end,
                            dateChoice=date_choice,
                            left_longitude=lons,
                            left_latitude=lats,
                            out_fi=0)
    print('datas', datas)
    month_max = None
    month_min = None

    value_max = -np.inf
    value_min = np.inf
    for month, value in datas.items():
        if value > value_max:
            month_max = month
            value_max = value
        if value < value_min:
            month_min = month
            value_min = value

    result = {
        'month_max': month_max,
        'value_max': round(value_max[0], 2),

        'month_min': month_min,
        'value_min': round(value_min[0], 2),
    }

    print('finish{}'.format(json.dumps(result)))
