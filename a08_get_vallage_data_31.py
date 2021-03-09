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
    args = parser.parse_args()

    date_choice = args.dateChoice
    date_start = str(int(date_choice) - 10)
    date_end = str(int(date_choice) - 1)

    data_type = 'GHI'
    task_choice = 'yearAnomaly'

    village_info = Village.get_village_info_by_file(POOR_XLSX)
    lons = village_info['lon'].to_numpy()
    lats = village_info['lat'].to_numpy()
    datas, _, _ = num_point(dataType=data_type,
                            taskChoice=task_choice,
                            dateStart=date_start,
                            dateEnd=date_end,
                            dateChoice=date_choice,
                            left_longitude=lons,
                            left_latitude=lats,
                            out_fi=0)
    print(datas)
    x_y = dict()
    if date_choice:
        datas = {date_choice: datas[date_choice]}
    for time, data in datas.items():
        x_y[time] = dict()
        village_info[data_type] = datas[date_choice]
        values = village_info.groupby('sheng')[data_type].agg('mean')
        values = values.sort_values()
        x = pd.Series(values.index)
        x[x == '广西壮族自治区'] = '广西'
        x[x == '宁夏回族自治区'] = '宁夏'
        x[x == '西藏自治区'] = '西藏'
        x[x == '内蒙古自治区'] = '内蒙古'
        x[x == '新疆维吾尔自治区'] = '新疆'
        x[x == '黑龙江省'] = '黑龙江'
        x_y[time]['x'] = x.to_numpy()
        x_y[time]['y'] = values.values

        sheng = x.to_numpy()
        value = values.to_numpy()

        idx_max = np.argmax(value)
        v31_high_count = len(np.where(value > 0)[0])
        v31_high_province = sheng[idx_max]
        v31_high_max = round(value[idx_max], 1)

        idx_min = np.argmin(value)
        v31_low_count = len(np.where(value < 0)[0])
        v31_low_province = sheng[idx_min]
        v31_low_min = round(value[idx_min], 1)

        result = {
            'provincehighcount': v31_high_count,
            'highprovince': v31_high_province,
            'provincehighmax': v31_high_max,

            'provincelowcount': v31_low_count,
            'lowprovince': v31_low_province,
            'provincelowmin': v31_low_min,
        }

        print('finish{}'.format(json.dumps(result)))
