#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020-12-24 14:09:49
# @Author  : Yan Kaijie
import argparse
import csv
from collections import defaultdict

import numpy as np

from utils.database import *
from utils.model import Station


def get_data_by_db(out_file):
    year_choice = 2020
    session = Session()
    station = Station()
    data_all = station.query(session)
    year_list = []
    for year in range(year_choice - 30, year_choice):
        year_list.append(str(year))
    sta_gti_30_dic = defaultdict(list)
    for da_m in data_all:
        if da_m.year in year_list:
            sta_gti_30_dic[da_m.station].append(da_m.GTI)
    headers = ['station', 'H0', 'H20', 'H25']
    rows = []
    for sta, gti_list in sta_gti_30_dic.items():
        np_qti = np.array(gti_list)
        gti_mean = np_qti.mean() * 12
        row = [sta, gti_mean * 0.8, gti_mean * 0.8 * 0.92, gti_mean * 0.8 * 0.90]
        rows.append(row)
    with open(out_file, 'w')as f:
        f_csv = csv.writer(f)
        f_csv.writerow(headers)
        f_csv.writerows(rows)
    print('finish')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GFSSI Schedule')
    parser.add_argument('--out_file', '-o', help='指定文件路径及文件名，YYYY(2019)', required=True)
    args = parser.parse_args()
    get_data_by_db(args.out_file)
# python3 a07_get_h0_h20_h25_by_db.py -o /home/kts_project_v1/gffp/test.txt
