#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-11-16 15:47
# @Author  : YanKaijie
import os
import argparse
from datetime import datetime, date
from warnings import filterwarnings
import numpy as np
from dateutil.relativedelta import relativedelta
from schedule import *
from utils.config import get_datatype
from utils.get_index_by_lonlat import get_point_index_by_lon_lat, get_area_index_by_lon_lat
from utils.data import DemLoader
from utils.path import *
from utils.hdf5 import *
import pandas as pd
import h5py

'''
1）输入单点经纬度，输出txt格式的数据；
2）输入区域经纬度范围，输出
'''


def ave(sum, len):
    num_np = np.array(sum)
    ave = np.sum(num_np) / len
    return ave


def ave_a(lis, li_len):
    print('开始求平均')
    # print(len(lis[0]))
    # print(len(lis[0][0]))
    sta, end = len(lis[0]), len(lis[0][0])
    sum_a = np.zeros((sta, end))
    # print(sum_a)
    for s in lis:
        sum_a = sum_a + s
    # print(sum_a)
    ave_a = sum_a / li_len
    return ave_a


def juping(oneyear, ave):
    # （一年值 - 多年平均值） / 多年平均值 * 100 %
    jp = (oneyear - ave) / ave * 100
    jl = '{}%'.format(jp)
    return jl


def juping_a(oneyear, ave):
    # （一年值 - 多年平均值） / 多年平均值 * 100 %
    jp = (oneyear - ave) / ave * 100
    jp_li = []
    for j in jp:
        jp_row = []
        for p in j:
            jl = '{}%'.format(p)
            # print(jl)
            jp_row.append(jl)
        jp_li.append(jp_row)
    return jp_li


def num_p_a(dataType, dateStart, dateEnd, leftLongitude, leftLatitude):
    # 选择数据类型
    d_t = get_datatype()
    if dataType in d_t:
        datapath = "{}".format(dataType)
    else:
        return '数据类型错误！'
    print(dataType)
    # 获取数据
    loa = np.array([float(leftLongitude), float(leftLatitude)])
    row, col = get_point_index_by_lon_lat(loa[0], loa[1])
    print(int(row), int(col))
    print(row, col)
    data_list = []
    year_list = []
    for year in np.arange(int(dateStart), int(dateEnd) + 1):
        mon_list = []
        num_year = 0.0
        for mon in np.arange(1, 13):
            if mon < 10:
                path = '{}/{}_{}0{}.hdf'.format(str(datapath), str(dataType), str(year), str(mon))
            else:
                path = '{}/{}_{}{}.hdf'.format(str(datapath), str(dataType), str(year), str(mon))
            print(path)
            dem = DemLoader()
            dem.file_hdf = os.path.join(AID_PATH, 'data/{}'.format(path))
            data = dem.get_data()
            # print(data)
            print(data.shape)
            mon_data = data[int(row)][int(col)]
            mon_list.append(mon_data)
            # 逐年年值
            num_year = num_year + mon_data
            # print(num_year)
        # 逐季度总量（1 - 3、4 - 6、7 - 9、10 - 12月）
        s_mon = 1
        qua_data_list = []
        while s_mon < 11:
            qua = 0.0
            e_mon = s_mon + 3
            for mo in np.arange(s_mon, e_mon):
                qua = mon_list[s_mon - 1] + qua
            qua_data_list.append(qua)
            s_mon = e_mon
        # ---------- 月份数据    年值         季度数据
        year_data = [mon_list, num_year, qua_data_list]
        year_list.append(year_data)
    y_d_num = []
    fir_list = []
    sec_list = []
    thr_list = []
    fou_list = []
    for y_d in year_list:
        y_d_num.append(y_d[1])
        fir_list.append(y_d[2][0])
        sec_list.append(y_d[2][1])
        thr_list.append(y_d[2][2])
        fou_list.append(y_d[2][3])
    # 任意起止年份的平均年值；
    year_ave = ave(y_d_num, len(year_list))
    # 任意起止年份的平均季度值；
    fir_ave = ave(fir_list, len(year_list))
    sec_ave = ave(sec_list, len(year_list))
    thr_ave = ave(thr_list, len(year_list))
    fou_ave = ave(fou_list, len(year_list))
    # 逐季节总量（3 - 5、6 - 8、9 - 11、12 - 2月），
    for i in np.arange(0, len(year_list)):
        s_mon = 3
        sea_data_list = []
        while s_mon < 13:
            sea = 0.0
            e_mon = s_mon + 3
            for mo in np.arange(s_mon, e_mon):
                if s_mon < 10:
                    sea = year_list[i][0][s_mon - 1] + sea
                else:
                    if dateStart + i < dateEnd:
                        sea = year_list[i][0][s_mon - 1] + year_list[i + 1][0][0] + year_list[i + 1][0][1]
                    else:
                        sea = year_list[i][0][s_mon - 1]
            sea_data_list.append(sea)
            s_mon = e_mon
        # 加入每年的季节数据
        year_list[i].append(sea_data_list)
    # 任意起止年份的平均季节值；
    MAM_list = []
    JJA_list = []
    SON_list = []
    DJF_list = []
    for y_d in year_list:
        MAM_list.append(y_d[3][0])
        JJA_list.append(y_d[3][1])
        SON_list.append(y_d[3][2])
        DJF_list.append(y_d[3][3])
    MAM_ave = ave(MAM_list, len(year_list))
    JJA_ave = ave(JJA_list, len(year_list))
    SON_ave = ave(SON_list, len(year_list))
    DJF_ave = ave(DJF_list, len(year_list))
    # 任意起止年份的平均逐月值，如1961 - 1990年平均的1月份的值
    monList = [[], [], [], [], [], [], [], [], [], [], [], []]
    for year_m in year_list:
        for i in np.arange(0, 12):
            monList[i].append(year_m[0][i])
    Jan_ave = ave(monList[0], len(year_list))
    Feb_ave = ave(monList[1], len(year_list))
    Mar_ave = ave(monList[2], len(year_list))
    Apr_ave = ave(monList[3], len(year_list))
    May_ave = ave(monList[4], len(year_list))
    Jun_ave = ave(monList[5], len(year_list))
    Jul_ave = ave(monList[6], len(year_list))
    Aug_ave = ave(monList[7], len(year_list))
    Sep_ave = ave(monList[8], len(year_list))
    Oct_ave = ave(monList[9], len(year_list))
    Nov_ave = ave(monList[10], len(year_list))
    Dec_ave = ave(monList[11], len(year_list))

    # year_data = [mon_list, num_year, qua_data_list， sea_data_list]
    # 任意一年年值 / 季度值 / 季节值 / 月值与任意起止年份相应平均值的距平（距平即相对差值：（一年值 - 多年平均值） / 多年平均值 * 100 %）。

    # year_data = [mon_list, num_year, qua_data_list， sea_data_list]
    for ye in year_list:
        qua_jp_list = []
        sea_jp_list = []
        mon_jp_list = []
        # 年值 距平
        year_jp = juping(ye[1], year_ave)
        # 季度值 距平
        fir_jp = juping(ye[2][0], fir_ave)
        sec_jp = juping(ye[2][1], sec_ave)
        thr_jp = juping(ye[2][2], thr_ave)
        fou_jp = juping(ye[2][3], fou_ave)
        qua_jp_list.append(fir_jp)
        qua_jp_list.append(sec_jp)
        qua_jp_list.append(thr_jp)
        qua_jp_list.append(fou_jp)
        # 季节值 距平
        MAM_jp = juping(ye[3][0], MAM_ave)
        JJA_jp = juping(ye[3][1], JJA_ave)
        SON_jp = juping(ye[3][2], SON_ave)
        DJF_jp = juping(ye[3][3], DJF_ave)
        sea_jp_list.append(MAM_jp)
        sea_jp_list.append(JJA_jp)
        sea_jp_list.append(SON_jp)
        sea_jp_list.append(DJF_jp)
        # 月值 距平
        Jan_jp = juping(ye[0][0], Jan_ave)
        Feb_jp = juping(ye[0][1], Feb_ave)
        Mar_jp = juping(ye[0][2], Mar_ave)
        Apr_jp = juping(ye[0][3], Apr_ave)
        May_jp = juping(ye[0][4], May_ave)
        Jun_jp = juping(ye[0][5], Jun_ave)
        Jul_jp = juping(ye[0][6], Jul_ave)
        Aug_jp = juping(ye[0][7], Aug_ave)
        Sep_jp = juping(ye[0][8], Sep_ave)
        Oct_jp = juping(ye[0][9], Oct_ave)
        Nov_jp = juping(ye[0][10], Nov_ave)
        Dec_jp = juping(ye[0][11], Dec_ave)
        mon_jp_list.append(Jan_jp)
        mon_jp_list.append(Feb_jp)
        mon_jp_list.append(Mar_jp)
        mon_jp_list.append(Apr_jp)
        mon_jp_list.append(May_jp)
        mon_jp_list.append(Jun_jp)
        mon_jp_list.append(Jul_jp)
        mon_jp_list.append(Aug_jp)
        mon_jp_list.append(Sep_jp)
        mon_jp_list.append(Oct_jp)
        mon_jp_list.append(Nov_jp)
        mon_jp_list.append(Dec_jp)
        # 加入每年的数据中
        ye.append(year_jp)
        ye.append(qua_jp_list)
        ye.append(sea_jp_list)
        ye.append(mon_jp_list)

    # data_ave_name = ['年平均值', '一季度平均值', '二季度平均值', '三季度平均值', '四季度平均值', '春季平均值', '夏季平均值', '秋季平均值', '冬季平均值', '1月平均值',
    #                  '2月平均值',
    #                  '3月平均值', '4月平均值', '5月平均值', '6月平均值',
    #                  '7月平均值', '8月平均值', '9月平均值', '10月平均值', '11月平均值', '12月平均值']
    data_ave_name = ['year', 'fir_ave', 'sec_ave', 'thr_ave', 'fou_ave', 'MAM_ave', 'JJA_ave', 'SON_ave', 'DJF_ave',
                     'Jan_ave', 'Feb_ave', 'Mar_ave', 'Apr_ave', 'May_ave', 'Jun_ave',
                     'Jul_ave', 'Aug_ave', 'Sep_ave', 'Oct_ave', 'Nov_ave', 'Dec_ave']

    # data_list.append(data_ave_1)
    data_ave_val = [year_ave, fir_ave, sec_ave, thr_ave, fou_ave, MAM_ave, JJA_ave, SON_ave, DJF_ave, Jan_ave,
                    Feb_ave, Mar_ave, Apr_ave, May_ave, Jun_ave, Jul_ave, Aug_ave, Sep_ave, Oct_ave, Nov_ave, Dec_ave]
    # data_list.append(data_ave_2)
    for i in np.arange(0, len(data_ave_name)):
        data_list.append([data_ave_name[i], data_ave_val[i]])
    # [mon_list, num_year, qua_data_list， sea_data_list,year_jp, qua_jp_list, sea_jp_list, mon_jp_list]
    #    0           1          2             3             4          5           6               7
    # data_row_name = ['年值', '年距平', '一季度值', '一季度距平', '二季度值', '二季度距平', '三季度值', '三季度距平', '四季度值', '四季度距平', '春季值',
    #                  '春季距平', '夏季值',
    #                  '夏季距平', '秋季值',
    #                  '秋季距平', '冬季值', '冬季距平', '1月值', '1月距平', '2月值', '2月距平', '3月值', '3月距平', '4月值', '4月距平', '5月值', '5月距平',
    #                  '6月值', '6月距平',
    #                  '7月值', '7月距平', '8月值', '8月距平', '9月值', '9月距平', '10月值', '10月距平', '11月值', '11月距平', '12月值', '12月距平']
    data_row_name = ['year_data', 'year_dep', 'fir_data', 'fir_dep', 'sec_data', 'sec_dep', 'thr_data', 'thr_dep',
                     'fou_data', 'fou_dep',
                     'MAM_data', 'MAM_dep', 'JJA_data', 'JJA_dep', 'SON_data', 'SON_dep', 'DJF_data', 'DJF_dep',
                     'Jan_data', 'Jan_dep',
                     'Feb_data', 'Feb_dep', 'Mar_data', 'Mar_dep', 'Apr_data', 'Apr_dep', 'May_data', 'May_dep',
                     'Jun_data', 'Jun_dep',
                     'Jul_data', 'Jul_dep', 'Aug_data', 'Aug_dep', 'Sep_data', 'Sep_dep', 'Oct_data', 'Oct_dep',
                     'Nov_data', 'Nov_dep',
                     'Dec_data', 'Dec_dep']

    year = int(dateStart)
    for ye in year_list:
        name_end = []
        for na in data_row_name:
            na = '{}_{}'.format(year, na)
            name_end.append(na)
        data_row_val = [ye[1], ye[4],
                        ye[2][0], ye[5][0], ye[2][1], ye[5][1], ye[2][2], ye[5][2], ye[2][3], ye[5][3],
                        ye[3][0], ye[6][0], ye[3][1], ye[6][1], ye[3][2], ye[6][2], ye[3][3], ye[6][3],
                        ye[0][0], ye[7][0], ye[0][1], ye[7][1], ye[0][2], ye[7][2], ye[0][3], ye[7][3],
                        ye[0][4], ye[7][4], ye[0][5], ye[7][5], ye[0][6], ye[7][6], ye[0][7], ye[7][7],
                        ye[0][8], ye[7][8], ye[0][9], ye[7][9], ye[0][10], ye[7][10], ye[0][11], ye[7][11]
                        ]
        for i in np.arange(0, len(data_row_name)):
            data_list.append([name_end[i], data_row_val[i]])
        year += 1
        print(data_list)
    return data_list


def num_a(dataType, dateStart, dateEnd, leftLongitude, leftLatitude,
          rightLongitude, rightLatitude, ):
    # 选择数据类型
    d_t = get_datatype()
    if dataType in d_t:
        datapath = "{}".format(dataType)
    else:
        return '数据类型错误！'
    print(dataType)
    loa = np.array([float(leftLongitude), float(leftLatitude), float(rightLongitude), float(rightLatitude)])
    # row_min, row_max, col_min, col_max = get_area_index_by_lon_lat(loa[0], loa[1], loa[2], loa[3])  # !!！
    row_min, col_min = get_point_index_by_lon_lat(loa[0], loa[1])
    row_max, col_max = get_point_index_by_lon_lat(loa[2], loa[3])
    print('行列：', row_min, row_max, col_min, col_max)
    # print(int(row), int(col))
    # print(row, col)
    data_list = []
    year_list = []
    for year in np.arange(int(dateStart), int(dateEnd) + 1):
        mon_list = []
        num_year = np.zeros((int(row_max) - int(row_min) + 1, int(col_max) - int(col_min) + 1))  # !!!

        for mon in np.arange(1, 13):
            if mon < 10:
                path = '{}/{}_{}0{}.hdf'.format(str(datapath), str(dataType), str(year), str(mon))
            else:
                path = '{}/{}_{}{}.hdf'.format(str(datapath), str(dataType), str(year), str(mon))
            print(path)
            # 获取数据
            dem = DemLoader()
            dem.file_hdf = os.path.join(AID_PATH, 'data/{}'.format(path))
            data = dem.get_data()
            # print(data)
            print(data.shape)
            mon_data = np.array(data[int(row_min) - 1:int(row_max), int(col_min) - 1:int(col_max)])

            mon_list.append(mon_data)
            # 逐年年值
            num_year = num_year + mon_data
            # print('num_year;', num_year)
        # 逐季度总量（1 - 3、4 - 6、7 - 9、10 - 12月）
        s_mon = 1
        qua_data_list = []
        while s_mon < 11:
            qua = np.zeros((int(row_max) - int(row_min) + 1, int(col_max) - int(col_min) + 1))  # !!!
            e_mon = s_mon + 3
            for mo in np.arange(s_mon, e_mon):
                qua = mon_list[s_mon - 1] + qua
            qua_data_list.append(qua)
            s_mon = e_mon
        # ---------- 月份数据    年值         季度数据
        year_data = [mon_list, num_year, qua_data_list]
        year_list.append(year_data)
    y_d_num = []
    fir_list = []
    sec_list = []
    thr_list = []
    fou_list = []
    for y_d in year_list:
        y_d_num.append(y_d[1])
        fir_list.append(y_d[2][0])
        sec_list.append(y_d[2][1])
        thr_list.append(y_d[2][2])
        fou_list.append(y_d[2][3])
    # 任意起止年份的平均年值；
    year_ave = ave_a(y_d_num, len(year_list))
    # 任意起止年份的平均季度值；
    fir_ave = ave_a(fir_list, len(year_list))
    sec_ave = ave_a(sec_list, len(year_list))
    thr_ave = ave_a(thr_list, len(year_list))
    fou_ave = ave_a(fou_list, len(year_list))
    # 逐季节总量（3 - 5、6 - 8、9 - 11、12 - 2月），
    for i in np.arange(0, len(year_list)):
        s_mon = 3
        sea_data_list = []
        while s_mon < 13:
            sea = np.zeros((int(row_max) - int(row_min) + 1, int(col_max) - int(col_min) + 1))
            e_mon = s_mon + 3
            for mo in np.arange(s_mon, e_mon):
                if s_mon < 10:
                    sea = year_list[i][0][s_mon - 1] + sea
                else:
                    if dateStart + i < dateEnd:
                        sea = year_list[i][0][s_mon - 1] + year_list[i + 1][0][0] + year_list[i + 1][0][1]
                    else:
                        sea = year_list[i][0][s_mon - 1]
            sea_data_list.append(sea)
            s_mon = e_mon
        # 加入每年的季节数据
        year_list[i].append(sea_data_list)
    # 任意起止年份的平均季节值；
    MAM_list = []
    JJA_list = []
    SON_list = []
    DJF_list = []
    for y_d in year_list:
        MAM_list.append(y_d[3][0])
        JJA_list.append(y_d[3][1])
        SON_list.append(y_d[3][2])
        DJF_list.append(y_d[3][3])
    MAM_ave = ave_a(MAM_list, len(year_list))
    JJA_ave = ave_a(JJA_list, len(year_list))
    SON_ave = ave_a(SON_list, len(year_list))
    DJF_ave = ave_a(DJF_list, len(year_list))
    # 任意起止年份的平均逐月值，如1961 - 1990年平均的1月份的值
    monList = [[], [], [], [], [], [], [], [], [], [], [], []]
    for year_m in year_list:
        for i in np.arange(0, 12):
            monList[i].append(year_m[0][i])
    Jan_ave = ave_a(monList[0], len(year_list))
    Feb_ave = ave_a(monList[1], len(year_list))
    Mar_ave = ave_a(monList[2], len(year_list))
    Apr_ave = ave_a(monList[3], len(year_list))
    May_ave = ave_a(monList[4], len(year_list))
    Jun_ave = ave_a(monList[5], len(year_list))
    Jul_ave = ave_a(monList[6], len(year_list))
    Aug_ave = ave_a(monList[7], len(year_list))
    Sep_ave = ave_a(monList[8], len(year_list))
    Oct_ave = ave_a(monList[9], len(year_list))
    Nov_ave = ave_a(monList[10], len(year_list))
    Dec_ave = ave_a(monList[11], len(year_list))

    # year_data = [mon_list, num_year, qua_data_list， sea_data_list]
    # 任意一年年值 / 季度值 / 季节值 / 月值与任意起止年份相应平均值的距平（距平即相对差值：（一年值 - 多年平均值） / 多年平均值 * 100 %）。

    # year_data = [mon_list, num_year, qua_data_list， sea_data_list]
    for ye in year_list:
        qua_jp_list = []
        sea_jp_list = []
        mon_jp_list = []
        # 年值 距平
        year_jp = juping_a(ye[1], year_ave)
        # 季度值 距平
        fir_jp = juping_a(ye[2][0], fir_ave)
        sec_jp = juping_a(ye[2][1], sec_ave)
        thr_jp = juping_a(ye[2][2], thr_ave)
        fou_jp = juping_a(ye[2][3], fou_ave)
        qua_jp_list.append(fir_jp)
        qua_jp_list.append(sec_jp)
        qua_jp_list.append(thr_jp)
        qua_jp_list.append(fou_jp)
        # 季节值 距平
        MAM_jp = juping_a(ye[3][0], MAM_ave)
        JJA_jp = juping_a(ye[3][1], JJA_ave)
        SON_jp = juping_a(ye[3][2], SON_ave)
        DJF_jp = juping_a(ye[3][3], DJF_ave)
        sea_jp_list.append(MAM_jp)
        sea_jp_list.append(JJA_jp)
        sea_jp_list.append(SON_jp)
        sea_jp_list.append(DJF_jp)
        # 月值 距平
        Jan_jp = juping_a(ye[0][0], Jan_ave)
        Feb_jp = juping_a(ye[0][1], Feb_ave)
        Mar_jp = juping_a(ye[0][2], Mar_ave)
        Apr_jp = juping_a(ye[0][3], Apr_ave)
        May_jp = juping_a(ye[0][4], May_ave)
        Jun_jp = juping_a(ye[0][5], Jun_ave)
        Jul_jp = juping_a(ye[0][6], Jul_ave)
        Aug_jp = juping_a(ye[0][7], Aug_ave)
        Sep_jp = juping_a(ye[0][8], Sep_ave)
        Oct_jp = juping_a(ye[0][9], Oct_ave)
        Nov_jp = juping_a(ye[0][10], Nov_ave)
        Dec_jp = juping_a(ye[0][11], Dec_ave)
        mon_jp_list.append(Jan_jp)
        mon_jp_list.append(Feb_jp)
        mon_jp_list.append(Mar_jp)
        mon_jp_list.append(Apr_jp)
        mon_jp_list.append(May_jp)
        mon_jp_list.append(Jun_jp)
        mon_jp_list.append(Jul_jp)
        mon_jp_list.append(Aug_jp)
        mon_jp_list.append(Sep_jp)
        mon_jp_list.append(Oct_jp)
        mon_jp_list.append(Nov_jp)
        mon_jp_list.append(Dec_jp)
        # 加入每年的数据中
        ye.append(year_jp)
        ye.append(qua_jp_list)
        ye.append(sea_jp_list)
        ye.append(mon_jp_list)

    data_ave_name = ['year', 'fir_ave', 'sec_ave', 'thr_ave', 'fou_ave', 'MAM_ave', 'JJA_ave', 'SON_ave', 'DJF_ave',
                     'Jan_ave', 'Feb_ave', 'Mar_ave', 'Apr_ave', 'May_ave', 'Jun_ave',
                     'Jul_ave', 'Aug_ave', 'Sep_ave', 'Oct_ave', 'Nov_ave', 'Dec_ave']

    data_ave_val = [year_ave, fir_ave, sec_ave, thr_ave, fou_ave, MAM_ave, JJA_ave, SON_ave, DJF_ave, Jan_ave,
                    Feb_ave, Mar_ave, Apr_ave, May_ave, Jun_ave, Jul_ave, Aug_ave, Sep_ave, Oct_ave, Nov_ave, Dec_ave]

    data_row_name = ['year_data', 'year_dep', 'fir_data', 'fir_dep', 'sec_data', 'sec_dep', 'thr_data', 'thr_dep',
                     'fou_data', 'fou_dep',
                     'MAM_data', 'MAM_dep', 'JJA_data', 'JJA_dep', 'SON_data', 'SON_dep', 'DJF_data', 'DJF_dep',
                     'Jan_data', 'Jan_dep',
                     'Feb_data', 'Feb_dep', 'Mar_data', 'Mar_dep', 'Apr_data', 'Apr_dep', 'May_data', 'May_dep',
                     'Jun_data', 'Jun_dep',
                     'Jul_data', 'Jul_dep', 'Aug_data', 'Aug_dep', 'Sep_data', 'Sep_dep', 'Oct_data', 'Oct_dep',
                     'Nov_data', 'Nov_dep',
                     'Dec_data', 'Dec_dep']
    # [[gr_name1, [key_name1, val1], [key_name2, val2]],[gr_name2, [key_name1, val1], [key_name2, val2]]]
    #   00              010     011      020      021       10        110        111     120          121
    fir_task = [['years_data']]  # 1）逐年年值
    sec_task = []  # 2）任意起止年份的平均年值
    thi_task = [['quas_data'], ['qua_ave']]  # 3）逐季度总量（1-3、4-6、7-9、10-12月），任意起止年份的平均季度值；
    fou_task = [['seas_data'], ['sea_ave']]  # 4）逐季节总量（3-5、6-8、9-11、12-2月），任意起止年份的平均季节值；
    fif_task = [['mons_ave']]  # 5）任意起止年份的平均逐月值，如1961-1990年平均的1月份的值
    six_task = [['years'], ['quas'], ['seas'], ['mons']]  # 6）所有一年年值/季度值/季节值/月值与任意起止年份相应平均值的距平
    six_task2 = []  # 6）任意一年年值/季度值/季节值/月值与任意起止年份相应平均值的距平

    year = int(dateStart)
    for ye in year_list:
        data_row = [ye[1], ye[4],
                    ye[2][0], ye[5][0], ye[2][1], ye[5][1], ye[2][2], ye[5][2], ye[2][3], ye[5][3],
                    ye[3][0], ye[6][0], ye[3][1], ye[6][1], ye[3][2], ye[6][2], ye[3][3], ye[6][3],
                    ye[0][0], ye[7][0], ye[0][1], ye[7][1], ye[0][2], ye[7][2], ye[0][3], ye[7][3],
                    ye[0][4], ye[7][4], ye[0][5], ye[7][5], ye[0][6], ye[7][6], ye[0][7], ye[7][7],
                    ye[0][8], ye[7][8], ye[0][9], ye[7][9], ye[0][10], ye[7][10], ye[0][11], ye[7][11]
                    ]
        # 1
        fir_task[0].append([year, data_row[0]])
        # 3 逐季度
        for i in np.arange(2, 10, 2):
            qua_data_key_name = '{}_{}'.format(year, data_row_name[i])
            thi_task[0].append([qua_data_key_name, data_row[i]])
        # 4 逐季节
        for i in np.arange(10, 18, 2):
            sea_data_key_name = '{}_{}'.format(year, data_row_name[i])
            fou_task[0].append([sea_data_key_name, data_row[i]])
        dep_li = []
        # 6 逐年
        six_task[0].append([year, data_row[1]])
        dep_li.append(['years', [year, data_row[1]]])

        # 6 逐季度
        dep_li.append(['quas'])
        for i in np.arange(3, 11, 2):
            juping_data_key_name = '{}_{}'.format(year, data_row_name[i])
            six_task[1].append([juping_data_key_name, data_row[i]])
            dep_li[1].append([juping_data_key_name, data_row[i]])
        # 6 逐季节
        dep_li.append(['seas'])
        for i in np.arange(11, 19, 2):
            juping_data_key_name = '{}_{}'.format(year, data_row_name[i])
            six_task[2].append([juping_data_key_name, data_row[i]])
            dep_li[2].append([juping_data_key_name, data_row[i]])
        # 6 逐月
        dep_li.append(['mons'])
        for i in np.arange(19, 43, 2):
            # print(len(data_row_name), i, year)
            juping_data_key_name = '{}_{}'.format(year, data_row_name[i])
            six_task[3].append([juping_data_key_name, data_row[i]])
            dep_li[3].append([juping_data_key_name, data_row[i]])
        six_task2.append(dep_li)
        # data_list.append(data_row)
        year += 1
    # 2
    sec_task.append(['years_ave', ['data', year_ave]])
    # 3 平均
    for i in np.arange(1, 5):
        thi_task[1].append([data_ave_name[i], data_ave_val[i]])
    # 4 平均
    for i in np.arange(5, 9):
        fou_task[1].append([data_ave_name[i], data_ave_val[i]])
    # 5 平均
    for i in np.arange(9, 21):
        fif_task[0].append([data_ave_name[i], data_ave_val[i]])
    # task_name = ['逐年年值', '平均年值', '逐季度总量与平均季度值', '逐季节总量与平均季节值', '平均逐月值', '距平']
    data_list.append([fir_task, sec_task, thi_task, fou_task, fif_task, six_task, six_task2])
    return data_list


def data_point(dataType, dateStart, dateEnd, leftLongitude, leftLatitude, outPath):
    dateStart = int(dateStart)
    dateEnd = int(dateEnd)
    data = num_p_a(dataType, dateStart, dateEnd, leftLongitude, leftLatitude)
    data_str = ''

    kg = '  '
    hh = '\n'
    for ad in data:
        print(ad)
        for dd in ad:
            data_str = data_str + str(dd) + kg
        data_str = data_str + hh
        print(data_str)
    # 输出txt
    path = os.path.join(AID_PATH, '{}/data.txt'.format(outPath))
    dirs = os.path.join(AID_PATH, '{}'.format(outPath))
    if not os.path.exists(dirs):
        os.makedirs(dirs)
    # print(path)
    with open(path, "w", encoding='utf-8') as f:
        f.write(data_str)


def data_area(dataType, task, dateStart, dateEnd, dataEvery, leftLongitude, leftLatitude,
              rightLongitude, rightLatitude, outPath):
    dataEvery = int(dataEvery)
    dateStart = int(dateStart)
    dateEnd = int(dateEnd)
    print('Start')
    if dataEvery != 0 and task != 6:
        return '参数错误！'
    elif dataEvery != 0 and (dataEvery > dateEnd or dataEvery < dateStart):
        return 'dataEvery参数错误！'
    data_a = num_a(dataType, dateStart, dateEnd, leftLongitude, leftLatitude, rightLongitude, rightLatitude, )
    # [[gr_name1, [key_name1, val1], [key_name2, val2]],[gr_name2, [key_name1, val1], [key_name2, val2]]]
    #   00              010     011      020      021       10        110        111     120          121
    task_name = ['year_data', 'years_ave', 'qua_and_quas_ave', 'sea_and_seas_ave', 'mon_ave', 'dep']
    task_path = task_name[int(task) - 1]
    data = data_a[0]
    datas = {}
    if task == 6 and dataEvery != 0:
        path = os.path.join(AID_PATH, '{}\{}_{}.hdf'.format(outPath, dataEvery, task_path))
        data_out = data[6][dataEvery - int(dateStart)]
    else:
        path = os.path.join(AID_PATH, '{}\{}.hdf'.format(outPath, task_path))
        data_out = data[int(task) - 1]
    print(path)
    for da in data_out:
        gr_name = str(da[0]);
        print(gr_name)
        for d in da[1:]:
            key_name = str(d[0])
            datas['{}/{}'.format(gr_name, key_name)] = d[1]
    dirs =os.path.join(AID_PATH, '{}'.format(outPath))
    if not os.path.exists(dirs):
        os.makedirs(dirs)
    write_hdf5_and_compress(datas, path)
    print('over')
    return 'over'


filterwarnings("ignore")

resolution_types = {
    '1KM',
    '1KMCorrect',
    '4KM',
    '4KMCorrect',
}
functions = {
    'fy4a_save_4km_orbit_data_in_database',
    'fy4a_save_4km_orbit_ref_data_in_database',
    'product_fy3d_1km_daily_data',
    'product_fy4a_4kmcorrect_disk_full_data_orbit',
    'product_fy4a_1km_disk_full_data_orbit',
    'product_fy4a_1kmcorrect_disk_full_data_orbit',
    'product_fy4a_disk_full_image_orbit',
    'product_combine_data',
    'product_image',
    'product_cloud_image',
}

sat_sensors = {
    'FY4A_AGRI',
    'FY3D_MERSI',
}

frequencys = {
    'Orbit',
    'Daily',
    'Monthly'
    'Yearly',
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GFSSI Schedule')
    parser.add_argument('--dataType', '-t', help='数据类型(GHI/DBI/DHI/GTI/...)', required=True)
    parser.add_argument('--modeType', '-m', help='单点or范围(point或area)', required=True)
    parser.add_argument('--taskChoice', '-c',
                        help='求范围值时任务选择：1、逐年年值：(y) 2、平均年值:(ya) 3、逐季度总量与平均季度值:(qa) 4、逐季节总量与平均季节值:(sa) 5、平均逐月值:(m) 6、距平:(d) ',
                        required=False)
    parser.add_argument('--dateStart', '-s', help='开始年份，YYYY(2019)', required=True)
    parser.add_argument('--dateEnd', '-e', help='结束年份，YYYY(2019)', required=True)
    parser.add_argument('--dateEvery', '-v', help='计算指定年份距平，YYYY(2019)；或所有年份对应的距平（0）', required=False)
    parser.add_argument('--leftLongitude', '-l', help='经度或左上角经度，47.302235', required=True)
    parser.add_argument('--leftLatitude', '-a', help='纬度或左上角纬度，85.880519', required=True)
    parser.add_argument('--rightLongitude', '-r', help='右下角经度，47.302235', required=False)
    parser.add_argument('--rightLatitude', '-i', help='右下角经度，85.880519', required=False)
    parser.add_argument('--outPath', '-o', help='输出路径', required=True)
    args = parser.parse_args()

    # assert args.function.lower() in functions, '{}'.format(functions)
    # assert args.resolution_type in resolution_types, '{}'.format(resolution_types)
    # assert args.sat_sensor in sat_sensors, '{}'.format(sat_sensors)

    # datetime_start = datetime.strptime(args.dateStart, '%Y')
    # datetime_end = datetime.strptime(args.dateEnd, '%Y')
    # dateEvery = datetime.strptime(args.dateEvery, '%Y%m%d')
    task_dict = {
        'y': 1, 'ya': 2, 'qa': 3, 'sa': 4, 'm': 5, 'd': 6
    }
    if args.modeType == 'point':
        data_point(args.dataType, args.dateStart, args.dateEnd, args.leftLongitude, args.leftLatitude,
                   args.outPath)
    elif args.modeType == 'area':
        task = task_dict[str(args.taskChoice)]
        if args.dateEvery:
            dateEvery = args.dateEvery
        else:
            dateEvery = 0
        print(args.dataType, task, args.dateStart, args.dateEnd, dateEvery, args.leftLongitude,
              args.leftLatitude, args.rightLongitude, args.rightLatitude, args.outPath)
        data_area(args.dataType, task, args.dateStart, args.dateEnd, dateEvery, args.leftLongitude,
                  args.leftLatitude, args.rightLongitude, args.rightLatitude, args.outPath)

    # t_a = data_area('DHI', 6, 1966, 1969, 1966, '70.01509999999999', '10.225', '70.06509999999999',
    #                 '10.034999999999998', 'outdata\hdf')
    # print(t_a)
