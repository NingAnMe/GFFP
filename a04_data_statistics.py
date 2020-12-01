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
# from schedule import *
from utils.config import get_datatype
from utils.get_index_by_lonlat import get_point_index_by_lon_lat, get_area_index_by_lon_lat
from utils.data import DemLoader
from utils.path import *
from utils.hdf5 import *
from datetime import datetime
import pandas as pd
import h5py
from collections import defaultdict
from user_config import *

task_dict = {
    'yearSum': 1, 'yearMean': 2, 'yearAnomaly': 3, 'monthSum': 4, 'monthMean': 5, 'monthAnomaly': 6,
    'seasonSum': 7, 'seasonMean': 8, 'seasonAnomaly': 9, 'quarterSum': 10, 'quarterMean': 11, 'quarterAnomaly': 12
}
'''
1）输入单点经纬度，输出txt格式的数据；
2）输入区域经纬度范围，输出
'''


def ave(sum, len):  # R
    num_np = np.array(sum)
    ave = np.sum(num_np) / len
    return ave


def ave_a(lis, li_len):
    print('开始求平均')
    print(lis)
    print(len(lis))
    print(len(lis[0]))
    print(len(lis[0][0]))
    sta, end = len(lis[0]), len(lis[0][0])
    sum_a = np.zeros((sta, end))
    # print(sum_a)
    for s in lis:
        sum_a = sum_a + s
    # print(sum_a)
    ave_a = sum_a / li_len
    return ave_a


def juping(oneyear, ave):  # R
    '''
    :param oneyear: 一年总值
    :param ave: 多年平均值
    :return:
    '''
    # （一年值 - 多年平均值） / 多年平均值 * 100 %
    jp = (oneyear - ave) / ave * 100
    jl = '{}%'.format(jp)
    return jl


def juping_a(oneyear, ave):
    # （一年值 - 多年平均值） / 多年平均值 * 100 %
    jp = (oneyear - ave) / ave * 100
    print('距平：', jp)
    return jp


def year_sum(year_data):
    '''
    :param year_data: 一年中每个月的数据
    :return: 年值
    '''
    num_year = np.zeros((len(year_data[0]), len(year_data[0][0])))
    print('num_year.shape', num_year.shape)
    # 年值
    for mon_data in year_data:
        num_year = num_year + mon_data
    return num_year


def years_sum(dateStart, dateEnd, data):
    '''
    :param dateStart:
    :param dateEnd:
    :param data:
    :return: {
            year1 : 全部年值
            year2 : 全部年值
            }
    '''
    years_num = dict()
    for year in np.arange(int(dateStart), int(dateEnd) + 1):
        data_lo_la_year = data[year]
        data = data_lo_la_year['data']
        year_sum_s = year_sum(data)
        print(year)
        years_num[year] = year_sum_s

    return years_num


def years_sum_point(dateStart, dateEnd, data, row, col):  # R
    '''
    :return: {year1 ; 年值,year2 ; 年值}
    '''
    years_num = data
    print(years_num)
    years_num_p = dict()
    for year in np.arange(int(dateStart), int(dateEnd) + 1):
        year_sum = years_num[year]
        print(row, col)
        year_point_data = year_sum[(row, col)]
        print(year)
        print('year_point_data', year_point_data)
        years_num_p[year] = year_point_data

    return years_num_p


def years_sum_area(dateStart, dateEnd, data, row_min, row_max, col_min, col_max):  # R
    '''
     :return: {year1 ; 年值,year2 ; 年值}
    '''
    years_num = data
    print(years_num)
    years_num_p = dict()
    for year in np.arange(int(dateStart), int(dateEnd) + 1):
        year_sum = years_num[year]
        year_area_data = year_sum[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
        print(year)
        print('year_area_data', year_area_data)
        years_num_p[year] = year_area_data
    return years_num_p


def get_mon_data(data):
    mon_dic = {}
    for year, data_y in data.items():
        print(year, data_y)
        for i in np.arange(1, 13):  # 获取月值
            if i < 10:
                key_name = '{}0{}'.format(year, i)
            else:
                key_name = '{}{}'.format(year, i)
            mon_dic[key_name] = data_y['data'][i - 1]
    return mon_dic


def get_date_start_end(dateStart, dateEnd, data_type):  # R
    '''
    获取所需文件列表
    '''
    print(dateStart, dateEnd, data_type)
    data_path = os.path.join(DATA_1KM_MONTH, data_type)
    # data_path = 'D:\project\py\gz\ky\gffp\\aid\data\{}'.format(data_type)
    print('get_date_start_end', data_path)
    file_list = []
    for file_ in os.listdir(data_path):
        filename = os.path.split(file_)[1]
        print(filename)
        date_str = str(filename).split('_')[1]
        file_date = date(int(date_str[:4]), int(date_str[4:6]), 1)
        # print(file_date)
        file_date = file_date.strftime("%Y")
        # print(int(file_date))
        if int(dateStart) <= int(file_date) <= int(dateEnd):
            # print('file_', file_)
            file_list.append(file_)
    if not file_list:
        raise ValueError('No file in {} {}'.format(dateStart, dateEnd))
    # print(file_list)
    print('----------------------------------------------------------------------------')
    return file_list


def judge_file(date_start, date_end, data_type, task_name):  # R
    '''
    :param task_name: 'sea' 'qua' 'year' 'mon'
    :return:file_list
    '''
    print('judge_file')
    if task_name == 'sea':
        data_path = os.path.join(DATA_1KM_SEASON, data_type)
    elif task_name == 'qua':
        data_path = os.path.join(DATA_1KM_QUARTER, data_type)
    elif task_name == 'year':
        data_path = os.path.join(DATA_1KM_YEAR, data_type)
    elif task_name == 'mon':
        data_path = os.path.join(DATA_1KM_MONTH, data_type)
    print(date_start, date_end, data_type)
    print(data_path)
    if not os.path.exists(data_path):
        os.makedirs(data_path)
        return 0
    # GHI_2020_Fir.hdf
    file_list = []
    for file_ in os.listdir(data_path):
        filename = os.path.split(file_)[1]
        print(filename)
        date_str = str(filename).split('_')[1]
        file_date = date(int(date_str[:4]), 1, 1)
        file_date = file_date.strftime("%Y")
        if int(date_start) <= int(file_date) <= int(date_end):
            file_list.append(file_)
    if not file_list:
        print('Judge No File')
        return 0
    return file_list


def get_file_year(files):  # R
    '''获取'''
    print('get_file_year')
    files_year = defaultdict(list)
    for file_ in files:
        # print(file_)
        filename = os.path.split(file_)[1]
        # print(filename)
        date_str = str(filename).split('_')[1]
        file_date = date(int(date_str[:4]), int(date_str[4:6]), 1)
        y = file_date.year
        files_year[y].append(file_)
    print(files_year)
    print('---------------------------------------------------------------------')
    return files_year


def get_file_mon(files):  # R
    ''''
    :return [[{{2020:Feb_file}}][][][][]]
    '''
    files_mon = [[{}], [{}], [{}], [{}], [{}], [{}], [{}], [{}], [{}], [{}], [{}], [{}]]
    for file_ in files:
        print(file_)
        filename = os.path.split(file_)[1]
        # print(filename)
        date_str = str(filename).split('_')[1]
        file_date = date(int(date_str[:4]), int(date_str[4:6]), 1)
        y = int(file_date.year)
        m = int(file_date.month)
        if m < 10:
            file_key = '{}0{}'.format(y, m)
        else:
            file_key = '{}{}'.format(y, m)
        print(file_key)
        files_mon[m - 1][0][file_key] = file_
    print(files_mon)
    print('---------------------------------------------------------------------')
    return files_mon


def get_mon_data_point(file_list, path, dataType, row, col):  # R
    mon_list = get_file_mon(file_list)
    mon_dic_point = {}
    a = 0
    for mon_dic in mon_list:
        for y_m_key, hdf in mon_dic[0].items():
            a += 1
            data_path = os.path.join(path, dataType)
            file_hdf = '{}/{}'.format(data_path, hdf)
            print('file_hdf', file_hdf, dataType)
            data = get_hdf5_data(file_hdf, dataType, 1, 0, [0, np.inf], np.nan)[(row, col)]
            mon_dic_point[y_m_key] = data
            # print(file_hdf, data)
            if a == 1:
                dem = DemLoader()
                dem.file_hdf = file_hdf
                lon, lat = dem.get_lon_lat()
            # print(lon, lat)
    print(mon_dic_point, lon[(row, col)], lat[(row, col)])
    return mon_dic_point, lon[(row, col)], lat[(row, col)]


def get_mon_data_area(file_list, path, dataType, row_min, row_max, col_min, col_max):  # R
    mon_list = get_file_mon(file_list)
    mon_dic_point = {}
    a = 0
    for mon_dic in mon_list:
        for y_m_key, hdf in mon_dic[0].items():
            a += 1
            data_path = os.path.join(path, dataType)
            file_hdf = '{}/{}'.format(data_path, hdf)
            print('file_hdf', file_hdf, dataType)
            data = get_hdf5_data(file_hdf, dataType, 1, 0, [0, np.inf], np.nan)
            mon_dic_point[y_m_key] = data[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
            # print(file_hdf, data)
            if a == 1:
                dem = DemLoader()
                dem.file_hdf = file_hdf
                lon, lat = dem.get_lon_lat()
            # print(lon, lat)
    lon = lon[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    lat = lat[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    print(mon_dic_point, lon, lat)
    return mon_dic_point, lon, lat


def get_mon_mean_data_point(file_list, path, dataType, row, col):  # R
    mon_list = get_file_mon(file_list)
    mon_mean_dic_point = {}
    a = 0
    for mon in np.arange(1, len(mon_list) + 1):
        mon_dic = mon_list[mon - 1]
        m_sum = 0
        i = 0
        for y_m_key, hdf in mon_dic[0].items():
            a += 1
            i += 1
            data_path = os.path.join(path, dataType)
            file_hdf = '{}/{}'.format(data_path, hdf)
            print('file_hdf', file_hdf, dataType)
            data = get_hdf5_data(file_hdf, dataType, 1, 0, [0, np.inf], np.nan)[(row, col)]
            m_sum = data + m_sum
            if a == 1:
                dem = DemLoader()
                dem.file_hdf = file_hdf
                lon, lat = dem.get_lon_lat()
            # print(lon, lat)
        mon_mean_dic_point[mon] = m_sum / i
    print(mon_mean_dic_point, lon[(row, col)], lat[(row, col)])
    return mon_mean_dic_point, lon[(row, col)], lat[(row, col)]


def get_mon_mean_data_area(file_list, path, dataType, row_min, row_max, col_min, col_max):  # R
    mon_list = get_file_mon(file_list)
    mon_mean_dic_point = {}
    a = 0
    for mon in np.arange(1, len(mon_list) + 1):
        mon_dic = mon_list[mon - 1]
        i = 0
        for y_m_key, hdf in mon_dic[0].items():
            a += 1
            i += 1
            data_path = os.path.join(path, dataType)
            file_hdf = '{}/{}'.format(data_path, hdf)
            print('file_hdf', file_hdf, dataType)
            data = get_hdf5_data(file_hdf, dataType, 1, 0, [0, np.inf], np.nan)
            if i == 1:
                m_sum = data[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
            else:
                m_sum = data[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1] + m_sum
            if a == 1:
                dem = DemLoader()
                dem.file_hdf = file_hdf
                lon, lat = dem.get_lon_lat()
            # print(lon, lat)
        mon_mean_dic_point[mon] = m_sum / i
    lon = lon[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    lat = lat[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    print(mon_mean_dic_point, lon, lat)
    return mon_mean_dic_point, lon, lat


def get_mon_ano_data_point(file_list, path, dataType, row, col):  # R
    mon_list = get_file_mon(file_list)
    mon_dic_point = {}
    mon_mean_dic_point = {}
    a = 0
    for mon in np.arange(1, len(mon_list) + 1):
        mon_dic = mon_list[mon - 1]
        m_sum = 0
        i = 0
        mon_data_dic = {}
        for y_m_key, hdf in mon_dic[0].items():
            a += 1
            i += 1
            data_path = os.path.join(path, dataType)
            file_hdf = '{}/{}'.format(data_path, hdf)
            print('file_hdf', file_hdf, dataType)
            data = get_hdf5_data(file_hdf, dataType, 1, 0, [0, np.inf], np.nan)[(row, col)]
            mon_data_dic[y_m_key] = data
            m_sum = data + m_sum
            if a == 1:
                dem = DemLoader()
                dem.file_hdf = file_hdf
                lon, lat = dem.get_lon_lat()
            # print(lon, lat)
        mon_mean_dic_point[mon] = m_sum / i
        # print(mon_data_dic)
        for y_m_key2, data in mon_data_dic.items():
            mon_dic_point[y_m_key2] = juping_a(data, m_sum / i)

    print(mon_dic_point, lon[(row, col)], lat[(row, col)])
    return mon_dic_point, lon[(row, col)], lat[(row, col)]


def get_mon_ano_data_area(file_list, path, dataType, row_min, row_max, col_min,
                          col_max):  # R

    mon_list = get_file_mon(file_list)
    mon_dic_point = {}
    mon_mean_dic_point = {}
    a = 0
    for mon in np.arange(1, len(mon_list) + 1):
        mon_dic = mon_list[mon - 1]
        m_sum = 0
        i = 0
        mon_data_dic = {}
        for y_m_key, hdf in mon_dic[0].items():
            a += 1
            i += 1
            data_path = os.path.join(path, dataType)
            file_hdf = '{}/{}'.format(data_path, hdf)
            print('file_hdf', file_hdf, dataType)
            data = get_hdf5_data(file_hdf, dataType, 1, 0, [0, np.inf], np.nan)
            print('data.shape', data.shape)
            print(int(row_min), int(row_max) + 1, int(col_min), int(col_max) + 1)
            if i == 1:
                m_sum = data[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
            else:
                m_sum = data[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1] + m_sum
            mon_data_dic[y_m_key] = data[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
            if a == 1:
                dem = DemLoader()
                dem.file_hdf = file_hdf
                lon, lat = dem.get_lon_lat()
            # print(lon, lat)
        print(m_sum.shape)
        mon_mean_dic_point[mon] = m_sum / i
        # print(mon_data_dic)
        for y_m_key2, data in mon_data_dic.items():
            mon_dic_point[y_m_key2] = juping_a(data, m_sum / i)
    lon = lon[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    lat = lat[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    print(len(mon_dic_point), lon.shape, lat.shape, row_min, row_max, col_min,
          col_max)
    return mon_dic_point, lon, lat


def get_file_sea(files, date_start, date_end):  # R
    ''' {year:{{'MAM':{mon:file},'JJA':{},'SON':{},'DJF':{}}}'''
    sea_dic = {}
    for file_ in files:
        print(file_)
        filename = os.path.split(file_)[1]
        print(filename)
        date_str = str(filename).split('_')[1]
        file_date = date(int(date_str[:4]), int(date_str[4:6]), 1)
        y = int(file_date.year)
        m = int(file_date.month)
        print('m:', m)
        if m in [3, 4, 5]:
            try:
                sea_dic[y]
            except:
                sea_dic[y] = {}
            try:
                sea_dic[y]['MAM']
            except:
                sea_dic[y]['MAM'] = {}
            sea_dic[y]['MAM'][m] = file_
        elif m in [6, 7, 8]:
            try:
                sea_dic[y]
            except:
                sea_dic[y] = {}
            try:
                sea_dic[y]['JJA']
            except:
                sea_dic[y]['JJA'] = {}
            sea_dic[y]['JJA'][m] = file_
        elif m in [9, 10, 11]:
            try:
                sea_dic[y]
            except:
                sea_dic[y] = {}
            try:
                sea_dic[y]['SON']
            except:
                sea_dic[y]['SON'] = {}
            sea_dic[y]['SON'][m] = file_
        elif m == 12:
            try:
                sea_dic[y]
            except:
                sea_dic[y] = {}
            try:
                sea_dic[y]['DJF']
            except:
                sea_dic[y]['DJF'] = {}
            sea_dic[y]['DJF'][m] = file_
        elif m in [1, 2]:
            try:
                sea_dic[y - 1]
            except:
                sea_dic[y - 1] = {}
            try:
                sea_dic[y - 1]['DJF']
            except:
                sea_dic[y - 1]['DJF'] = {}
            sea_dic[y - 1]['DJF'][m] = file_
    del sea_dic[int(date_start) - 1]
    del sea_dic[int(date_end)]['DJF']
    print(sea_dic)
    print('---------------------------------------------------------------------')
    return sea_dic


def get_file_sea_hdf(files):  # R

    sea_dic = {}
    for file_ in files:
        print(file_)
        filename = os.path.split(file_)[1]
        print(filename)
        date_str = str(filename).split('_')[1]
        sea_str = str(filename).split('_')[2][:-4]
        file_date = date(int(date_str[:4]), 1, 1)
        y = int(file_date.year)
        sea_key = '{}_{}'.format(y, sea_str)
        sea_dic[sea_key] = file_
    print(sea_dic)
    print('---------------------------------------------------------------------')
    return sea_dic


def get_sea_mean(data_sea, dateStart, dateEnd):
    sea_lsit = [[], [], [], []]
    for sea_key, data in data_sea.items():
        filename = os.path.split(sea_key)[1]
        # print(filename)
        sea_str = str(filename).split('_')[1]
        print('sea_str', sea_str)
        if sea_str == 'MAM':
            sea_lsit[0].append(data)
        elif sea_str == 'JJA':
            sea_lsit[1].append(data)
        elif sea_str == 'SON':
            sea_lsit[2].append(data)
        elif sea_str == 'DJF':
            sea_lsit[3].append(data)
    mean = {}
    len_year = int(dateEnd) - int(dateStart) + 1
    mean['MAM'] = ave_a(sea_lsit[0], len_year)
    mean['JJA'] = ave_a(sea_lsit[1], len_year)
    mean['SON'] = ave_a(sea_lsit[2], len_year)
    if len_year != 1:
        mean['DJF'] = ave_a(sea_lsit[3], len_year - 1)
    return mean


def get_sea_mean_area(data_sea, dateStart, dateEnd):
    sea_lsit = [[], [], [], []]
    for sea_key, data in data_sea.items():
        filename = os.path.split(sea_key)[1]
        # print(filename)
        sea_str = str(filename).split('_')[1]
        print('sea_str', sea_str)
        if sea_str == 'MAM':
            sea_lsit[0].append(data)
        elif sea_str == 'JJA':
            sea_lsit[1].append(data)
        elif sea_str == 'SON':
            sea_lsit[2].append(data)
        elif sea_str == 'DJF':
            sea_lsit[3].append(data)
    mean = {}
    len_year = int(dateEnd) - int(dateStart) + 1
    mean['MAM'] = ave_a(sea_lsit[0], len_year)
    mean['JJA'] = ave_a(sea_lsit[1], len_year)
    mean['SON'] = ave_a(sea_lsit[2], len_year)
    if len_year != 1:
        mean['DJF'] = ave_a(sea_lsit[3], len_year - 1)
    return mean


def get_sea_ano(data_sea, mean_sea, dateStart, dateEnd):
    mean = mean_sea
    jp_dic = {}
    for sea_key, data in data_sea.items():
        filename = os.path.split(sea_key)[1]
        # print(filename)
        sea_str = str(filename).split('_')[1]
        print('sea_str', sea_str)
        if sea_str == 'MAM':
            jp = juping_a(data, mean['MAM'])
        elif sea_str == 'JJA':
            jp = juping_a(data, mean['JJA'])
        elif sea_str == 'SON':
            jp = juping_a(data, mean['SON'])
        elif sea_str == 'DJF':
            jp = juping_a(data, mean['DJF'])
        jp_dic[sea_key] = jp
    return jp_dic


def get_sea_ano_area(data_sea, mean_sea, dateStart, dateEnd):
    mean = mean_sea
    jp_dic = {}
    for sea_key, data in data_sea.items():
        filename = os.path.split(sea_key)[1]
        # print(filename)
        sea_str = str(filename).split('_')[1]
        print('sea_str', sea_str)
        if sea_str == 'MAM':
            jp = juping_a(data, mean['MAM'])
        elif sea_str == 'JJA':
            jp = juping_a(data, mean['JJA'])
        elif sea_str == 'SON':
            jp = juping_a(data, mean['SON'])
        elif sea_str == 'DJF':
            jp = juping_a(data, mean['DJF'])
        jp_dic[sea_key] = jp
    return jp_dic


def get_file_qua(files, date_start, date_end):  # R
    ''' {year:{{'fir':{mon:file},'sec':{},'thr':{},'fou':{}}}'''
    qua_dic = {}
    for file_ in files:
        print(file_)
        filename = os.path.split(file_)[1]
        print(filename)
        date_str = str(filename).split('_')[1]
        file_date = date(int(date_str[:4]), int(date_str[4:6]), 1)
        y = int(file_date.year)
        m = int(file_date.month)
        print('m:', m)
        if m in [1, 2, 3]:
            try:
                qua_dic[y]
            except:
                qua_dic[y] = {}
            try:
                qua_dic[y]['fir']
            except:
                qua_dic[y]['fir'] = {}
            qua_dic[y]['fir'][m] = file_
        elif m in [4, 5, 6]:
            try:
                qua_dic[y]
            except:
                qua_dic[y] = {}
            try:
                qua_dic[y]['sec']
            except:
                qua_dic[y]['sec'] = {}
            qua_dic[y]['sec'][m] = file_
        elif m in [7, 8, 9]:
            try:
                qua_dic[y]
            except:
                qua_dic[y] = {}
            try:
                qua_dic[y]['thr']
            except:
                qua_dic[y]['thr'] = {}
            qua_dic[y]['thr'][m] = file_
        elif m in [10, 11, 12]:
            try:
                qua_dic[y]
            except:
                qua_dic[y] = {}
            try:
                qua_dic[y]['fou']
            except:
                qua_dic[y]['fou'] = {}
            qua_dic[y]['fou'][m] = file_
    print(qua_dic)
    print('---------------------------------------------------------------------')
    return qua_dic


def get_file_qua_hdf(files):  # R

    sea_dic = {}
    for file_ in files:
        print(file_)
        filename = os.path.split(file_)[1]
        print(filename)
        date_str = str(filename).split('_')[1]
        sea_str = str(filename).split('_')[2][:-4]
        file_date = date(int(date_str[:4]), 1, 1)
        y = int(file_date.year)
        sea_key = '{}_{}'.format(y, sea_str)
        sea_dic[sea_key] = file_
    print(sea_dic)
    print('---------------------------------------------------------------------')
    return sea_dic


def get_qua_mean(data_sea, dateStart, dateEnd):
    sea_lsit = [[], [], [], []]
    for sea_key, data in data_sea.items():
        filename = os.path.split(sea_key)[1]
        # print(filename)
        sea_str = str(filename).split('_')[1]
        print('sea_str', sea_str)
        if sea_str == 'fir':
            sea_lsit[0].append(data)
        elif sea_str == 'sec':
            sea_lsit[1].append(data)
        elif sea_str == 'thr':
            sea_lsit[2].append(data)
        elif sea_str == 'fou':
            sea_lsit[3].append(data)
    mean = {}
    len_year = int(dateEnd) - int(dateStart) + 1
    mean['fir'] = ave_a(sea_lsit[0], len_year)
    mean['sec'] = ave_a(sea_lsit[1], len_year)
    mean['thr'] = ave_a(sea_lsit[2], len_year)
    mean['fou'] = ave_a(sea_lsit[3], len_year)
    return mean


def get_qua_mean_a(data_sea, dateStart, dateEnd):
    sea_lsit = [[], [], [], []]
    for sea_key, data in data_sea.items():
        filename = os.path.split(sea_key)[1]
        # print(filename)
        sea_str = str(filename).split('_')[1]
        print('sea_str', sea_str)
        if sea_str == 'fir':
            sea_lsit[0].append(data)
        elif sea_str == 'sec':
            sea_lsit[1].append(data)
        elif sea_str == 'thr':
            sea_lsit[2].append(data)
        elif sea_str == 'fou':
            sea_lsit[3].append(data)
    mean = {}
    len_year = int(dateEnd) - int(dateStart) + 1
    mean['fir'] = ave_a(sea_lsit[0], len_year)
    mean['sec'] = ave_a(sea_lsit[1], len_year)
    mean['thr'] = ave_a(sea_lsit[2], len_year)
    mean['fou'] = ave_a(sea_lsit[3], len_year)
    return mean


def get_qua_ano(data_sea, mean_sea, dateStart, dateEnd):
    mean = mean_sea
    jp_dic = {}
    for sea_key, data in data_sea.items():
        filename = os.path.split(sea_key)[1]
        # print(filename)
        sea_str = str(filename).split('_')[1]
        print('sea_str', sea_str)
        if sea_str == 'fir':
            jp = juping_a(data, mean['fir'])
        elif sea_str == 'sec':
            jp = juping_a(data, mean['sec'])
        elif sea_str == 'thr':
            jp = juping_a(data, mean['thr'])
        elif sea_str == 'fou':
            jp = juping_a(data, mean['fou'])
        jp_dic[sea_key] = jp
    return jp_dic


def get_qua_ano_a(data_sea, mean_sea, dateStart, dateEnd):
    mean = mean_sea
    jp_dic = {}
    for sea_key, data in data_sea.items():
        filename = os.path.split(sea_key)[1]
        # print(filename)
        sea_str = str(filename).split('_')[1]
        print('sea_str', sea_str)
        if sea_str == 'fir':
            jp = juping_a(data, mean['fir'])
        elif sea_str == 'sec':
            jp = juping_a(data, mean['sec'])
        elif sea_str == 'thr':
            jp = juping_a(data, mean['thr'])
        elif sea_str == 'fou':
            jp = juping_a(data, mean['fou'])
        jp_dic[sea_key] = jp
    return jp_dic


def get_hdf_list_data(files_list, path, dataType):  # R
    hdf_data_list = []
    a = 0
    for hdf in files_list:
        a += 1
        data_path = os.path.join(path, dataType)
        file_hdf = '{}/{}'.format(data_path, hdf)
        print('file_hdf', file_hdf, dataType)
        data = get_hdf5_data(file_hdf, dataType, 1, 0, [0, np.inf], np.nan)
        # print(file_hdf, data)
        if a == 1:
            dem = DemLoader()
            dem.file_hdf = file_hdf
            lon, lat = dem.get_lon_lat()
        # print(lon, lat)
        hdf_data_list.append(data)
    return hdf_data_list, lon, lat


def get_hdf_dic_data_point(files_dic, path, dataType, row, col):  # R
    hdf_data_dic = {}
    a = 0
    for sea_key, hdf in files_dic.items():
        a += 1
        data_path = os.path.join(path, dataType)
        file_hdf = '{}/{}'.format(data_path, hdf)
        print('file_hdf', file_hdf, dataType)
        data = get_hdf5_data(file_hdf, dataType, 1, 0, [0, np.inf], np.nan)
        # print(file_hdf, data)
        if a == 1:
            dem = DemLoader()
            dem.file_hdf = file_hdf
            lon, lat = dem.get_lon_lat()
        # print(lon, lat)
        hdf_data_dic[sea_key] = data[(row, col)]
    return hdf_data_dic, lon[(row, col)], lat[(row, col)]


def get_hdf_dic_data_area(files_dic, path, dataType, row_min, row_max, col_min,
                          col_max):  # R
    hdf_data_dic = {}
    a = 0
    for sea_key, hdf in files_dic.items():
        a += 1
        data_path = os.path.join(path, dataType)
        file_hdf = '{}/{}'.format(data_path, hdf)
        print('file_hdf', file_hdf, dataType)
        data = get_hdf5_data(file_hdf, dataType, 1, 0, [0, np.inf], np.nan)
        # print(file_hdf, data)
        if a == 1:
            dem = DemLoader()
            dem.file_hdf = file_hdf
            lon, lat = dem.get_lon_lat()
        # print(lon, lat)
        hdf_data_dic[sea_key] = data[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    lon = lon[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    lat = lat[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    return hdf_data_dic, lon, lat


def get_hdf_list_data_and_sum(files_list, path, data_type):  # R
    hdf_data_list = []
    a = 0
    for hdf in files_list:
        a += 1
        data_path = os.path.join(path, data_type)
        file_hdf = '{}/{}'.format(data_path, hdf)
        print('file_hdf', file_hdf, data_type)
        data = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
        # print(file_hdf, data)
        if a == 1:
            dem = DemLoader()
            dem.file_hdf = file_hdf
            lon, lat = dem.get_lon_lat()
        # print(lon, lat)
        hdf_data_list.append(data)
    sum_year = year_sum(hdf_data_list)
    return sum_year, lon, lat


def get_year_data(date_start, date_end, data_path, data_type):  # R
    '''
    :return: { year:data} lon lat
    '''
    files = get_date_start_end(date_start, date_end, data_type)  # 获取月数据分组文件
    results_return = dict()
    for year, files_one_year in get_file_year(files).items():
        sum_year, lon, lat = get_hdf_list_data_and_sum(files_one_year, data_path, data_type)
        results_return[year] = sum_year
        # write_hdf5_and_compress()
    return results_return, lon, lat


def get_sea_data(date_start, date_end, dataType, row, col):  # R
    '''
    :return:  {key:data}
    '''
    files = get_date_start_end(date_start, date_end, dataType)
    results_return = dict()
    i = 0
    for year, files_one_year in get_file_sea(files, date_start,
                                             date_end, ).items():  # {year:{{'MAM':{mon:file},'JJA':{},'SON':{},'DJF':{}}}'
        for sea, mon_dic in files_one_year.items():
            a = 0
            for mon, file in mon_dic.items():
                a += 1
                i += 1
                data_path = os.path.join(DATA_1KM_MONTH, dataType)
                file_hdf = '{}/{}'.format(data_path, file)
                print('file_hdf', file_hdf, dataType)
                data = get_hdf5_data(file_hdf, dataType, 1, 0, [0, np.inf], np.nan)
                if a != 1:
                    sea_sum = sea_sum + data
                else:
                    sea_sum = data
                if i == 1:
                    dem = DemLoader()
                    dem.file_hdf = file_hdf
                    lons, lats = dem.get_lon_lat()
            sea_key = '{}_{}'.format(year, sea)
            results_return[sea_key] = sea_sum[(row, col)]
            data_area_to_hdf(DATA_1KM, {sea_key: sea_sum}, lons, lats, dataType, 'SEASON/{}'.format(dataType))
        # write_hdf5_and_compress()
    return results_return, lons[(row, col)], lats[(row, col)]


def get_sea_data_area(date_start, date_end, dataType, row_min, row_max, col_min,
                      col_max):  # R
    '''
    :return:  {key:data}
    '''
    files = get_date_start_end(date_start, date_end, dataType)
    results_return = dict()
    i = 0
    for year, files_one_year in get_file_sea(files, date_start,
                                             date_end, ).items():  # {year:{{'MAM':{mon:file},'JJA':{},'SON':{},'DJF':{}}}'
        for sea, mon_dic in files_one_year.items():
            a = 0
            for mon, file in mon_dic.items():
                a += 1
                i += 1
                data_path = os.path.join(DATA_1KM_MONTH, dataType)
                file_hdf = '{}/{}'.format(data_path, file)
                print('file_hdf', file_hdf, dataType)
                data = get_hdf5_data(file_hdf, dataType, 1, 0, [0, np.inf], np.nan)
                if a != 1:
                    sea_sum = sea_sum + data
                else:
                    sea_sum = data
                if i == 1:
                    dem = DemLoader()
                    dem.file_hdf = file_hdf
                    lons, lats = dem.get_lon_lat()
            sea_key = '{}_{}'.format(year, sea)
            results_return[sea_key] = sea_sum[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
            data_area_to_hdf(DATA_1KM, {sea_key: sea_sum}, lons, lats, dataType, 'SEASON/{}'.format(dataType))
        # write_hdf5_and_compress()
    lon = lons[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    lat = lats[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    return results_return, lon, lat


def get_qua_data(date_start, date_end, dataType, row, col):  # R
    '''
    :return:  {key:data}
    '''
    files = get_date_start_end(date_start, date_end, dataType)
    results_return = dict()
    i = 0
    for year, files_one_year in get_file_qua(files, date_start, date_end).items():
        for sea, mon_dic in files_one_year.items():
            a = 0
            for mon, file in mon_dic.items():
                a += 1
                i += 1
                data_path = os.path.join(DATA_1KM_MONTH, dataType)
                file_hdf = '{}/{}'.format(data_path, file)
                print('file_hdf', file_hdf, dataType)
                data = get_hdf5_data(file_hdf, dataType, 1, 0, [0, np.inf], np.nan)
                if a != 1:
                    sea_sum = sea_sum + data
                else:
                    sea_sum = data
                if i == 1:
                    dem = DemLoader()
                    dem.file_hdf = file_hdf
                    lons, lats = dem.get_lon_lat()
            sea_key = '{}_{}'.format(year, sea)
            results_return[sea_key] = sea_sum[(row, col)]
            data_area_to_hdf(DATA_1KM, {sea_key: sea_sum}, lons, lats, dataType, 'QUARTER/{}'.format(dataType))
        # write_hdf5_and_compress()
    return results_return, lons[(row, col)], lats[(row, col)]


def get_qua_data_area(date_start, date_end, dataType, row_min, row_max, col_min,
                      col_max):  # R
    '''
    :return:  {key:data}
    '''
    files = get_date_start_end(date_start, date_end, dataType)
    results_return = dict()
    i = 0
    for year, files_one_year in get_file_qua(files, date_start, date_end).items():
        for sea, mon_dic in files_one_year.items():
            a = 0
            for mon, file in mon_dic.items():
                a += 1
                i += 1
                data_path = os.path.join(DATA_1KM_MONTH, dataType)
                file_hdf = '{}/{}'.format(data_path, file)
                print('file_hdf', file_hdf, dataType)
                data = get_hdf5_data(file_hdf, dataType, 1, 0, [0, np.inf], np.nan)
                if a != 1:
                    sea_sum = sea_sum + data
                else:
                    sea_sum = data
                if i == 1:
                    dem = DemLoader()
                    dem.file_hdf = file_hdf
                    lons, lats = dem.get_lon_lat()
            sea_key = '{}_{}'.format(year, sea)
            results_return[sea_key] = sea_sum[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
            data_area_to_hdf(DATA_1KM, {sea_key: sea_sum}, lons, lats, dataType, 'QUARTER/{}'.format(dataType))
        # write_hdf5_and_compress()
    lon = lons[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    lat = lats[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    return results_return, lon, lat


def get_data(date_start, date_end, data_type):
    '''
    :return: { year:{'data': data,}}
    '''
    d_t = get_datatype()
    if data_type in d_t:
        datapath = "{}".format(data_type)
    else:
        return '数据类型错误！'
    # data_path = 'D:\project\py\gz\ky\gffp\\aid\data\{}'.format(data_type)
    data_path = os.path.join(DATA_1KM, data_type)
    files = get_date_start_end(date_start, date_end, data_type)
    results_return = dict()
    for year, files_one_year in get_file_year(files).items():
        data, lon, lat = get_hdf_list_data(files_one_year, data_path, data_type)
        result = {'data': data, }
        results_return[year] = result
        # write_hdf5_and_compress()
    return results_return, lon, lat


def data_point_to_txt(path, data, dataType):  # R
    '''
    :param data:  dict{key:data}
    '''
    data_str = ''
    kg = '  '
    hh = '\n'
    print(data)
    print(type(data))
    for ad, da in data.items():
        data_str = data_str + str(ad) + kg + str(da) + hh
    # 输出txt
    date_str = datetime.now().strftime("%Y%m%d%H%M%S")
    path = os.path.join(path, date_str)
    dirs = os.path.join(path, 'Point_{}.txt'.format(dataType))
    if not os.path.exists(path):
        os.makedirs(path)
    print('finish{}'.format(path))
    with open(dirs, "w", encoding='utf-8') as f:
        f.write(data_str)


def data_area_to_hdf(path, data, lons, lats, dataType, date_str):  # R
    '''
    :param data: dict{key:data}
    '''
    for ad, da in data.items():
        datas = {}
        datas[dataType] = da
        datas['lon'] = lons
        datas['lat'] = lats
        path_out = os.path.join(path, date_str)
        dirs = os.path.join(path_out, '{}_{}.hdf'.format(dataType, ad))
        if not os.path.exists(path_out):
            os.makedirs(path_out)
        write_hdf5_and_compress(datas, dirs)
        print('finish{}'.format(path_out))


def num_point(dataType, taskChoice, dateStart, dateEnd, leftLongitude, leftLatitude, out_fi=1):
    # 选择数据类型
    d_t = get_datatype()
    if dataType in d_t:
        datapath = "{}".format(dataType)
    else:
        return '数据类型错误！'
    task = task_dict[str(taskChoice)]
    print(dataType)
    print(task)
    row, col = get_point_index_by_lon_lat(leftLongitude, leftLatitude)
    print(row, col)
    if task == 1:
        print('任务：', taskChoice)
        # get file , for read , num add
        y_l = judge_file(dateStart, dateEnd, dataType, 'year')
        if y_l == 0:
            data_year, lons, lats = get_year_data(dateStart, dateEnd, DATA_1KM_MONTH, dataType)
            print('data_year:', data_year)
            data_area_to_hdf(DATA_1KM, data_year, lons, lats, dataType, 'YEAR/{}'.format(dataType))
        else:
            path = DATA_1KM_YEAR
            hdf_data_list, lons, lats = get_hdf_list_data(y_l, path, dataType)
            data_year = {}
            for year in np.arange(int(dateStart), int(dateEnd) + 1):
                a = 0
                data_year[year] = hdf_data_list[a]
                a += 1
        years_sum = years_sum_point(dateStart, dateEnd, data_year, row, col)  # 获取年值
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, years_sum, dataType)  # 输出至txt
        return years_sum, lons[(row, col)], lats[(row, col)]
    elif task == 2:
        print('任务：', taskChoice)
        y_l = judge_file(dateStart, dateEnd, dataType, 'year')
        if y_l == 0:
            data_year_2, lons, lats = get_year_data(dateStart, dateEnd, DATA_1KM_MONTH, dataType)
            print('data_year:', data_year_2)
            data_area_to_hdf(DATA_1KM, data_year_2, lons, lats, dataType, 'YEAR/{}'.format(dataType))
            years_sum = years_sum_point(dateStart, dateEnd, data_year_2, row, col)  # 获取年值
            year_data_list = []
            for key_, year_sum in years_sum.items():
                year_data_list.append(year_sum)
            year_ave = ave_a(year_data_list, int(dateEnd) - int(dateStart) + 1)
        else:
            path = DATA_1KM_YEAR
            hdf_data_list, lons, lats = get_hdf_list_data(y_l, path, dataType)
            year_data_list = []
            for da in hdf_data_list:
                year_data_list.append(da[(row, col)])
            year_ave = ave_a(year_data_list, int(dateEnd) - int(dateStart) + 1)
        year_ave_dict = {'yearMean': year_ave, }
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, year_ave_dict, dataType)  # 输出至txt
        return year_ave_dict, lons[(row, col)], lats[(row, col)]
    elif task == 3:
        print('任务：', taskChoice)
        y_l = judge_file(dateStart, dateEnd, dataType, 'year')
        if y_l == 0:
            data_year_2, lons, lats = get_year_data(dateStart, dateEnd, DATA_1KM_MONTH, dataType)
            print('data_year:', data_year_2)
            data_area_to_hdf(DATA_1KM, data_year_2, lons, lats, dataType, 'YEAR/{}'.format(dataType))
            years_sum = years_sum_point(dateStart, dateEnd, data_year_2, row, col)  # 获取年值
            year_data_list = []
            for key_, year_sum in years_sum.items():
                year_data_list.append(year_sum)
            year_ave = ave_a(year_data_list, int(dateEnd) - int(dateStart) + 1)
            year_jp_all_dict = {}
            for year, year_sum in years_sum.items():
                year_jp_all = juping_a(year_sum, year_ave)
                year_jp_all_dict[year] = year_jp_all
        else:
            path = DATA_1KM_YEAR
            hdf_data_list, lons, lats = get_hdf_list_data(y_l, path, dataType)
            year_data_list = []
            for da in hdf_data_list:
                year_data_list.append(da[(row, col)])
            year_ave = ave_a(year_data_list, int(dateEnd) - int(dateStart) + 1)
            year_jp_all_dict = {}
            for year in np.arange(int(dateStart), int(dateEnd) + 1):
                a = 0
                year_s_d = hdf_data_list[a][(row, col)]
                year_jp_all = juping_a(year_s_d, year_ave)
                year_jp_all_dict[year] = year_jp_all
                a += 1
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, year_jp_all_dict, dataType)
        return year_jp_all_dict, lons[(row, col)], lats[(row, col)]
    elif task == 4:
        print('任务：', taskChoice)
        m_l = get_date_start_end(dateStart, dateEnd, dataType)
        mon_data_point, lon, lat = get_mon_data_point(m_l, DATA_1KM_MONTH, dataType, row, col)
        # get_file_mon(m_l)
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, mon_data_point, dataType)  # 输出至txt
        return mon_data_point, lon, lat
    elif task == 5:
        print('任务：', taskChoice)
        m_l = get_date_start_end(dateStart, dateEnd, dataType)
        mon_data_point, lon, lat = get_mon_mean_data_point(m_l, DATA_1KM_MONTH, dataType, row, col)
        # get_file_mon(m_l)
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, mon_data_point, dataType)  # 输出至txt
        return mon_data_point, lon, lat
    elif task == 6:
        print('任务：', taskChoice)
        m_l = get_date_start_end(dateStart, dateEnd, dataType)
        mon_ano_data_point, lon, lat = get_mon_ano_data_point(m_l, DATA_1KM_MONTH, dataType, row, col)
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, mon_ano_data_point, dataType)
        return mon_ano_data_point, lon, lat
    elif task == 7:
        print('任务：', taskChoice)
        s_l = judge_file(dateStart, dateEnd, dataType, 'sea')
        if s_l == 0:
            data_sea, lon, lat = get_sea_data(dateStart, dateEnd, dataType, row, col)
            print('data_sea:', data_sea)
        else:
            path = DATA_1KM_SEASON
            sea_file_dic = get_file_sea_hdf(s_l)
            data_sea, lon, lat = get_hdf_dic_data_point(sea_file_dic, path, dataType, row, col)
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, data_sea, dataType)  # 输出至txt
        return data_sea, lon, lat
    elif task == 8:
        print('任务：', taskChoice)
        s_l = judge_file(dateStart, dateEnd, dataType, 'sea')
        if s_l == 0:
            data_sea, lon, lat = get_sea_data(dateStart, dateEnd, dataType, row, col)
            print('data_sea:', data_sea)
        else:
            path = DATA_1KM_SEASON
            sea_file_dic = get_file_sea_hdf(s_l)
            data_sea, lon, lat = get_hdf_dic_data_point(sea_file_dic, path, dataType, row, col)
        mean_sea = get_sea_mean(data_sea, dateStart, dateEnd)
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, mean_sea, dataType)  # 输出至txt
        return mean_sea, lon, lat
    elif task == 9:
        print('任务：', taskChoice)
        s_l = judge_file(dateStart, dateEnd, dataType, 'sea')
        if s_l == 0:
            data_sea, lon, lat = get_sea_data(dateStart, dateEnd, dataType, row, col)
            print('data_sea:', data_sea)
        else:
            path = DATA_1KM_SEASON
            sea_file_dic = get_file_sea_hdf(s_l)
            data_sea, lon, lat = get_hdf_dic_data_point(sea_file_dic, path, dataType, row, col)
        mean_sea = get_sea_mean(data_sea, dateStart, dateEnd)
        ano_sea = get_sea_ano(data_sea, mean_sea, dateStart, dateEnd)
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, ano_sea, dataType)  # 输出至txt
        return ano_sea, lon, lat
    elif task == 10:
        print('任务：', taskChoice)
        q_l = judge_file(dateStart, dateEnd, dataType, 'qua')
        if q_l == 0:
            data_qua, lon, lat = get_qua_data(dateStart, dateEnd, dataType, row, col)
            print('data_qua:', data_qua)
        else:
            path = DATA_1KM_QUARTER
            qua_file_dic = get_file_qua_hdf(q_l)
            data_qua, lon, lat = get_hdf_dic_data_point(qua_file_dic, path, dataType, row, col)
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, data_qua, dataType)  # 输出至txt
        return data_qua, lon, lat
    elif task == 11:
        print('任务：', taskChoice)
        q_l = judge_file(dateStart, dateEnd, dataType, 'qua')
        if q_l == 0:
            data_qua, lon, lat = get_qua_data(dateStart, dateEnd, dataType, row, col)
            print('data_qua:', data_qua)
        else:
            path = DATA_1KM_QUARTER
            qua_file_dic = get_file_qua_hdf(q_l)
            data_qua, lon, lat = get_hdf_dic_data_point(qua_file_dic, path, dataType, row, col)
        mean_qua = get_qua_mean(data_qua, dateStart, dateEnd)
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, mean_qua, dataType)  # 输出至txt
        return mean_qua, lon, lat
    elif task == 12:
        print('任务：', taskChoice)
        q_l = judge_file(dateStart, dateEnd, dataType, 'qua')
        if q_l == 0:
            data_qua, lon, lat = get_qua_data(dateStart, dateEnd, dataType, row, col)
            print('data_qua:', data_qua)
        else:
            path = DATA_1KM_QUARTER
            qua_file_dic = get_file_qua_hdf(q_l)
            data_qua, lon, lat = get_hdf_dic_data_point(qua_file_dic, path, dataType, row, col)
        mean_qua = get_qua_mean(data_qua, dateStart, dateEnd)
        ano_qua = get_qua_ano(data_qua, mean_qua, dateStart, dateEnd)
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, ano_qua, dataType)  # 输出至txt
        return ano_qua, lon, lat


def num_area(dataType, taskChoice, dateStart, dateEnd, leftLongitude, leftLatitude,
             rightLongitude, rightLatitude, out_fi=1):
    d_t = get_datatype()
    task = task_dict[str(taskChoice)]
    if dataType in d_t:
        datapath = "{}".format(dataType)
    else:
        return '数据类型错误！'
    print(dataType)
    date_str = datetime.now().strftime("%Y%m%d%H%M%S")
    loa = np.array([float(leftLongitude), float(leftLatitude), float(rightLongitude), float(rightLatitude)])
    # row_min, row_max, col_min, col_max = get_area_index_by_lon_lat(loa[0], loa[1], loa[2], loa[3])  # !!！
    row_min, col_min = get_point_index_by_lon_lat(loa[0], loa[1])
    row_max, col_max = get_point_index_by_lon_lat(loa[2], loa[3])
    print('行列：', row_min, row_max, col_min, col_max)
    if task == 1:
        print('任务：', taskChoice)
        # get file , for read , num add
        y_l = judge_file(dateStart, dateEnd, dataType, 'year')
        if y_l == 0:
            data_year, lons, lats = get_year_data(dateStart, dateEnd, DATA_1KM_MONTH, dataType)
            print('data_year:', data_year)
            data_area_to_hdf(DATA_1KM, data_year, lons, lats, dataType, 'YEAR/{}'.format(dataType))
        else:
            path = DATA_1KM_YEAR
            hdf_data_list, lons, lats = get_hdf_list_data(y_l, path, dataType)
            data_year = {}
            for year in np.arange(int(dateStart), int(dateEnd) + 1):
                a = 0
                data_year[year] = hdf_data_list[a]
                a += 1
        years_sum = years_sum_area(dateStart, dateEnd, data_year, row_min, row_max, col_min, col_max)
        lon = lons[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
        lat = lats[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, years_sum, lon, lat, dataType, date_str)
        return years_sum, lon, lat
    elif task == 2:
        print('任务：', taskChoice)
        # get file , for read , num add
        y_l = judge_file(dateStart, dateEnd, dataType, 'year')
        if y_l == 0:
            data_year, lons, lats = get_year_data(dateStart, dateEnd, DATA_1KM_MONTH, dataType)
            print('data_year:', data_year)
            data_area_to_hdf(DATA_1KM, data_year, lons, lats, dataType, 'YEAR/{}'.format(dataType))
        else:
            path = DATA_1KM_YEAR
            hdf_data_list, lons, lats = get_hdf_list_data(y_l, path, dataType)
            data_year = {}
            for year in np.arange(int(dateStart), int(dateEnd) + 1):
                a = 0
                data_year[year] = hdf_data_list[a]
                a += 1
        years_sum = years_sum_area(dateStart, dateEnd, data_year, row_min, row_max, col_min, col_max)
        lon = lons[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
        lat = lats[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
        year_data_list = []
        for key_, year_sum in years_sum.items():
            year_data_list.append(year_sum)
        year_area_ave = ave_a(year_data_list, int(dateEnd) - int(dateStart) + 1)
        year_area_ave_dict = {'yearMean': year_area_ave, }
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, year_area_ave_dict, lon, lat, dataType, date_str)
        return year_area_ave_dict, lon, lat
    elif task == 3:
        print('任务：', taskChoice)
        # get file , for read , num add
        y_l = judge_file(dateStart, dateEnd, dataType, 'year')
        if y_l == 0:
            data_year, lons, lats = get_year_data(dateStart, dateEnd, DATA_1KM_MONTH, dataType)
            print('data_year:', data_year)
            data_area_to_hdf(DATA_1KM, data_year, lons, lats, dataType, 'YEAR/{}'.format(dataType))
        else:
            path = DATA_1KM_YEAR
            hdf_data_list, lons, lats = get_hdf_list_data(y_l, path, dataType)
            data_year = {}
            for year in np.arange(int(dateStart), int(dateEnd) + 1):
                a = 0
                data_year[year] = hdf_data_list[a]
                a += 1
        years_sum = years_sum_area(dateStart, dateEnd, data_year, row_min, row_max, col_min, col_max)
        lon = lons[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
        lat = lats[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
        year_data_list = []
        for key_, year_sum in years_sum.items():
            year_data_list.append(year_sum)
        year_area_ave = ave_a(year_data_list, int(dateEnd) - int(dateStart) + 1)
        year_jp_all_dict = {}
        for year, year_sum in years_sum.items():
            year_jp_all = juping_a(year_sum, year_area_ave)
            year_jp_all_dict[year] = year_jp_all
            year_jp_dict = {'yearAnomaly': year_jp_all, }
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, year_jp_all_dict, lon, lat, dataType, date_str)
        return year_jp_all_dict, lon, lat
    elif task == 4:
        print('任务：', taskChoice)
        m_l = get_date_start_end(dateStart, dateEnd, dataType)
        mon_data_area, lon, lat = get_mon_data_area(m_l, DATA_1KM_MONTH, dataType, row_min, row_max, col_min, col_max)
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, mon_data_area, lon, lat, dataType, date_str)
        return mon_data_area, lon, lat
    elif task == 5:
        print('任务：', taskChoice)
        m_l = get_date_start_end(dateStart, dateEnd, dataType)
        mon_data_area, lon, lat = get_mon_mean_data_area(m_l, DATA_1KM_MONTH, dataType, row_min, row_max, col_min,
                                                         col_max)
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, mon_data_area, lon, lat, dataType, date_str)
        return mon_data_area, lon, lat
    elif task == 6:
        print('任务：', taskChoice)
        m_l = get_date_start_end(dateStart, dateEnd, dataType)
        mon_ano_data_area, lon, lat = get_mon_ano_data_area(m_l, DATA_1KM_MONTH, dataType, row_min, row_max, col_min,
                                                            col_max)
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, mon_ano_data_area, lon, lat, dataType, date_str)
        return mon_ano_data_area, lon, lat
    elif task == 7:
        print('任务：', taskChoice)
        s_l = judge_file(dateStart, dateEnd, dataType, 'sea')
        if s_l == 0:
            data_sea, lon, lat = get_sea_data_area(dateStart, dateEnd, dataType, row_min, row_max, col_min,
                                                   col_max)
            print('data_sea:', data_sea)
        else:
            path = DATA_1KM_SEASON
            sea_file_dic = get_file_sea_hdf(s_l)
            data_sea, lon, lat = get_hdf_dic_data_area(sea_file_dic, path, dataType, row_min, row_max, col_min,
                                                       col_max)
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, data_sea, lon, lat, dataType, date_str)
        return data_sea, lon, lat
    elif task == 8:
        print('任务：', taskChoice)
        s_l = judge_file(dateStart, dateEnd, dataType, 'sea')
        if s_l == 0:
            data_sea, lon, lat = get_sea_data_area(dateStart, dateEnd, dataType, row_min, row_max, col_min,
                                                   col_max)
            print('data_sea:', data_sea)
        else:
            path = DATA_1KM_SEASON
            sea_file_dic = get_file_sea_hdf(s_l)
            data_sea, lon, lat = get_hdf_dic_data_area(sea_file_dic, path, dataType, row_min, row_max, col_min,
                                                       col_max)
        mean_sea = get_sea_mean_area(data_sea, dateStart, dateEnd)
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, mean_sea, lon, lat, dataType, date_str)
        return mean_sea, lon, lat
    elif task == 9:
        print('任务：', taskChoice)
        s_l = judge_file(dateStart, dateEnd, dataType, 'sea')
        if s_l == 0:
            data_sea, lon, lat = get_sea_data_area(dateStart, dateEnd, dataType, row_min, row_max, col_min,
                                                   col_max)
            print('data_sea:', data_sea)
        else:
            path = DATA_1KM_SEASON
            sea_file_dic = get_file_sea_hdf(s_l)
            data_sea, lon, lat = get_hdf_dic_data_area(sea_file_dic, path, dataType, row_min, row_max, col_min,
                                                       col_max)
        mean_sea = get_sea_mean_area(data_sea, dateStart, dateEnd)
        ano_sea = get_sea_ano_area(data_sea, mean_sea, dateStart, dateEnd)
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, ano_sea, lon, lat, dataType, date_str)
        return ano_sea, lon, lat
    elif task == 10:
        print('任务：', taskChoice)
        q_l = judge_file(dateStart, dateEnd, dataType, 'qua')
        if q_l == 0:
            data_qua, lon, lat = get_qua_data(dateStart, dateEnd, dataType, row_min, row_max, col_min,
                                              col_max)
            print('data_qua:', data_qua)
        else:
            path = DATA_1KM_QUARTER
            qua_file_dic = get_file_qua_hdf(q_l)
            data_qua, lon, lat = get_hdf_dic_data_area(qua_file_dic, path, dataType, row_min, row_max, col_min,
                                                       col_max)
        print(data_qua)
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, data_qua, lon, lat, dataType, date_str)
        return data_qua, lon, lat
    elif task == 11:
        print('任务：', taskChoice)
        q_l = judge_file(dateStart, dateEnd, dataType, 'qua')
        if q_l == 0:
            data_qua, lon, lat = get_qua_data(dateStart, dateEnd, dataType, row_min, row_max, col_min,
                                              col_max)
            print('data_qua:', data_qua)
        else:
            path = DATA_1KM_QUARTER
            qua_file_dic = get_file_qua_hdf(q_l)
            data_qua, lon, lat = get_hdf_dic_data_area(qua_file_dic, path, dataType, row_min, row_max, col_min,
                                                       col_max)
        mean_qua = get_qua_mean_a(data_qua, dateStart, dateEnd)
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, mean_qua, lon, lat, dataType, date_str)
        return mean_qua, lon, lat
    elif task == 12:
        print('任务：', taskChoice)
        q_l = judge_file(dateStart, dateEnd, dataType, 'qua')
        if q_l == 0:
            data_qua, lon, lat = get_qua_data(dateStart, dateEnd, dataType, row_min, row_max, col_min,
                                              col_max)
            print('data_qua:', data_qua)
        else:
            path = DATA_1KM_QUARTER
            qua_file_dic = get_file_qua_hdf(q_l)
            data_qua, lon, lat = get_hdf_dic_data_area(qua_file_dic, path, dataType, row_min, row_max, col_min,
                                                       col_max)
        mean_qua = get_qua_mean_a(data_qua, dateStart, dateEnd)
        ano_qua = get_qua_ano_a(data_qua, mean_qua, dateStart, dateEnd)
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, ano_qua, lon, lat, dataType, date_str)
        return ano_qua, lon, lat


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
                        help='时间：year, month, season, quarter   '
                             '任务: sum, mean, anomaly  '
                             'yearSum, yearMean, yearAnomaly,'
                             'monthSum, monthMean, monthAnomaly,'
                             'seasonSum, seasonMean, seasonAnomaly,'
                             'quarterSum, quarterMean, quarterAnomaly,',
                        required=False)
    parser.add_argument('--dateStart', '-s', help='开始年份，YYYY(2019)', required=True)
    parser.add_argument('--dateEnd', '-e', help='结束年份，YYYY(2019)', required=True)
    parser.add_argument('--leftLongitude', '-l', help='经度或左上角经度，47.302235', required=True)
    parser.add_argument('--leftLatitude', '-a', help='纬度或左上角纬度，85.880519', required=True)
    parser.add_argument('--rightLongitude', '-r', help='右下角经度，47.302235', required=False)
    parser.add_argument('--rightLatitude', '-i', help='右下角经度，85.880519', required=False)
    args = parser.parse_args()

    if args.modeType == 'point':
        print(args.dataType, args.taskChoice, args.dateStart, args.dateEnd, args.leftLongitude, args.leftLatitude)
        n_p = num_point(args.dataType, args.taskChoice, args.dateStart, args.dateEnd, args.leftLongitude,
                        args.leftLatitude)
        # print(n_p)
    elif args.modeType == 'area':
        print(args.dataType, args.taskChoice, args.dateStart, args.dateEnd, args.leftLongitude,
              args.leftLatitude, args.rightLongitude, args.rightLatitude)
        n_a = num_area(args.dataType, args.taskChoice, args.dateStart, args.dateEnd, args.leftLongitude,
                       args.leftLatitude, args.rightLongitude, args.rightLatitude)
        # print(n_a)
    # python3 a04_data_statistics.py -t GHI -m point  -c yearSum -s 2019 -e 2019 -l 113 -a 43
    # python3 a04_data_statistics.py -t GHI -m point  -c yearMean -s 2019 -e 2019 -l 113 -a 43
    # python3 a04_data_statistics.py -t GHI -m point  -c yearAnomaly -s 2019 -e 2019 -l 113 -a 43
    # python3 a04_data_statistics.py -t GHI -m area  -c yearSum -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    # python3 a04_data_statistics.py -t GHI -m area  -c yearMean -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    # python3 a04_data_statistics.py -t GHI -m area  -c yearAnomaly -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    # t_a = data_area('DHI', 6, 1966, 1969, 1966, '70.01509999999999', '10.225', '70.06509999999999',
    #                 '10.034999999999998', 'outdata\hdf')
    # print(t_a)
    # data = get_data(2019, 2019, 'DBI')
    # print(data)
    # # years_sum = years_sum(2019, 2019, data)
    # # print(years_sum)
    # years_sum = years_sum_point(2019, 2019, data, 5, 5)
    # print(years_sum)
    # n_P = num_point('DBI', 1, 2019, 2019,  '70.01509999999999', '10.225', )
    # print(n_P)
