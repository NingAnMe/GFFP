#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020-11-16 15:47
# @Author  : Yan Kaijie
import os
import argparse
from datetime import datetime, date
from warnings import filterwarnings
import numpy as np
import copy
# from schedule import *
from utils.config import get_datatype
from utils.get_index_by_lonlat import get_point_index_by_lon_lat
from utils.data import DemLoader
from utils.hdf5 import get_hdf5_data, write_hdf5_and_compress
from utils.config import DEM_HDF, PRO_MASK_HDF
from collections import defaultdict
from user_config import DATA_1KM, DATA_1KM_MONTH, DATA_1KM_SEASON, DATA_1KM_QUARTER, DATA_1KM_YEAR, DATA_STAT

task_dict = {
    'yearSum': 1, 'yearMean': 2, 'yearAnomaly': 3, 'monthSum': 4, 'monthMean': 5, 'monthAnomaly': 6,
    'seasonSum': 7, 'seasonMean': 8, 'seasonAnomaly': 9, 'quarterSum': 10, 'quarterMean': 11, 'quarterAnomaly': 12
}
'''
1）输入单点经纬度，输出txt格式的数据；
2）输入区域经纬度范围，输出hdf
3）输入省级名称，输出省级数据
4）输入all，输出全国数据
'''


def ave_a(lis, li_len):
    print('开始求平均')
    sum_a = np.ndarray
    # print(sum_a)
    a = 0
    for s in lis:
        a += 1
        if a == 1:
            sum_a = s
        else:
            sum_a = sum_a + s
    # print(sum_a)
    ave_area = sum_a / li_len
    return ave_area


def juping_a(one_year, ave_area):
    # （一年值 - 多年平均值） / 多年平均值 * 100 %
    jp = (one_year - ave_area) / ave_area * 100
    print('距平：', jp)
    return jp


def year_sum(year_data):
    """
    :param year_data: 一年中每个月的数据
    :return: 年值
    """
    num_year = np.zeros((len(year_data[0]), len(year_data[0][0])))
    print('num_year.shape', num_year.shape)
    # 年值
    for mon_data in year_data:
        num_year = num_year + mon_data
    return num_year


def years_sum_point(years_num, row, col):  # R
    """
    :return: {year1 ; 年值,year2 ; 年值}
    """
    print(years_num)
    years_num_p = dict()
    for year, data in years_num.items():
        print(row, col)
        year_point_data = data[(row, col)]
        print(year)
        print('year_point_data', year_point_data)
        years_num_p[year] = year_point_data
    return years_num_p


def years_sum_area(date_start, date_end, data, row_min, row_max, col_min, col_max):  # R
    """
     :return: {year1 ; 年值,year2 ; 年值}
    """
    years_num = data
    print(years_num)
    years_num_p = dict()
    for year in np.arange(int(date_start), int(date_end) + 1):
        year_sum_p = years_num[year]
        year_area_data = year_sum_p[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
        print(year)
        print('year_area_data', year_area_data)
        years_num_p[year] = year_area_data
    return years_num_p


def get_date_start_end(date_start, date_end, data_type):  # R
    """
    获取所需文件列表
    """
    print(date_start, date_end, data_type)
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
        if int(date_start) <= int(file_date) <= int(date_end):
            # print('file_', file_)
            file_list.append(file_)
    if not file_list:
        raise ValueError('No file in {} {}'.format(date_start, date_end))
    # print(file_list)
    print('----------------------------------------------------------------------------')
    return file_list


def judge_file(date_start, date_end, data_type, task_name):  # R
    """
    :param date_start:
    :param date_end:
    :param data_type:
    :param task_name: 'sea' 'qua' 'year' 'mon'
    :return:file_list
    """
    print('judge_file')
    data_path = str
    if task_name == 'sea':
        data_path = os.path.join(DATA_1KM_SEASON, data_type)
    elif task_name == 'qua':
        data_path = os.path.join(DATA_1KM_QUARTER, data_type)
    elif task_name == 'year':
        data_path = os.path.join(DATA_1KM_YEAR, data_type)
    elif task_name == 'mon':
        data_path = os.path.join(DATA_1KM_MONTH, data_type)
    print(date_start, date_end, data_type)
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
    """获取"""
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
    """'
    :return [[{{2020:Feb_file}}][][][][]]
    """
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


def get_mon_data_point(file_list, path, data_type, row, col):  # R
    mon_list = get_file_mon(file_list)
    mon_dic_point = {}
    a = 0
    lon = np.ndarray
    lat = np.ndarray
    for mon_dic in mon_list:
        for y_m_key, hdf in mon_dic[0].items():
            a += 1
            data_path = os.path.join(path, data_type)
            file_hdf = '{}/{}'.format(data_path, hdf)
            print('file_hdf', file_hdf, data_type)
            data = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)[(row, col)]
            mon_dic_point[y_m_key] = data
            # print(file_hdf, data)
            if a == 1:
                dem = DemLoader()
                dem.file_hdf = file_hdf
                lon, lat = dem.get_lon_lat()
            # print(lon, lat)
    print(mon_dic_point, lon[(row, col)], lat[(row, col)])
    return mon_dic_point, lon[(row, col)], lat[(row, col)]


def get_mon_data_area(file_list, path, data_type, row_min, row_max, col_min, col_max):  # R
    mon_list = get_file_mon(file_list)
    mon_dic_point = {}
    a = 0
    lon = np.ndarray
    lat = np.ndarray
    for mon_dic in mon_list:
        for y_m_key, hdf in mon_dic[0].items():
            a += 1
            data_path = os.path.join(path, data_type)
            file_hdf = '{}/{}'.format(data_path, hdf)
            print('file_hdf', file_hdf, data_type)
            data = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
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


def get_mon_mean_data_point(file_list, path, data_type, row, col):  # R
    mon_list = get_file_mon(file_list)
    mon_mean_dic_point = {}
    a = 0
    lon = np.ndarray
    lat = np.ndarray
    for mon in np.arange(1, len(mon_list) + 1):
        mon_dic = mon_list[mon - 1]
        m_sum = 0
        i = 0
        for y_m_key, hdf in mon_dic[0].items():
            a += 1
            i += 1
            data_path = os.path.join(path, data_type)
            file_hdf = '{}/{}'.format(data_path, hdf)
            print('file_hdf', file_hdf, data_type)
            data = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)[(row, col)]
            m_sum = data + m_sum
            if a == 1:
                dem = DemLoader()
                dem.file_hdf = file_hdf
                lon, lat = dem.get_lon_lat()
            # print(lon, lat)
        mon_mean_dic_point[mon] = m_sum / i
    print(mon_mean_dic_point, lon[(row, col)], lat[(row, col)])
    return mon_mean_dic_point, lon[(row, col)], lat[(row, col)]


def get_mon_mean_data_area(file_list, path, data_type, row_min, row_max, col_min, col_max):  # R
    mon_list = get_file_mon(file_list)
    mon_mean_dic_point = {}
    a = 0
    lon = np.ndarray
    lat = np.ndarray
    m_sum = np.ndarray
    for mon in np.arange(1, len(mon_list) + 1):
        mon_dic = mon_list[mon - 1]
        i = 0
        for y_m_key, hdf in mon_dic[0].items():
            a += 1
            i += 1
            data_path = os.path.join(path, data_type)
            file_hdf = '{}/{}'.format(data_path, hdf)
            print('file_hdf', file_hdf, data_type)
            data = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
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


def get_mon_ano_data_point(file_list, mon_mean_dic_point, path, data_type, row, col):  # R
    # 获取月份分组文件
    mon_list = get_file_mon(file_list)
    mon_dic_point = {}
    a = 0
    lon = np.ndarray
    lat = np.ndarray
    for mon in np.arange(1, len(mon_list) + 1):
        mon_dic = mon_list[mon - 1]
        m_sum = 0
        i = 0
        mon_data_dic = {}
        for y_m_key, hdf in mon_dic[0].items():
            a += 1
            i += 1
            data_path = os.path.join(path, data_type)
            file_hdf = '{}/{}'.format(data_path, hdf)
            print('file_hdf', file_hdf, data_type)
            data = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)[(row, col)]
            mon_data_dic[y_m_key] = data
            m_sum = data + m_sum
            if a == 1:
                dem = DemLoader()
                dem.file_hdf = file_hdf
                lon, lat = dem.get_lon_lat()
            # print(lon, lat)
        # print(mon_data_dic)
        for y_m_key2, data in mon_data_dic.items():
            mon_dic_point[y_m_key2] = juping_a(data, mon_mean_dic_point[mon])

    print(mon_dic_point, lon[(row, col)], lat[(row, col)])
    return mon_dic_point, lon[(row, col)], lat[(row, col)]


def get_mon_ano_data_area(file_list, mon_mean_dic_point, path, data_type, row_min, row_max, col_min,
                          col_max):  # R

    mon_list = get_file_mon(file_list)
    mon_dic_point = {}
    a = 0
    lon = np.ndarray
    lat = np.ndarray
    for mon in np.arange(1, len(mon_list) + 1):
        mon_dic = mon_list[mon - 1]
        m_sum = 0
        i = 0
        mon_data_dic = {}
        for y_m_key, hdf in mon_dic[0].items():
            a += 1
            i += 1
            data_path = os.path.join(path, data_type)
            file_hdf = '{}/{}'.format(data_path, hdf)
            print('file_hdf', file_hdf, data_type)
            data = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
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
        # print(mon_data_dic)
        for y_m_key2, data in mon_data_dic.items():
            mon_dic_point[y_m_key2] = juping_a(data, mon_mean_dic_point[mon])
    lon = lon[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    lat = lat[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    print(len(mon_dic_point), lon.shape, lat.shape, row_min, row_max, col_min,
          col_max)
    return mon_dic_point, lon, lat


def get_file_sea(files, date_start, date_end):  # R
    """ {year:{{'MAM':{mon:file},'JJA':{},'SON':{},'DJF':{}}}"""
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


def get_sea_mean(data_sea):
    sea_list = [[], [], [], []]
    for sea_key, data in data_sea.items():
        filename = os.path.split(sea_key)[1]
        # print(filename)
        sea_str = str(filename).split('_')[1]
        print('sea_str', sea_str)
        if sea_str == 'MAM':
            sea_list[0].append(data)
        elif sea_str == 'JJA':
            sea_list[1].append(data)
        elif sea_str == 'SON':
            sea_list[2].append(data)
        elif sea_str == 'DJF':
            sea_list[3].append(data)
    mean = {'MAM': ave_a(sea_list[0], len(sea_list[0])),
            'JJA': ave_a(sea_list[1], len(sea_list[0])),
            'SON': ave_a(sea_list[2], len(sea_list[0]))}
    if sea_list[3]:
        mean['DJF'] = ave_a(sea_list[3], len(sea_list[3]))
    return mean


def get_sea_ano(data_sea, mean_sea):
    mean = mean_sea
    jp_dic = {}
    jp = np.ndarray
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


def get_file_qua(files):  # R
    """ {year:{{'fir':{mon:file},'sec':{},'thr':{},'fou':{}}}"""
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


def get_qua_mean(data_sea):
    sea_list = [[], [], [], []]
    for sea_key, data in data_sea.items():
        filename = os.path.split(sea_key)[1]
        # print(filename)
        sea_str = str(filename).split('_')[1]
        print('sea_str', sea_str)
        if sea_str == 'fir':
            sea_list[0].append(data)
        elif sea_str == 'sec':
            sea_list[1].append(data)
        elif sea_str == 'thr':
            sea_list[2].append(data)
        elif sea_str == 'fou':
            sea_list[3].append(data)
    mean = {
        'fir': ave_a(sea_list[0], len(sea_list[0])),
        'sec': ave_a(sea_list[1], len(sea_list[0])),
        'thr': ave_a(sea_list[2], len(sea_list[0])),
        'fou': ave_a(sea_list[3], len(sea_list[0]))
            }
    return mean


def get_qua_ano(data_sea, mean_sea):
    mean = mean_sea
    jp_dic = {}
    jp = np.ndarray
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
    hdf_data_dic = {}
    a = 0
    lon = np.ndarray
    lat = np.ndarray
    print('files_list', files_list)
    for hdf in files_list:
        a += 1
        year_name = int(hdf[4:8])
        data_path = os.path.join(path, dataType)
        file_hdf = '{}/{}'.format(data_path, hdf)
        print('file_hdf', file_hdf, dataType)
        data = get_hdf5_data(file_hdf, dataType, 1, 0, [0, np.inf], np.nan)
        # print(file_hdf, data)
        print('data.shape', data.shape)
        if a == 1:
            dem = DemLoader()
            dem.file_hdf = file_hdf
            lon, lat = dem.get_lon_lat()
        # print(lon, lat)
        hdf_data_dic[year_name] = data
    return hdf_data_dic, lon, lat


def get_hdf_dic_data(files_list, path, dataType):  # R
    hdf_data_dic = {}
    a = 0
    lon = np.ndarray
    lat = np.ndarray
    for hdf in files_list:
        a += 1
        year_str = hdf[4:8]
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
        hdf_data_dic[year_str] = data
    return hdf_data_dic, lon, lat


def get_hdf_dic_data_point(files_dic, path, dataType, row, col):  # R
    hdf_data_dic = {}
    a = 0
    lon = np.ndarray
    lat = np.ndarray
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
    lon = np.ndarray
    lat = np.ndarray
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
    lon = np.ndarray
    lat = np.ndarray
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


def get_year_data(date_start, date_end, data_path, data_type, year_file_list=None):  # R
    """
    :return: { year:data} lon lat
    """
    lon = np.ndarray
    lat = np.ndarray
    files = get_date_start_end(date_start, date_end, data_type)  # 获取月数据分组文件
    year_str_list = []
    if year_file_list or year_file_list != 0:
        print('已生成的缓存文件：', year_str_list)
        for year_file in year_file_list:
            year_str = year_file[4:8]
            year_str_list.append(year_str)
    results_return = dict()
    for year, files_one_year in get_file_year(files).items():
        if str(year) in year_str_list:
            file_hdf = os.path.join(DATA_1KM_YEAR, '{}/{}_{}.hdf'.format(data_type, data_type, year))
            sum_year = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
            if not isinstance(lon.size, int):
                lon = get_hdf5_data(file_hdf, 'lon', 1, 0, [0, np.inf], np.nan)
                lat = get_hdf5_data(file_hdf, 'lat', 1, 0, [0, np.inf], np.nan)
        else:
            sum_year, lon, lat = get_hdf_list_data_and_sum(files_one_year, data_path, data_type)
        results_return[year] = sum_year
        # write_hdf5_and_compress()
    return results_return, lon, lat


def get_sea_data(date_start, date_end, data_type, row, col, file_list=None):  # R
    """
    :return:  {key:data}
    """
    files = get_date_start_end(date_start, date_end, data_type)
    results_return = dict()
    i = 0
    lon = np.ndarray
    lat = np.ndarray
    str_list = []
    if file_list or file_list != 0:
        print('已生成的缓存文件：', str_list)
        for t_file in file_list:
            name_str = t_file[4:]
            str_list.append(name_str)
    # {year:{{'MAM':{mon:file},'JJA':{},'SON':{},'DJF':{}}}'
    for year, files_one_year in get_file_sea(files, date_start, date_end, ).items():
        for sea, mon_dic in files_one_year.items():
            a = 0
            sea_key = '{}_{}'.format(year, sea)
            file_name = '{}_{}.hdf'.format(year, sea)
            if file_name in str_list:
                file_hdf = os.path.join(DATA_1KM_SEASON, '{}/{}_{}'.format(data_type, data_type, file_name))
                sea_sum = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
                if not isinstance(lon.size, int):
                    lon = get_hdf5_data(file_hdf, 'lon', 1, 0, [0, np.inf], np.nan)
                    lat = get_hdf5_data(file_hdf, 'lat', 1, 0, [0, np.inf], np.nan)
            else:
                sea_sum = np.ndarray
                for mon, file in mon_dic.items():
                    a += 1
                    i += 1
                    data_path = os.path.join(DATA_1KM_MONTH, data_type)
                    file_hdf = '{}/{}'.format(data_path, file)
                    print('file_hdf', file_hdf, data_type)
                    data = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
                    if a != 1:
                        sea_sum = sea_sum + data
                    else:
                        sea_sum = data
                    if not isinstance(lon.size, int):
                        dem = DemLoader()
                        dem.file_hdf = file_hdf
                        lon, lat = dem.get_lon_lat()
                data_area_to_hdf(DATA_1KM, {sea_key: sea_sum}, lon, lat, data_type, 'SEASON/{}'.format(data_type), 0)

            results_return[sea_key] = sea_sum[(row, col)]
        # write_hdf5_and_compress()
    return results_return, lon[(row, col)], lat[(row, col)]


def get_sea_data_area(date_start, date_end, data_type, row_min, row_max, col_min,
                      col_max, file_list=None):  # R
    """
    :return:  {key:data}
    """
    files = get_date_start_end(date_start, date_end, data_type)
    results_return = dict()
    i = 0
    lon = np.ndarray
    lat = np.ndarray
    str_list = []
    if file_list or file_list != 0:
        print('已生成的缓存文件：', str_list)
        for t_file in file_list:
            name_str = t_file[4:]
            str_list.append(name_str)
    # {year:{{'MAM':{mon:file},'JJA':{},'SON':{},'DJF':{}}}'
    for year, files_one_year in get_file_sea(files, date_start, date_end, ).items():
        for sea, mon_dic in files_one_year.items():
            a = 0
            sea_key = '{}_{}'.format(year, sea)
            file_name = '{}_{}.hdf'.format(year, sea)
            if file_name in str_list:
                file_hdf = os.path.join(DATA_1KM_SEASON, '{}/{}_{}'.format(data_type, data_type, file_name))
                sea_sum = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
                if not isinstance(lon.size, int):
                    lon = get_hdf5_data(file_hdf, 'lon', 1, 0, [0, np.inf], np.nan)
                    lat = get_hdf5_data(file_hdf, 'lat', 1, 0, [0, np.inf], np.nan)
            else:
                sea_sum = np.ndarray
                for mon, file in mon_dic.items():
                    a += 1
                    i += 1
                    data_path = os.path.join(DATA_1KM_MONTH, data_type)
                    file_hdf = '{}/{}'.format(data_path, file)
                    print('file_hdf', file_hdf, data_type)
                    data = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
                    if a != 1:
                        sea_sum = sea_sum + data
                    else:
                        sea_sum = data
                    if not isinstance(lon.size, int):
                        dem = DemLoader()
                        dem.file_hdf = file_hdf
                        lon, lat = dem.get_lon_lat()
                data_area_to_hdf(DATA_1KM, {sea_key: sea_sum}, lon, lat, data_type, 'SEASON/{}'.format(data_type), 0)
            results_return[sea_key] = sea_sum[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
        # write_hdf5_and_compress()
    lon = lon[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    lat = lat[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    return results_return, lon, lat


def get_qua_data(date_start, date_end, data_type, row, col, file_list=None):  # R
    """
    :return:  {key:data}
    """
    files = get_date_start_end(date_start, date_end, data_type)
    results_return = dict()
    i = 0
    lon = np.ndarray
    lat = np.ndarray
    str_list = []
    if file_list or file_list != 0:
        print('已生成的缓存文件：', str_list)
        for t_file in file_list:
            name_str = t_file[4:]
            str_list.append(name_str)
    for year, files_one_year in get_file_qua(files).items():
        for sea, mon_dic in files_one_year.items():
            a = 0
            sea_key = '{}_{}'.format(year, sea)
            file_name = '{}_{}.hdf'.format(year, sea)
            if file_name in str_list:
                file_hdf = os.path.join(DATA_1KM_QUARTER, '{}/{}_{}'.format(data_type, data_type, file_name))
                print('file_hdf', file_hdf, data_type)
                sea_sum = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
                if not isinstance(lon.size, int):
                    lon = get_hdf5_data(file_hdf, 'lon', 1, 0, [0, np.inf], np.nan)
                    lat = get_hdf5_data(file_hdf, 'lat', 1, 0, [0, np.inf], np.nan)
            else:
                sea_sum = np.ndarray
                for mon, file in mon_dic.items():
                    a += 1
                    i += 1
                    data_path = os.path.join(DATA_1KM_MONTH, data_type)
                    file_hdf = '{}/{}'.format(data_path, file)
                    print('file_hdf', file_hdf, data_type)
                    data = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
                    if a != 1:
                        sea_sum = sea_sum + data
                    else:
                        sea_sum = data
                    if not isinstance(lon.size, int):
                        dem = DemLoader()
                        dem.file_hdf = file_hdf
                        lon, lat = dem.get_lon_lat()
                data_area_to_hdf(DATA_1KM, {sea_key: sea_sum}, lon, lat, data_type, 'QUARTER/{}'.format(data_type), 0)
            results_return[sea_key] = sea_sum[(row, col)]  # write_hdf5_and_compress()
    return results_return, lon[(row, col)], lat[(row, col)]


def get_qua_data_area(date_start, date_end, data_type, row_min, row_max, col_min,
                      col_max, file_list=None):  # R
    """
    :return:  {key:data}
    """
    files = get_date_start_end(date_start, date_end, data_type)
    results_return = dict()
    i = 0
    lon = np.ndarray
    lat = np.ndarray
    str_list = []
    if file_list or file_list != 0:
        print('已生成的缓存文件：', str_list)
        for t_file in file_list:
            name_str = t_file[4:]
            str_list.append(name_str)
    for year, files_one_year in get_file_qua(files).items():
        for sea, mon_dic in files_one_year.items():
            a = 0
            sea_key = '{}_{}'.format(year, sea)
            file_name = '{}_{}.hdf'.format(year, sea)
            if file_name in str_list:
                file_hdf = os.path.join(DATA_1KM_QUARTER, '{}/{}_{}'.format(data_type, data_type, file_name))
                print('file_hdf', file_hdf, data_type)
                sea_sum = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
                if not isinstance(lon.size, int):
                    lon = get_hdf5_data(file_hdf, 'lon', 1, 0, [0, np.inf], np.nan)
                    lat = get_hdf5_data(file_hdf, 'lat', 1, 0, [0, np.inf], np.nan)
            else:
                sea_sum = np.ndarray
                for mon, file in mon_dic.items():
                    a += 1
                    i += 1
                    data_path = os.path.join(DATA_1KM_MONTH, data_type)
                    file_hdf = '{}/{}'.format(data_path, file)
                    print('file_hdf', file_hdf, data_type)
                    data = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
                    if a != 1:
                        sea_sum = sea_sum + data
                    else:
                        sea_sum = data
                    if not isinstance(lon.size, int):
                        dem = DemLoader()
                        dem.file_hdf = file_hdf
                        lon, lat = dem.get_lon_lat()
                data_area_to_hdf(DATA_1KM, {sea_key: sea_sum}, lon, lat, data_type, 'QUARTER/{}'.format(data_type), 0)
            results_return[sea_key] = sea_sum[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
        # write_hdf5_and_compress()
    lon = lon[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    lat = lat[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
    return results_return, lon, lat


def data_point_to_txt(path, data, dataType):  # R
    """
    :param dataType:
    :param path:
    :param data:  dict{key:data}
    """
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


def data_area_to_hdf(path, data, lons, lats, dataType, date_str, dee=1):  # R
    """
    :param dee:
    :param date_str:
    :param dataType:
    :param lats:
    :param lons:
    :param path:
    :param data: dict{key:data}
    """
    path_out = os.path.join(path, date_str)
    for ad, da in data.items():
        datas = {dataType: da, 'lon': lons, 'lat': lats}
        dirs = os.path.join(path_out, '{}_{}.hdf'.format(dataType, ad))
        if not os.path.exists(path_out):
            os.makedirs(path_out)
        write_hdf5_and_compress(datas, dirs)
    if dee == 1:
        print('finish{}'.format(path_out))


def num_point(dataType, taskChoice, dateStart, dateEnd, left_longitude, left_latitude, out_fi=1, dateAnomaly=0):
    # 选择数据类型
    d_t = get_datatype()
    if dataType not in d_t:
        return '数据类型错误！'
    task = task_dict[str(taskChoice)]
    print(dataType)
    print(task)
    print(left_longitude, left_latitude)
    print(type(left_longitude), type(left_latitude))
    if type(left_longitude) == str:
        left_longitude = float(left_longitude)
        left_latitude = float(left_latitude)
    row, col = get_point_index_by_lon_lat(left_longitude, left_latitude)
    row = row - 1
    col = col - 1
    print(row, col)
    if task in [1, 2, 3]:
        print('任务：', taskChoice)
        y_l = judge_file(dateStart, dateEnd, dataType, 'year')
        if y_l == 0 or len(y_l) < (int(dateEnd) - int(dateStart) + 1):
            data_year_2, lons, lats = get_year_data(dateStart, dateEnd, DATA_1KM_MONTH, dataType, y_l)
            print('data_year:', data_year_2)
            data_year = copy.deepcopy(data_year_2)
            if y_l != 0:
                for year_file in y_l:
                    year_str = year_file[4:8]
                    del data_year[int(year_str)]
            data_area_to_hdf(DATA_1KM, data_year, lons, lats, dataType, 'YEAR/{}'.format(dataType), 0)
            # 获取年值
        else:
            path = DATA_1KM_YEAR
            data_year_2, lons, lats = get_hdf_list_data(y_l, path, dataType)
        years_sum = years_sum_point(data_year_2, row, col)
        if task == 1:
            if out_fi == 1:
                data_point_to_txt(DATA_STAT, years_sum, dataType)  # 输出至txt
            return years_sum, lons[(row, col)], lats[(row, col)]
        year_data_list = []
        for key_, y_sum in years_sum.items():
            year_data_list.append(y_sum)
        year_ave = ave_a(year_data_list, len(years_sum))
        if task == 2:
            year_ave_dict = {'yearMean': year_ave, }
            if out_fi == 1:
                data_point_to_txt(DATA_STAT, year_ave_dict, dataType)  # 输出至txt
            return year_ave_dict, lons[(row, col)], lats[(row, col)]
        year_jp_all_dict = {}
        if int(dateAnomaly) != 0:
            y_l = judge_file(dateAnomaly, dateAnomaly, dataType, 'year')
            if y_l == 0 or len(y_l) < (int(dateAnomaly) - int(dateAnomaly) + 1):
                data_year_2, lons, lats = get_year_data(dateAnomaly, dateAnomaly, DATA_1KM_MONTH, dataType, y_l)
                print('data_year:', data_year_2)
                data_year = copy.deepcopy(data_year_2)
                if y_l != 0:
                    for year_file in y_l:
                        year_str = year_file[4:8]
                        del data_year[int(year_str)]
                data_area_to_hdf(DATA_1KM, data_year, lons, lats, dataType, 'YEAR/{}'.format(dataType), 0)
                # 获取年值
            else:
                path = DATA_1KM_YEAR
                data_year_2, lons, lats = get_hdf_list_data(y_l, path, dataType)
            years_sum = years_sum_point(data_year_2, row, col)
        for year, y_sum in years_sum.items():
            year_jp_all = juping_a(y_sum, year_ave)
            year_jp_all_dict[year] = year_jp_all
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, year_jp_all_dict, dataType)
        return year_jp_all_dict, lons[(row, col)], lats[(row, col)]
    elif task in [4, 5, 6]:
        print('任务：', taskChoice)
        m_l = judge_file(dateStart, dateEnd, dataType, 'mon')
        # get_file_mon(m_l)
        if task == 4:
            mon_data_point, lon, lat = get_mon_data_point(m_l, DATA_1KM_MONTH, dataType, row, col)
            if out_fi == 1:
                data_point_to_txt(DATA_STAT, mon_data_point, dataType)  # 输出至txt
            return mon_data_point, lon, lat
        mon_mean_data_point, lon, lat = get_mon_mean_data_point(m_l, DATA_1KM_MONTH, dataType, row, col)
        if task == 5:
            if out_fi == 1:
                data_point_to_txt(DATA_STAT, mon_mean_data_point, dataType)  # 输出至txt
            return mon_mean_data_point, lon, lat
        if int(dateAnomaly) != 0:
            m_l = get_date_start_end(dateAnomaly, dateAnomaly, dataType)
        mon_ano_data_point, lon, lat = get_mon_ano_data_point(m_l, mon_mean_data_point, DATA_1KM_MONTH, dataType, row,
                                                              col)
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, mon_ano_data_point, dataType)
        return mon_ano_data_point, lon, lat
    elif task in [7, 8, 9]:
        print('任务：', taskChoice)
        s_l = judge_file(dateStart, dateEnd, dataType, 'sea')
        if s_l == 0 or len(s_l) < (int(dateEnd) - int(dateStart) + 1) * 4 - 1:
            data_sea, lon, lat = get_sea_data(dateStart, dateEnd, dataType, row, col, s_l)
            print('data_sea:', data_sea)
        else:
            path = DATA_1KM_SEASON
            sea_file_dic = get_file_sea_hdf(s_l)
            data_sea, lon, lat = get_hdf_dic_data_point(sea_file_dic, path, dataType, row, col)
        if task == 7:
            if out_fi == 1:
                data_point_to_txt(DATA_STAT, data_sea, dataType)  # 输出至txt
            return data_sea, lon, lat
        mean_sea = get_sea_mean(data_sea)
        if task == 8:
            if out_fi == 1:
                data_point_to_txt(DATA_STAT, mean_sea, dataType)  # 输出至txt
            return mean_sea, lon, lat
        if int(dateAnomaly) != 0:
            s_l = judge_file(dateAnomaly, dateAnomaly, dataType, 'sea')
            if s_l == 0 or len(s_l) < (int(dateAnomaly) - int(dateAnomaly) + 1) * 4 - 1:
                data_sea, lon, lat = get_sea_data(dateAnomaly, dateAnomaly, dataType, row, col, s_l)
                print('data_sea:', data_sea)
            else:
                path = DATA_1KM_SEASON
                sea_file_dic = get_file_sea_hdf(s_l)
                data_sea, lon, lat = get_hdf_dic_data_point(sea_file_dic, path, dataType, row, col)
        ano_sea = get_sea_ano(data_sea, mean_sea)
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, ano_sea, dataType)  # 输出至txt
        return ano_sea, lon, lat
    elif task in [10, 11, 12]:
        print('任务：', taskChoice)
        q_l = judge_file(dateStart, dateEnd, dataType, 'qua')
        if q_l == 0 or len(q_l) < (int(dateEnd) - int(dateStart) + 1) * 4:
            data_qua, lon, lat = get_qua_data(dateStart, dateEnd, dataType, row, col, q_l)
            print('data_qua:', data_qua)
        else:
            path = DATA_1KM_QUARTER
            qua_file_dic = get_file_qua_hdf(q_l)
            data_qua, lon, lat = get_hdf_dic_data_point(qua_file_dic, path, dataType, row, col)
        if task == 10:
            if out_fi == 1:
                data_point_to_txt(DATA_STAT, data_qua, dataType)  # 输出至txt
            return data_qua, lon, lat
        mean_qua = get_qua_mean(data_qua)
        if task == 11:
            if out_fi == 1:
                data_point_to_txt(DATA_STAT, mean_qua, dataType)  # 输出至txt
            return mean_qua, lon, lat
        if int(dateAnomaly) != 0:
            q_l = judge_file(dateAnomaly, dateAnomaly, dataType, 'qua')
            if q_l == 0 or len(q_l) < (int(dateAnomaly) - int(dateAnomaly) + 1) * 4:
                data_qua, lon, lat = get_qua_data(dateAnomaly, dateAnomaly, dataType, row, col, q_l)
                print('data_qua:', data_qua)
            else:
                path = DATA_1KM_QUARTER
                qua_file_dic = get_file_qua_hdf(q_l)
                data_qua, lon, lat = get_hdf_dic_data_point(qua_file_dic, path, dataType, row, col)
        ano_qua = get_qua_ano(data_qua, mean_qua)
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, ano_qua, dataType)  # 输出至txt
        return ano_qua, lon, lat


def num_area(dataType, taskChoice, dateStart, dateEnd, leftLongitude, leftLatitude,
             rightLongitude, rightLatitude, out_fi=1, dateAnomaly=0):
    d_t = get_datatype()
    task = task_dict[str(taskChoice)]
    if dataType not in d_t:
        return '数据类型错误！'
    print(dataType)
    date_str = datetime.now().strftime("%Y%m%d%H%M%S")
    try:
        print(leftLongitude.dtype)
    except:
        loa = np.array([float(leftLongitude), float(leftLatitude), float(rightLongitude), float(rightLatitude)])
        # row_min, row_max, col_min, col_max = get_area_index_by_lon_lat(loa[0], loa[1], loa[2], loa[3])  # !!！
        row_min, col_min = get_point_index_by_lon_lat(loa[0], loa[1])
        row_max, col_max = get_point_index_by_lon_lat(loa[2], loa[3])
    else:
        row_min, col_min = 0, 0
        row_max, col_max = 4501, 7001
    print('行列：', row_min, row_max, col_min, col_max)
    if task in [1, 2, 3]:
        print('任务：', taskChoice)
        # get file , for read , num add
        y_l = judge_file(dateStart, dateEnd, dataType, 'year')
        if y_l == 0 or len(y_l) < (int(dateEnd) - int(dateStart) + 1):
            data_year, lons, lats = get_year_data(dateStart, dateEnd, DATA_1KM_MONTH, dataType, y_l)
            print('data_year:', data_year)
            data_year_2 = copy.deepcopy(data_year)
            if y_l != 0:
                for year_file in y_l:
                    year_str = year_file[4:8]
                    del data_year_2[int(year_str)]
            data_area_to_hdf(DATA_1KM, data_year_2, lons, lats, dataType, 'YEAR/{}'.format(dataType), 0)
        else:
            path = DATA_1KM_YEAR
            data_year, lons, lats = get_hdf_list_data(y_l, path, dataType)
        years_sum = years_sum_area(dateStart, dateEnd, data_year, row_min, row_max, col_min, col_max)
        lon = lons[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
        lat = lats[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
        if task == 1:
            if out_fi == 1:
                data_area_to_hdf(DATA_STAT, years_sum, lon, lat, dataType, date_str)
            return years_sum, lon, lat
        year_data_list = []
        for key_, y_sum in years_sum.items():
            year_data_list.append(y_sum)
        year_area_ave = ave_a(year_data_list, int(dateEnd) - int(dateStart) + 1)
        if task == 2:
            year_area_ave_dict = {'yearMean': year_area_ave, }
            if out_fi == 1:
                data_area_to_hdf(DATA_STAT, year_area_ave_dict, lon, lat, dataType, date_str)
            return year_area_ave_dict, lon, lat
        if int(dateAnomaly) != 0:
            print('计算指定年份的距平', dateAnomaly)
            y_l = judge_file(dateAnomaly, dateAnomaly, dataType, 'year')
            if y_l == 0 or len(y_l) < (int(dateAnomaly) - int(dateAnomaly) + 1):
                data_year, lons, lats = get_year_data(dateAnomaly, dateAnomaly, DATA_1KM_MONTH, dataType, y_l)
                print('data_year:', data_year)
                data_year_2 = copy.deepcopy(data_year)
                if y_l != 0:
                    for year_file in y_l:
                        year_str = year_file[4:8]
                        del data_year_2[int(year_str)]
                data_area_to_hdf(DATA_1KM, data_year_2, lons, lats, dataType, 'YEAR/{}'.format(dataType), 0)
                # 获取年值
            else:
                path = DATA_1KM_YEAR
                data_year, lons, lats = get_hdf_list_data(y_l, path, dataType)
            years_sum = years_sum_area(dateAnomaly, dateAnomaly, data_year, row_min, row_max, col_min, col_max)
        year_jp_all_dict = {}
        for year, y_sum in years_sum.items():
            year_jp_all = juping_a(y_sum, year_area_ave)
            year_jp_all_dict[year] = year_jp_all
            # year_jp_dict = {'yearAnomaly': year_jp_all, }
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, year_jp_all_dict, lon, lat, dataType, date_str)
        return year_jp_all_dict, lon, lat
    elif task in [4, 5, 6]:
        print('任务：', taskChoice)
        m_l = judge_file(dateStart, dateEnd, dataType, 'mon')
        if task == 4:
            mon_data_area, lon, lat = get_mon_data_area(m_l, DATA_1KM_MONTH, dataType, row_min, row_max, col_min,
                                                        col_max)
            if out_fi == 1:
                data_area_to_hdf(DATA_STAT, mon_data_area, lon, lat, dataType, date_str)
            return mon_data_area, lon, lat
        mon_mean_data_area, lon, lat = get_mon_mean_data_area(m_l, DATA_1KM_MONTH, dataType, row_min, row_max, col_min,
                                                              col_max)
        if task == 5:
            if out_fi == 1:
                data_area_to_hdf(DATA_STAT, mon_mean_data_area, lon, lat, dataType, date_str)
            return mon_mean_data_area, lon, lat
        if int(dateAnomaly) != 0:
            m_l = get_date_start_end(dateAnomaly, dateAnomaly, dataType)
        mon_ano_data_area, lon, lat = get_mon_ano_data_area(m_l, mon_mean_data_area, DATA_1KM_MONTH, dataType, row_min,
                                                            row_max, col_min,
                                                            col_max)
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, mon_ano_data_area, lon, lat, dataType, date_str)
        return mon_ano_data_area, lon, lat
    elif task in [7, 8, 9]:
        print('任务：', taskChoice)
        s_l = judge_file(dateStart, dateEnd, dataType, 'sea')
        if s_l == 0 or len(s_l) < (int(dateEnd) - int(dateStart) + 1) * 4 - 1:
            data_sea, lon, lat = get_sea_data_area(dateStart, dateEnd, dataType, row_min, row_max, col_min,
                                                   col_max, s_l)
            print('data_sea:', data_sea)
        else:
            path = DATA_1KM_SEASON
            sea_file_dic = get_file_sea_hdf(s_l)
            data_sea, lon, lat = get_hdf_dic_data_area(sea_file_dic, path, dataType, row_min, row_max, col_min,
                                                       col_max)
        if task == 7:
            if out_fi == 1:
                data_area_to_hdf(DATA_STAT, data_sea, lon, lat, dataType, date_str)
            return data_sea, lon, lat
        mean_sea = get_sea_mean(data_sea)
        if task == 8:
            if out_fi == 1:
                data_area_to_hdf(DATA_STAT, mean_sea, lon, lat, dataType, date_str)
            return mean_sea, lon, lat
        if int(dateAnomaly) != 0:
            s_l = judge_file(dateAnomaly, dateAnomaly, dataType, 'sea')
            if s_l == 0 or len(s_l) < (int(dateAnomaly) - int(dateAnomaly) + 1) * 4 - 1:
                data_sea, lon, lat = get_sea_data_area(dateAnomaly, dateAnomaly, dataType, row_min, row_max, col_min,
                                                       col_max, s_l)
                print('data_sea:', data_sea)
            else:
                path = DATA_1KM_SEASON
                sea_file_dic = get_file_sea_hdf(s_l)
                data_sea, lon, lat = get_hdf_dic_data_area(sea_file_dic, path, dataType, row_min, row_max, col_min,
                                                           col_max)
        ano_sea = get_sea_ano(data_sea, mean_sea)
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, ano_sea, lon, lat, dataType, date_str)
        return ano_sea, lon, lat
    elif task in [10, 11, 12]:
        print('任务：', taskChoice)
        q_l = judge_file(dateStart, dateEnd, dataType, 'qua')
        if q_l == 0 or len(q_l) < (int(dateEnd) - int(dateStart) + 1) * 4:
            data_qua, lon, lat = get_qua_data_area(dateStart, dateEnd, dataType, row_min, row_max, col_min,
                                                   col_max, q_l)
            print('data_qua:', data_qua)
        else:
            path = DATA_1KM_QUARTER
            qua_file_dic = get_file_qua_hdf(q_l)
            data_qua, lon, lat = get_hdf_dic_data_area(qua_file_dic, path, dataType, row_min, row_max, col_min,
                                                       col_max)
        print(data_qua)
        if task == 10:
            if out_fi == 1:
                data_area_to_hdf(DATA_STAT, data_qua, lon, lat, dataType, date_str)
            return data_qua, lon, lat
        mean_qua = get_qua_mean(data_qua)
        if task == 11:
            if out_fi == 1:
                data_area_to_hdf(DATA_STAT, mean_qua, lon, lat, dataType, date_str)
            return mean_qua, lon, lat
        if int(dateAnomaly) != 0:
            q_l = judge_file(dateAnomaly, dateAnomaly, dataType, 'qua')
            if q_l == 0 or len(q_l) < (int(dateAnomaly) - int(dateAnomaly) + 1) * 4:
                data_qua, lon, lat = get_qua_data_area(dateAnomaly, dateAnomaly, dataType, row_min, row_max, col_min,
                                                       col_max, q_l)
                print('data_qua:', data_qua)
            else:
                path = DATA_1KM_QUARTER
                qua_file_dic = get_file_qua_hdf(q_l)
                data_qua, lon, lat = get_hdf_dic_data_area(qua_file_dic, path, dataType, row_min, row_max, col_min,
                                                           col_max)
        ano_qua = get_qua_ano(data_qua, mean_qua)
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, ano_qua, lon, lat, dataType, date_str)
        return ano_qua, lon, lat


def num_province(dataType, province, taskChoice, dateStart, dateEnd, out_fi=0, avg=False, out_fig=1, dateAnomaly=0):
    # 获取掩码
    sheng_dic = {'北京市': 2, '湖南省': 14,
                 '天津市': 27, '广东省': 6,
                 '河北省': 10, '广西壮族自治区': 7,
                 '山西省': 25, '海南省': 9,
                 '内蒙古自治区': 19, '重庆市': 3,
                 '辽宁省': 18, '四川省': 26,
                 '吉林省': 17, '贵州省': 8,
                 '黑龙江省': 11, '云南省': 30,
                 '上海市': 24, '西藏自治区': 29,
                 '江苏省': 15, '陕西省': 22,
                 '浙江省': 31, '甘肃省': 5,
                 '安徽省': 1, '青海省': 21,
                 '福建省': 4, '宁夏回族自治区': 20,
                 '江西省': 16, '新疆维吾尔自治区': 28,
                 '山东省': 23, '台湾省': 32,
                 '河南省': 12, '香港特别行政区': 33,
                 '湖北省': 13, '澳门特别行政区': 34, }
    open_file_path = PRO_MASK_HDF
    pro_data = get_hdf5_data(open_file_path, 'province_mask', 1, 0, [0, np.inf], np.nan)
    print('province:', province)
    print('avg:', avg)
    if province != 'all':
        for pr, pr_da in sheng_dic.items():
            if province in pr:
                print(pr)
                pro_data[pro_data != pr_da] = 0
    pro_bool = pro_data.astype(bool)
    # 获取数据
    if province == 'all':
        data_dic, lons, lats = num_area(dataType, taskChoice, dateStart, dateEnd, pro_bool, pro_bool,
                                        pro_bool, pro_bool, out_fi, dateAnomaly=dateAnomaly)
        for ke, da in data_dic.items():
            print(da.shape)
            data_dic[ke] = da[pro_bool]
        lons, lats = lons[pro_bool], lats[pro_bool]
    else:
        # 获取经纬度
        file_path = DEM_HDF
        lons = get_hdf5_data(file_path, 'lon', 1, 0, [0, np.inf], np.nan)
        lats = get_hdf5_data(file_path, 'lat', 1, 0, [0, np.inf], np.nan)
        left_longitude, left_latitude = lons[pro_bool], lats[pro_bool]
        data_dic, lons, lats = num_point(dataType, taskChoice, dateStart, dateEnd, left_longitude, left_latitude,
                                         out_fi, dateAnomaly=dateAnomaly)
    # 输出到hdf
    date_str = datetime.now().strftime("%Y%m%d%H%M%S")
    out_data_dic = {}
    avg_year_dic = {}
    for ad, da in data_dic.items():
        out_data_dic[ad] = da.reshape(-1, 1)
        if avg:
            da = da[da.astype(bool)]
            avg_year = np.nanmean(da)
            avg_year_dic[ad] = avg_year
    lon = lons.reshape(-1, 1)
    lat = lats.reshape(-1, 1)
    if out_fig == 1:
        if avg:
            data_point_to_txt(DATA_STAT, avg_year_dic, dataType)
        else:
            data_area_to_hdf(DATA_STAT, out_data_dic, lon, lat, dataType, date_str)
    if avg:
        return avg_year_dic, lon, lat
    # return
    return out_data_dic, lon, lat


filterwarnings("ignore")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GFSSI Schedule')
    parser.add_argument('--dataType', '-t', help='数据类型(GHI/DBI/DHI/GTI/...)', required=True)
    parser.add_argument('--modeType', '-m', help='单点or范围or省or全国(point或area或province或all)', required=True)
    parser.add_argument('--province', '-z', help='省级行政区域名称（汉字）', required=False)
    parser.add_argument('--avg', '-g', help='区域平均值（True）', required=False)
    parser.add_argument('--taskChoice', '-c',
                        help='时间：year, month, season, quarter'
                             '任务: sum, mean, anomaly,'
                             'yearSum, yearMean, yearAnomaly,'
                             'monthSum, monthMean, monthAnomaly,'
                             'seasonSum, seasonMean, seasonAnomaly,'
                             'quarterSum, quarterMean, quarterAnomaly,',
                        required=False)
    parser.add_argument('--dateStart', '-s', help='开始年份，YYYY(2019)', required=True)
    parser.add_argument('--dateEnd', '-e', help='结束年份，YYYY(2019)', required=True)
    parser.add_argument('--dateAnomaly', '-o', help='指定计算距平年份，YYYY(2019)', required=False)
    parser.add_argument('--leftLongitude', '-l', help='经度或左上角经度，47.302235', required=False)
    parser.add_argument('--leftLatitude', '-a', help='纬度或左上角纬度，85.880519', required=False)
    parser.add_argument('--rightLongitude', '-r', help='右下角经度，47.302235', required=False)
    parser.add_argument('--rightLatitude', '-i', help='右下角纬度，85.880519', required=False)
    args = parser.parse_args()
    if args.dateAnomaly:
        date_anomaly = args.dateAnomaly
    else:
        date_anomaly = 0
    if args.modeType == 'point':
        print(args.dataType, args.taskChoice, args.dateStart, args.dateEnd, args.leftLongitude, args.leftLatitude,
              date_anomaly)
        n_p = num_point(args.dataType, args.taskChoice, args.dateStart, args.dateEnd, args.leftLongitude,
                        args.leftLatitude, dateAnomaly=date_anomaly)
        # print(n_p)
    elif args.modeType == 'area':
        print(args.dataType, args.taskChoice, args.dateStart, args.dateEnd, args.leftLongitude,
              args.leftLatitude, args.rightLongitude, args.rightLatitude, date_anomaly)
        n_a = num_area(args.dataType, args.taskChoice, args.dateStart, args.dateEnd, args.leftLongitude,
                       args.leftLatitude, args.rightLongitude, args.rightLatitude, dateAnomaly=date_anomaly)
        # print(n_a)
    elif args.modeType == 'province':
        print(args.dataType, args.province, args.taskChoice, args.dateStart, args.dateEnd, date_anomaly)
        num_province(args.dataType, args.province, args.taskChoice, args.dateStart, args.dateEnd,
                     dateAnomaly=date_anomaly)
    elif args.modeType == 'all':
        print(args.dataType, 'all', args.taskChoice, args.dateStart, args.dateEnd, args.avg, date_anomaly)
        if args.avg:
            num_province(args.dataType, 'all', args.taskChoice, args.dateStart, args.dateEnd, avg=args.avg,
                         dateAnomaly=date_anomaly)
        else:
            num_province(args.dataType, 'all', args.taskChoice, args.dateStart, args.dateEnd,
                         dateAnomaly=date_anomaly)
    '''
    python3 a04_data_statistics.py -t GHI -m point  -c yearSum -s 2019 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c yearMean -s 2019 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c yearAnomaly -s 2017 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c monthSum -s 2018 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c monthMean -s 2018 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c monthAnomaly -s 2018 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c seasonSum -s 2019 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c seasonMean -s 2019 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c seasonAnomaly -s 2017 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c quarterSum -s 2019 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c quarterMean -s 2019 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c quarterAnomaly -s 2017 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m area  -c yearSum -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c yearMean -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c yearAnomaly -s 2017 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c monthSum  -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c monthMean -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c monthAnomaly -s 2017 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c seasonSum -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c seasonMean -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c seasonAnomaly -s 2014 -e 2015 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c quarterSum -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c quarterMean -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c quarterAnomaly -s 2016 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m province -z 山东  -c yearSum -s 2019 -e 2019
    python3 a04_data_statistics.py -t GHI -m province -z 山东  -c yearMean -s 2019 -e 2019
    python3 a04_data_statistics.py -t GHI -m province -z 山东  -c yearAnomaly -s 2019 -e 2019
    python3 a04_data_statistics.py -t GHI -m province -z 山东  -c monthSum  -s 2019 -e 2019
    python3 a04_data_statistics.py -t GHI -m province -z 山东  -c monthMean -s 2019 -e 2019 
    python3 a04_data_statistics.py -t GHI -m province -z 山东  -c monthAnomaly -s 2019 -e 2019 
    python3 a04_data_statistics.py -t GHI -m province -z 山东  -c seasonSum -s 2019 -e 2019 
    python3 a04_data_statistics.py -t GHI -m province -z 山东  -c seasonMean -s 2019 -e 2019
    python3 a04_data_statistics.py -t GHI -m province -z 山东  -c seasonAnomaly -s 2019 -e 2019 
    python3 a04_data_statistics.py -t GHI -m province -z 山东  -c quarterSum -s 2019 -e 2019 
    python3 a04_data_statistics.py -t GHI -m province -z 山东  -c quarterMean -s 2019 -e 2019
    python3 a04_data_statistics.py -t GHI -m province -z 山东  -c quarterAnomaly -s 2019 -e 2019
    python3 a04_data_statistics.py -t GHI -m all -c yearMean -s 2019 -e 2019 -g True
    '''
