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


def ave(sum, len):
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


def juping(oneyear, ave):
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


def years_sum_point(dateStart, dateEnd, data, row, col):
    '''
    :param dateStart:
    :param dateEnd:
    :param data:
    :param row:
    :param col:
    :return: {
                year1 ; 年值
                year2 ; 年值
                }
    '''
    years_num = years_sum(dateStart, dateEnd, data)
    print(years_num)
    years_num_p = dict()
    for year in np.arange(int(dateStart), int(dateEnd) + 1):
        year_sum = years_num[year]
        year_point_data = year_sum[int(row)][int(col)]
        print(year)
        print('year_point_data', year_point_data)
        years_num_p[year] = year_point_data

    return years_num_p


def years_sum_area(dateStart, dateEnd, data, row_min, row_max, col_min, col_max):
    '''
    :param dateStart:
    :param dateEnd:
    :param data:
    :param row_min:
    :param row_max:
    :param col_min:
    :param col_max:
    :return: {
            year : np,
            }
    '''
    years_num = years_sum(dateStart, dateEnd, data)
    print(years_num)
    years_num_p = dict()
    for year in np.arange(int(dateStart), int(dateEnd) + 1):
        year_sum = years_num[year]
        year_area_data = year_sum[int(row_min) - 1:int(row_max), int(col_min) - 1:int(col_max)]
        print(year)
        print('year_area_data', year_area_data)
        years_num_p[year] = year_area_data

    return years_num_p


def num_point(dataType, taskChoice, dateStart, dateEnd, leftLongitude, leftLatitude, out_fi=1):
    # 选择数据类型
    d_t = get_datatype()
    if dataType in d_t:
        datapath = "{}".format(dataType)
    else:
        return '数据类型错误！'
    task = task_dict[str(taskChoice)]
    print(dataType)
    # 获取数据
    loa = np.array([float(leftLongitude), float(leftLatitude)])
    row, col = get_point_index_by_lon_lat(loa[0], loa[1])
    print(int(row), int(col))
    print(row, col)
    data = get_data(dateStart, dateEnd, dataType)
    if task == 1:
        years_sum = years_sum_point(dateStart, dateEnd, data, row, col)  # 获取年值
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, years_sum)  # 输出至txt
        return years_sum
    elif task == 2:
        years_sum = years_sum_point(dateStart, dateEnd, data, row, col)
        year_data_list = []
        for key_, year_sum in years_sum.items():
            year_data_list.append(year_sum)
        year_ave = ave(year_data_list, int(dateEnd) - int(dateStart) + 1)
        year_ave_dict = {'yearMean': year_ave, }
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, year_ave_dict)  # 输出至txt
        return year_ave_dict
    elif task == 3:
        years_sum = years_sum_point(dateStart, dateEnd, data, row, col)
        year_data_list = []
        for year, year_sum in years_sum.items():
            year_data_list.append(year_sum)
        year_ave = ave(year_data_list, int(dateEnd) - int(dateStart) + 1)
        year_jp_all_dict = {}
        for year, year_sum in years_sum.items():
            year_jp_all = juping(year_sum, year_ave)
            year_jp_all_dict[year] = year_jp_all
        if out_fi == 1:
            data_point_to_txt(DATA_STAT, year_jp_all_dict)
        return year_jp_all_dict


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
    data, lon, lat = get_data(dateStart, dateEnd, dataType)
    print('数据获取完毕')
    # print(data)
    # 获取经纬
    lons = lon[int(row_min) - 1: int(row_max), int(col_min) - 1: int(col_max)]
    lats = lat[int(row_min) - 1: int(row_max), int(col_min) - 1: int(col_max)]
    if task == 1:
        years_sum = years_sum_area(dateStart, dateEnd, data, row_min, row_max, col_min, col_max)
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, years_sum, lons, lats, dataType, date_str)
        return years_sum
    elif task == 2:
        years_sum = years_sum_area(dateStart, dateEnd, data, row_min, row_max, col_min, col_max)
        year_data_list = []
        for key_, year_sum in years_sum.items():
            year_data_list.append(year_sum)
        year_area_ave = ave_a(year_data_list, int(dateEnd) - int(dateStart) + 1)
        year_area_ave_dict = {'yearMean': year_area_ave, }
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, year_area_ave_dict, lons, lats, dataType, date_str)
        return year_area_ave_dict
    elif task == 3:
        years_sum = years_sum_area(dateStart, dateEnd, data, row_min, row_max, col_min, col_max)
        year_data_list = []
        for key_, year_sum in years_sum.items():
            year_data_list.append(year_sum)
        print()
        year_area_ave = ave_a(year_data_list, int(dateEnd) - int(dateStart) + 1)
        year_jp_all_dict = {}
        for year, year_sum in years_sum.items():
            year_jp_all = juping_a(year_sum, year_area_ave)
            year_jp_all_dict[year] = year_jp_all
            year_jp_dict = {'yearAnomaly': year_jp_all, }
        if out_fi == 1:
            data_area_to_hdf(DATA_STAT, year_jp_all_dict, lons, lats, dataType, date_str)
        return year_jp_all_dict


def get_date_start_end(date_start, date_end, data_type):
    print(date_start, date_end, data_type)
    data_path = os.path.join(DATA_1KM, data_type)
    # data_path = 'D:\project\py\gz\ky\gffp\\aid\data\{}'.format(data_type)
    print('get_date_start_end', data_path)
    file_list = []
    for file_ in os.listdir(data_path):
        filename = os.path.split(file_)[1]
        # print(filename)
        date_str = str(filename).split('_')[1]
        file_date = date(int(date_str[:4]), int(date_str[4:6]), 1)
        # print(file_date)
        file_date = file_date.strftime("%Y")
        # print(int(file_date))
        if int(date_start) <= int(file_date) <= int(date_end):
            # print('file_', file_)
            file_list.append(file_)
    return file_list


def get_file_year(files):
    files_year = defaultdict(list)
    for file_ in files:
        filename = os.path.split(file_)
        date_str = str(filename).split('_')[1]
        file_date = date(int(date_str[:4]), int(date_str[4:6]), 1)
        y = file_date.year
        files_year[y].append(file_)
    return files_year


def get_hdf_list_data(files_one_year, path, data_type):
    hdf_data_list = []
    a = 0
    for hdf in files_one_year:
        a += 1
        data_path = os.path.join(DATA_1KM, data_type)
        file_hdf = '{}/{}'.format(data_path, hdf)
        print('file_hdf', file_hdf, data_type)
        data = get_hdf5_data(file_hdf, data_type, 1, 0, [-9000, 9000], np.nan)
        # print(file_hdf, data)
        if a == 1:
            dem = DemLoader()
            dem.file_hdf = file_hdf
            lon, lat = dem.get_lon_lat()
        # print(lon, lat)
        hdf_data_list.append(data)
    return hdf_data_list, lon, lat


def get_data(date_start, date_end, data_type):
    '''
    :param date_start:
    :param date_end:
    :param data_type:
    :return: {
            year:{
                'data': data,
                'lon': lon,
                'lat': lat,
                }
            }
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
        result = {
            'data': data,
        }
        results_return[year] = result
        # write_hdf5_and_compress()
    return results_return, lon, lat


def data_point_to_txt(path, data):
    '''=
    :param path:
    :param data:  dict{
                        key:data
                        }
    :return:
    '''
    data_str = ''
    kg = '  '
    hh = '\n'
    print(data)
    print(type(data))
    for ad, da in data.items():
        data_str = str(ad) + kg + str(da) + hh
    # 输出txt
    date_str = datetime.now().strftime("%Y%m%d%H%M%S")
    path = os.path.join(path, date_str)
    dirs = os.path.join(path, 'Point_{}.txt'.format(data_type))
    if not os.path.exists(path):
        os.makedirs(path)
    print('finish{}'.format(path))
    with open(dirs, "w", encoding='utf-8') as f:
        f.write(data_str)


def data_area_to_hdf(path, data, lons, lats, data_type, date_str):
    '''
    :param path:
    :param data: dict{
                        key:data
                        }
    :param lons:
    :param lats:
    :return:
    '''
    for ad, da in data.items():
        datas = {}
        datas[data_type] = da
        datas['lon'] = lons
        datas['lat'] = lats
        path = os.path.join(path, date_str)
        dirs = os.path.join(path, 'area_{}_{}.hdf'.format(data_type, ad))
        if not os.path.exists(path):
            os.makedirs(path)
        write_hdf5_and_compress(datas, dirs)
        print('finish{}'.format(path))


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
