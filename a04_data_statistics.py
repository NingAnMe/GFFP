#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020-11-16 15:47
# @Author  : Yan Kaijie
import os
import argparse
import copy
from datetime import datetime, date
from warnings import filterwarnings
import numpy as np
from utils.config import get_datatype
from utils.get_index_by_lonlat import get_point_index_by_lon_lat
from utils.data import DemLoader
from utils.hdf5 import get_hdf5_data, write_hdf5_and_compress
from utils.config import PRO_MASK_HDF, PROVINCE_MASK
# from collections import defaultdict
from user_config import DATA_1KM_MONTH, DATA_1KM_SEASON, DATA_1KM_QUARTER, DATA_1KM_YEAR, DATA_STAT

'''
1）输入单点经纬度，输出txt格式的数据；
2）输入区域经纬度范围，输出hdf
3）输入省级名称，输出省级数据
4）输入all，输出全国数据
'''
task_name_list = ['year', 'mon', 'sea', 'qua']


def judge_file(date_start, date_end, data_type, task_name):
    """
    :param date_start:开始年份
    :param date_end:结束年份
    :param data_type:数据类型
    :param task_name: 'sea' 'qua' 'year' 'mon' 任务所需数据数据名称
    :return:file_list 返回文件名列表
    """
    print('judge_file')
    print(date_start, date_end, data_type, task_name)
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
        print('已创建文件夹', data_path)
    # GHI_2020_Fir.hdf
    if data_type in ['H0', 'H20', 'H25']:
        return 0
    file_list = []
    for file_ in os.listdir(data_path):
        filename = os.path.split(file_)[1]
        date_str = str(filename).split('_')[1]
        file_date = date(int(date_str[:4]), 1, 1)
        file_date = file_date.strftime("%Y")
        if int(date_start) <= int(file_date) <= int(date_end):
            file_list.append(file_)
    print(file_list)
    if not file_list:
        print('Judge No File')
        return 0
    return file_list


def get_month_data_by_start_end(date_start, date_end, data_type):
    """
    通过起止时间和文件类型获取所需月数据文件列表
    """
    print(date_start, date_end, data_type)
    data_path = os.path.join(DATA_1KM_MONTH, data_type)
    print('get_month_data_by_start_end', data_path)
    file_list = []
    for file_ in os.listdir(data_path):
        filename = os.path.split(file_)[1]
        date_str = str(filename).split('_')[1]
        file_date = date(int(date_str[:4]), int(date_str[4:6]), 1)
        file_date = file_date.strftime("%Y")
        if int(date_start) <= int(file_date) <= int(date_end):
            file_list.append(file_)
    if not file_list:
        raise ValueError('No file in {} - {}'.format(date_start, date_end))
    return file_list


def get_group_files(files, group_key, date_start, date_end):
    """
    文件分组
    :param files: list 文件列表
    :param group_key: str分组关键字
    :param date_start: str 开始时间
    :param date_end:  str
    :return: {key1:{key2:[files],}}
            {year:{y:[files],}}{m:{m:[files],}}{year:{sea:[files],}}{year:{qua:[files],}}
    """
    data_dic = {}
    for fi in files:
        # print(fi)
        filename = os.path.split(fi)[1]
        date_str = str(filename).split('_')[1]
        file_date = date(int(date_str[:4]), int(date_str[4:6]), 1)
        y = int(file_date.year)
        m = int(file_date.month)
        if group_key == task_name_list[0]:
            try:
                data_dic[y]
            except:
                data_dic[y] = {}
            try:
                data_dic[y][y]
            except:
                data_dic[y][y] = []
            data_dic[y][y].append(fi)
        elif group_key == task_name_list[1]:
            try:
                data_dic[m]
            except:
                data_dic[m] = {}
            try:
                data_dic[m][m]
            except:
                data_dic[m][m] = []
            data_dic[m][m].append(fi)
        elif group_key == task_name_list[2]:
            if m in [3, 4, 5]:
                try:
                    data_dic[y]
                except:
                    data_dic[y] = {}
                try:
                    data_dic[y]['MAM']
                except:
                    data_dic[y]['MAM'] = []
                data_dic[y]['MAM'].append(fi)
            elif m in [6, 7, 8]:
                try:
                    data_dic[y]
                except:
                    data_dic[y] = {}
                try:
                    data_dic[y]['JJA']
                except:
                    data_dic[y]['JJA'] = []
                data_dic[y]['JJA'].append(fi)
            elif m in [9, 10, 11]:
                try:
                    data_dic[y]
                except:
                    data_dic[y] = {}
                try:
                    data_dic[y]['SON']
                except:
                    data_dic[y]['SON'] = []
                data_dic[y]['SON'].append(fi)
            elif m == 12:
                try:
                    data_dic[y]
                except:
                    data_dic[y] = {}
                try:
                    data_dic[y]['DJF']
                except:
                    data_dic[y]['DJF'] = []
                data_dic[y]['DJF'].append(fi)
            elif m in [1, 2]:
                try:
                    data_dic[y - 1]
                except:
                    data_dic[y - 1] = {}
                try:
                    data_dic[y - 1]['DJF']
                except:
                    data_dic[y - 1]['DJF'] = []
                data_dic[y - 1]['DJF'].append(fi)
        elif group_key == task_name_list[3]:
            if m in [1, 2, 3]:
                try:
                    data_dic[y]
                except:
                    data_dic[y] = {}
                try:
                    data_dic[y]['fir']
                except:
                    data_dic[y]['fir'] = []
                data_dic[y]['fir'].append(fi)
            elif m in [4, 5, 6]:
                try:
                    data_dic[y]
                except:
                    data_dic[y] = {}
                try:
                    data_dic[y]['sec']
                except:
                    data_dic[y]['sec'] = []
                data_dic[y]['sec'].append(fi)
            elif m in [7, 8, 9]:
                try:
                    data_dic[y]
                except:
                    data_dic[y] = {}
                try:
                    data_dic[y]['thr']
                except:
                    data_dic[y]['thr'] = []
                data_dic[y]['thr'].append(fi)
            elif m in [10, 11, 12]:
                try:
                    data_dic[y]
                except:
                    data_dic[y] = {}
                try:
                    data_dic[y]['fou']
                except:
                    data_dic[y]['fou'] = []
                data_dic[y]['fou'].append(fi)
    if group_key == task_name_list[2]:
        del data_dic[int(date_start) - 1]
        del data_dic[int(date_end)]['DJF']
    return data_dic


def data_point_to_txt(path, data, dataType, date_str):
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
    path = os.path.join(path, date_str)
    dirs = os.path.join(path, 'Point_{}.txt'.format(dataType))
    if not os.path.exists(path):
        os.makedirs(path)
    with open(dirs, "w") as f:
        f.write(data_str)
    print('finish{}'.format(path))


def data_area_to_hdf(path, data, lons, lats, dataType, date_str, dee=1):
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


def get_data(date_start, date_end, data_path, data_type, task_key=None, file_list=None):
    """
    :return: { year:data} lon lat
    """
    print('get_data')
    print(date_start, date_end, data_path, data_type, task_key, file_list)
    lon = np.ndarray
    lat = np.ndarray
    results_return = dict()
    mean_len = 0
    mon_mean_dic = dict()
    if data_type in ['H0', 'H20', 'H25']:
        files = get_month_data_by_start_end(date_start, date_end, 'GTI')  # 获取月数据分组文件
        date_set = set()
        for file_ in files:
            filename = os.path.split(file_)[1]
            date_str = str(filename).split('_')[1]
            file_date = date(int(date_str[:4]), int(date_str[4:6]), 1)
            file_date = file_date.strftime("%Y")
            date_set.add(file_date)
        print(date_set)
        pro_bool = np.ndarray
        for year in date_set:
            data, lon, lat = data_statistics(data_type='GTI',
                                             mode_type='origin',
                                             # date_start=int(year) - 29,
                                             date_start=int(year),
                                             date_end=year,
                                             task_choice='yearMean',
                                             out_fig=0)
            da_h = float()
            for na, da in data.items():
                if data_type == 'H0':
                    da_h = da * 0.8
                elif data_type == 'H20':
                    da_h = da * 0.8 * 0.92
                elif data_type == 'H25':
                    da_h = da * 0.8 * 0.90
                results_return[int(year)] = da_h
            mean_len = len(results_return)
    else:
        files = get_month_data_by_start_end(date_start, date_end, data_type)  # 获取月数据列表
        print('files', files)
        i = 0
        lon = np.ndarray
        lat = np.ndarray
        # {year:{y:[files],}} {m:{m:[files],}} {year:{sea:[files],}} {year:{qua:[files],}}
        group_files = get_group_files(files, task_key, date_start, date_end)
        print('group_files', group_files)
        if task_key != task_name_list[1]:
            mean_len = len(group_files)
        for first_key, files_one_year in group_files.items():
            fig = 0
            for second_key, dic_files_list in files_one_year.items():
                if task_key == task_name_list[0] and len(dic_files_list) != 12:
                    continue
                print('dic_files_list', dic_files_list)
                a = 0
                if fig == 1:
                    continue
                if task_key == task_name_list[0] and task_key != task_name_list[1]:
                    dic_key = '{}'.format(first_key)
                    file_name = '{}_{}.hdf'.format(data_type, first_key)
                else:
                    dic_key = '{}_{}'.format(first_key, second_key)
                    file_name = '{}_{}_{}.hdf'.format(data_type, first_key, second_key)
                if task_key == task_name_list[1]:
                    dic_key = first_key
                    if mean_len == 0:
                        mean_len = len(dic_files_list)
                if file_list and file_name in file_list:
                    file_hdf = os.path.join(data_path, '{}/{}'.format(data_type, file_name))
                    data_sum = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
                    if not isinstance(lon.size, int):
                        lon = get_hdf5_data(file_hdf, 'lon', 1, 0, [0, np.inf], np.nan)
                        lat = get_hdf5_data(file_hdf, 'lat', 1, 0, [0, np.inf], np.nan)
                    if task_key == task_name_list[0]:
                        fig = 1
                else:
                    data_sum = np.ndarray
                    data_copy_nan = np.ndarray  # 统计所有文件中所有点的nan值情况

                    for file in dic_files_list:
                        a += 1
                        i += 1
                        data_dir = os.path.join(DATA_1KM_MONTH, data_type)
                        file_hdf = '{}/{}'.format(data_dir, file)
                        # print('file_hdf', file_hdf, data_type)
                        data = get_hdf5_data(file_hdf, data_type, 1, 0, [0, np.inf], np.nan)
                        data_copy = copy.deepcopy(data)
                        data_copy[~np.isnan(data_copy)] = 1
                        data_copy[np.isnan(data_copy)] = 0
                        data[np.isnan(data)] = 0
                        if a != 1:
                            data_sum = data_sum + data
                            data_copy_nan = data_copy_nan + data_copy
                        else:
                            data_sum = data
                            data_copy_nan = data_copy
                        if not isinstance(lon.size, int):
                            dem = DemLoader()
                            dem.file_hdf = file_hdf
                            lon, lat = dem.get_lon_lat()
                    data_copy_nan[data_copy_nan == 0] = np.nan
                    data_sum = data_sum + data_copy_nan
                    # 当任务不为逐月时，输出到hdf
                    if task_key != task_name_list[1]:
                        data_area_to_hdf(data_path, {dic_key: data_sum}, lon, lat, data_type, data_type, 0)
                    else:
                        mon_mean_dic[dic_key] = data_copy_nan
                if np.nan in data_sum:
                    print('=============')
                # print(dic_key, data_sum)
                results_return[dic_key] = data_sum
    if mon_mean_dic:
        mean_len = mon_mean_dic
    return results_return, lon, lat, mean_len


def np_mean(lis):
    sum_a = np.ndarray
    divisor_nd = np.ndarray
    a = 0
    for s in lis:
        a += 1
        s_copy = copy.deepcopy(s)
        s_copy[~np.isnan(s_copy)] = 1
        s_copy[np.isnan(s_copy)] = 0
        s[np.isnan(s)] = 0
        if a == 1:
            sum_a = s
            divisor_nd = s_copy
        else:
            sum_a = sum_a + s
            divisor_nd = divisor_nd + s_copy
    divisor_nd[divisor_nd == 0] = np.nan
    np_me = sum_a / divisor_nd
    return np_me


def get_mean(data_dic, task_key, len_get):
    # print('开始计算平均')
    data_mean_dic = {}
    if task_key == task_name_list[0]:
        list_y = []
        for key_d, da in data_dic.items():
            list_y.append(da)
        data_mean_dic = {'years_mean': np_mean(list_y)}
    elif task_key == task_name_list[1]:
        for key_d, da in data_dic.items():
            data_mean_dic[key_d] = da / len_get[key_d]
    elif task_key in task_name_list[2:4]:
        s_q_list = [[], [], [], []]
        if task_key == task_name_list[2]:
            name_dic = {'MAM': 0, 'JJA': 1, 'SON': 2, 'DJF': 3}
        else:
            name_dic = {'fir': 0, 'sec': 1, 'thr': 2, 'fou': 3}
        for s_q_key, data in data_dic.items():
            filename = os.path.split(s_q_key)[1]
            s_q_str = str(filename).split('_')[1]
            for na, nu in name_dic.items():
                if s_q_str == na:
                    s_q_list[nu].append(data)
                # s_q_list[3].append(data)
        for na, nu in name_dic.items():
            if na != 'DJF':
                data_mean_dic[na] = np_mean(s_q_list[nu])
            else:
                data_mean_dic[na] = np_mean(s_q_list[nu])
    # print('平均计算完毕')
    return data_mean_dic


def np_ano(one_year, ave_area):
    """
    （一年值 - 多年平均值） / 多年平均值 * 100 %
    :param one_year: 一年值
    :param ave_area: 多年平均值
    :return:
    """
    jp = (one_year - ave_area) / ave_area * 100
    print('距平：', jp)
    return jp


def get_ano(task_key, data_dic, mean_dic):
    print('开始计算距平')
    ano_dic = {}
    for data_key, data in data_dic.items():
        if task_key == task_name_list[0]:
            ano = np_ano(data, mean_dic['years_mean'])
            ano_dic[data_key] = ano
        elif task_key == task_name_list[1]:
            ano = np_ano(data, mean_dic[data_key])
            ano_dic[data_key] = ano
        elif task_key in task_name_list[2:4]:
            filename = os.path.split(data_key)[1]
            sea_str = str(filename).split('_')[1]
            ano = np_ano(data, mean_dic[sea_str])
            ano_dic[data_key] = ano
    print('距平计算完毕')
    return ano_dic


def num_point(dataType, taskChoice, dateStart, dateEnd, left_longitude, left_latitude, out_fi=1, dateChoice=None):
    out_put = data_statistics(
        data_type=dataType,
        mode_type='point',
        task_choice=taskChoice,
        date_start=dateStart,
        date_end=dateEnd,
        date_choice=dateChoice,
        left_longitude=left_longitude,
        left_latitude=left_latitude,
        out_fig=out_fi
    )
    return out_put[0], out_put[1], out_put[2]


def num_area(dataType, taskChoice, dateStart, dateEnd, leftLongitude, leftLatitude,
             rightLongitude, rightLatitude, out_fi=1, dateChoice=None):
    out_put = data_statistics(
        data_type=dataType,
        mode_type='area',
        task_choice=taskChoice,
        date_start=dateStart,
        date_end=dateEnd,
        date_choice=dateChoice,
        left_longitude=leftLongitude,
        left_latitude=leftLatitude,
        right_longitude=rightLongitude,
        right_latitude=rightLatitude,
        out_fig=out_fi
    )
    return out_put[0], out_put[1], out_put[2]


def num_province(dataType, province, taskChoice, dateStart, dateEnd, avg=False, out_fi=1, dateChoice=None):
    if province == 'all':
        province = None
        modeType = 'all'
    else:
        modeType = 'province'
    out_put = data_statistics(
        data_type=dataType,
        mode_type=modeType,
        task_choice=taskChoice,
        date_start=dateStart,
        date_end=dateEnd,
        date_choice=dateChoice,
        province=province,
        out_fig=out_fi,
        avg=avg
    )
    return out_put[0], out_put[1], out_put[2]


filterwarnings("ignore")


def data_statistics(data_type, mode_type, task_choice, date_start=None, date_end=None, date_choice=None, province=None,
                    avg=False, left_longitude=None, left_latitude=None, right_longitude=None, right_latitude=None,
                    out_fig=1):
    """
    根据输入参数计算结果
    :param data_type: 数据类型(GHI/DBI/DHI/GTI/H0/H20/H25)
    :param mode_type: 范围模式：单点or矩形范围or省or全国或不筛选(point或area或province或all或origin)
    :param task_choice:任务: sum, mean, anomaly,'
                             'yearSum, yearMean, yearAnomaly,'
                             'monthSum, monthMean, monthAnomaly,'
                             'seasonSum, seasonMean, seasonAnomaly,'
                             'quarterSum, quarterMean, quarterAnomaly'
    :param date_start:开始年份，YYYY(2019)
    :param date_end:结束年份，YYYY(2019)
    :param left_longitude:经度或左上角经度，47.302235
    :param left_latitude:纬度或左上角纬度，85.880519
    :param date_choice:指定计算年份，YYYY(2019)
    :param province:省级行政区域名称（汉字）
    :param avg:区域平均值（True）
    :param right_longitude:右下角经度，47.302235
    :param right_latitude:右下角纬度，85.880519
    :param out_fig:结果是否输出标志位（0/1）
    :return:元组(字典数据，经度，纬度)或None或错误信息
    """
    print(
        'data_type, mode_type, task_choice, date_start, '
        'date_end, date_choice, province,avg, left_longitude, '
        'left_latitude, right_longitude, right_latitude,out_fig')
    print(data_type, mode_type, task_choice, date_start, date_end, date_choice, province,
          avg, left_longitude, left_latitude, right_longitude, right_latitude,
          out_fig)
    # return final_data
    final_data = None
    # 标志位
    txt_hdf_fig = 0  # 选择输出到txt或hdf标志位
    # mode_type列表
    mode_type_list = ['point', 'area', 'province', 'all', 'origin']
    # task_choice 列表
    task_choice_list = ['yearSum', 'yearMean', 'yearAnomaly',
                        'monthSum', 'monthMean', 'monthAnomaly',
                        'seasonSum', 'seasonMean', 'seasonAnomaly',
                        'quarterSum', 'quarterMean', 'quarterAnomaly', ]
    # 判断输入参数是否正确
    if data_type not in get_datatype():
        final_data = '数据类型输入错误'
    elif mode_type not in mode_type_list:
        final_data = '范围模式输入错误'
    elif task_choice not in task_choice_list:
        final_data = '任务类型输入错误'
    elif (not (date_start and date_end)) and (not date_choice):
        final_data = '请输入起止年份或指定年份'
    elif mode_type == 'point' and (left_longitude is None or left_latitude is None):
        final_data = '经纬度未输入或输入有误'
    elif mode_type == 'area' and (left_longitude is None or
                                  left_latitude is None or
                                  right_longitude is None or
                                  right_latitude is None):
        final_data = '经纬度未输入或输入有误'
    elif mode_type == 'province' and province is None:
        final_data = '请输入省级名称'
    else:
        date_s = str()
        date_e = str()
        task_name = str()
        data_path = str()
        if date_choice and date_start is None and date_end is None:
            date_s = date_choice
            date_e = date_choice
        elif (date_choice is None and date_start and date_end) or (date_start and date_end and date_choice):
            date_s = date_start
            date_e = date_end
        if task_choice in task_choice_list[0:3]:
            task_name = task_name_list[0]
            data_path = DATA_1KM_YEAR
        elif task_choice in task_choice_list[3:6]:
            task_name = task_name_list[1]
            data_path = DATA_1KM_MONTH
        elif task_choice in task_choice_list[6:9]:
            task_name = task_name_list[2]
            data_path = DATA_1KM_SEASON
        elif task_choice in task_choice_list[9:12]:
            task_name = task_name_list[3]
            data_path = DATA_1KM_QUARTER
        # 通过数据类型、任务类型 判断所需使用的原始数据 检测数据 返回数据文件列表
        file_list = judge_file(date_s, date_e, data_type, task_name)
        # 获取数据 若需要使用缓存数据则检测缓存数据是否存在 不存在则生成
        data, lon, lat, len_m = get_data(date_s, date_e, data_path, data_type, task_name, file_list)
        print('数据获取完毕')
        # 原始数据或缓存数据进行处理
        # 获取年值或逐月份多年累加值或逐季节多年累加值或逐季度多年累加值
        data_sec = data
        print('----------')
        for na, da in data_sec.items():
            print(na, np.nanmean(da), np.nanmin(da), np.nanmax(da))
        print('----------')
        # 获取相应数据的平均值
        if task_choice in ['yearMean', 'monthMean',
                           'seasonMean', 'quarterMean',
                           'yearAnomaly', 'monthAnomaly',
                           'seasonAnomaly', 'quarterAnomaly',
                           ]:
            print('开始获取均值')
            data_mean_dic = get_mean(data, task_name, len_m)
            print('均值获取完毕')
            print('----------')
            for na, da in data_mean_dic.items():
                print(na, np.nanmean(da), np.nanmin(da), np.nanmax(da))
            print('----------')
        if task_choice in ['yearMean',
                           'monthMean',
                           'seasonMean',
                           'quarterMean', ]:
            data_sec = data_mean_dic
        # 获取相应数据或指定时间的距平
        if task_choice in ['yearAnomaly',
                           'monthAnomaly',
                           'seasonAnomaly',
                           'quarterAnomaly', ]:
            print('开始获取距平')
            if date_choice and date_start and date_choice:
                file_list = judge_file(date_choice, date_choice, data_type, task_name)
                data, lon, lat, len_m = get_data(date_choice, date_choice, data_path, data_type, task_name, file_list)
            data_sec = get_ano(task_name, data, data_mean_dic)
            print('距平获取完毕')
            print('-----da-----')
            for na, da in data.items():
                print(na, np.nanmean(da), np.nanmin(da), np.nanmax(da))
            print('----------')
            print('-----an-----')
            for na, da in data_sec.items():
                print(na, np.nanmean(da), np.nanmin(da), np.nanmax(da))
            print('----------')
        # 通过范围筛选数据
        if mode_type == mode_type_list[0]:
            if type(left_longitude) == str:
                left_longitude = float(left_longitude)
                left_latitude = float(left_latitude)
            row, col = get_point_index_by_lon_lat(left_longitude, left_latitude)
            row = row - 1
            col = col - 1
            data_thr = {}
            for ke_na, da in data_sec.items():
                data_thr[ke_na] = da[(row, col)]

            final_data = (data_thr, lon[(row, col)], lat[(row, col)])
        elif mode_type == mode_type_list[1]:
            loa = np.array(
                [float(left_longitude), float(left_latitude), float(right_longitude), float(right_latitude)])
            row_min, col_min = get_point_index_by_lon_lat(loa[0], loa[1])
            row_max, col_max = get_point_index_by_lon_lat(loa[2], loa[3])
            data_thr = {}
            for ke_na, da in data_sec.items():
                data_thr[ke_na] = da[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
            lon = lon[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
            lat = lat[int(row_min):int(row_max) + 1, int(col_min):int(col_max) + 1]
            final_data = (data_thr, lon, lat)
            txt_hdf_fig = 1
        elif mode_type in mode_type_list[2:4]:
            # 获取掩码
            sheng_dic = PROVINCE_MASK
            open_file_path = PRO_MASK_HDF
            pro_data = get_hdf5_data(open_file_path, 'province_mask', 1, 0, [0, np.inf], np.nan)
            if mode_type != 'all':
                for pr, pr_da in sheng_dic.items():
                    if province in pr:
                        pro_data[pro_data != pr_da] = 0
            pro_bool = pro_data.astype(bool)
            data_thr = {}
            for ke_na, da in data_sec.items():
                data_thr[ke_na] = da[pro_bool]
            lon = lon[pro_bool]
            lat = lat[pro_bool]
            final_data_0 = {}
            for ad, da in data_thr.items():
                final_data_0[ad] = da.reshape(-1, 1)
                txt_hdf_fig = 1
                if avg:
                    da = da[da.astype(bool)]
                    avg_data = np.nanmean(da)
                    final_data_0[ad] = avg_data
                    txt_hdf_fig = 0
            lon = lon.reshape(-1, 1)
            lat = lat.reshape(-1, 1)
            final_data = (final_data_0, lon, lat)
        else:
            final_data = (data_sec, lon, lat)
            txt_hdf_fig = 1
        # 输出到txt或hdf
        if out_fig == 1:
            date_str = datetime.now().strftime("%Y%m%d%H%M%S")
            if txt_hdf_fig == 0:
                data_point_to_txt(DATA_STAT, final_data[0], data_type, date_str)
            else:
                data_area_to_hdf(DATA_STAT, final_data[0], lon, lat, data_type, date_str)
    # print(final_data)
    return final_data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GFSSI Schedule')
    parser.add_argument('--dataType', '-t', help='数据类型(GHI/DBI/DHI/GTI/H0/H20/H25...)', required=True)
    parser.add_argument('--modeType', '-m', help='范围模式：单点or矩形范围or省or全国or全部(point或area或province或all或origin)',
                        required=True)
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
    parser.add_argument('--dateStart', '-s', help='开始年份，YYYY(2019)', required=False)
    parser.add_argument('--dateEnd', '-e', help='结束年份，YYYY(2019)', required=False)
    parser.add_argument('--dateChoice', '-o', help='指定计算年份，YYYY(2019)', required=False)
    parser.add_argument('--leftLongitude', '-l', help='经度或左上角经度，47.302235', required=False)
    parser.add_argument('--leftLatitude', '-a', help='纬度或左上角纬度，85.880519', required=False)
    parser.add_argument('--rightLongitude', '-r', help='右下角经度，47.302235', required=False)
    parser.add_argument('--rightLatitude', '-i', help='右下角纬度，85.880519', required=False)
    args = parser.parse_args()

    out_put = data_statistics(
        data_type=args.dataType,
        mode_type=args.modeType,
        province=args.province,
        avg=args.avg,
        task_choice=args.taskChoice,
        date_start=args.dateStart,
        date_end=args.dateEnd,
        date_choice=args.dateChoice,
        left_longitude=args.leftLongitude,
        left_latitude=args.leftLatitude,
        right_longitude=args.rightLongitude,
        right_latitude=args.rightLatitude
    )
    # print(out_put)
    '''
    python3 a04_data_statistics.py -t GHI -m point  -c yearSum -s 2019 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t H0 -m point  -c yearSum -s 2019 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t H20 -m point  -c yearSum -s 2018 -e 2019 -o 2020 -l 113 -a 43
    python3 a04_data_statistics.py -t H0 -m point  -c yearSum -s 2019 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c yearMean -s 2019 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c yearAnomaly -s 2017 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c monthSum -s 2018 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c monthMean -s 2018 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c monthAnomaly -s 2018 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c monthAnomaly -s 2018 -e 2019 -o 2020 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c seasonSum -s 2019 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c seasonMean -s 2019 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t DHI -m point  -c seasonMean -s 2016 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c seasonAnomaly -s 2017 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t DHI -m point  -c seasonAnomaly -s 2013 -e 2013 -o 2016 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c quarterSum -s 2019 -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c quarterMean -s 2010 -e 2015 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c quarterAnomaly -s 2010 -e 2016 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m point  -c quarterAnomaly -s 2010 -e 2015 -o 2016 -l 113 -a 43
    python3 a04_data_statistics.py -t GTI -m point  -c H0    -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GTI -m point  -c H20   -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GTI -m point  -c H25   -e 2019 -l 113 -a 43
    python3 a04_data_statistics.py -t GHI -m area  -c yearSum -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t H0  -m area  -c yearSum -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t H25 -m area  -c yearSum -s 2017 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c yearMean -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c yearAnomaly -s 2017 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t DBI -m area  -c yearAnomaly -s 2017 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c monthSum  -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c monthMean -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t DBI -m area  -c monthAnomaly -s 2017 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c monthAnomaly -s 2017 -e 2019 -o 2020 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c seasonSum -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c seasonMean -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c seasonAnomaly -s 2014 -e 2015 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t DBI -m area  -c seasonAnomaly -s 2014 -e 2015 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c quarterSum -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c quarterMean -s 2019 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t GHI -m area  -c quarterAnomaly -s 2016 -e 2019 -l 113 -a 43 -r 120 -i 36
    python3 a04_data_statistics.py -t DBI -m area  -c quarterAnomaly -s 2016 -e 2019 -l 113 -a 43 -r 120 -i 36
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
    python3 a04_data_statistics.py -t GHI -m cn -c yearMean -s 2019 -e 2019 
    python3 a04_data_statistics.py -t GHI -m cn -c yearAnomaly -s 2019 -e 2019 
    python3 a04_data_statistics.py -t GHI -m all -c yearAnomaly -s 2019 -e 2019 
    python3 a04_data_statistics.py -t H0 -m all  -c yearSum -s 2019 -e 2019 -g true
    python3 a04_data_statistics.py -t GHI -m cn -c yearMean -s 2019 -e 2019 -g True
    python3 a04_data_statistics.py -t GHI -m all  -c yearSum -s 2019 -e 2019 -g true
    python3 a04_data_statistics.py -t GTI -m all  -c yearMean -s 2009 -e 2018 -g true
    '''
