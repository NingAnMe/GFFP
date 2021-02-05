#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File : conversion_of_general_SHP_file_to_H5_file.py 
# @Time : 2021/1/21 9:46 
# @Author : YanKai jie
import sys
import os

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from shapefile import Reader
from shapely.geometry import Point, Polygon
import numpy as np
import pandas as pd
from utils.path import ROOT_PATH, AID_PATH
from utils.config import PRO_MASK_HDF, DEM_HDF
from utils.hdf5 import get_hdf5_data, write_hdf5_and_compress
# import multiprocessing
import geopandas as gpd
import copy
from datetime import datetime, timedelta
import h5py
from collections import defaultdict
import time

DUIZHAOBIAO_CSV = os.path.join(AID_PATH, 'duizhaobiao.csv')


def get_point_index_by_lon_lat(lons, lats, xllcorner=69.9951, yllcorner=9.995, res=0.01, nrows=4501):
    """
    通过经纬度获取点的index
    :param lons: float\np.array\\list 点的经度
    :param lats: float\np.array\\list 点的纬度
    :param xllcorner:  float 列起始位置经度
    :param yllcorner: float 行起始位置纬度
    :param res: float 分辨率
    :param nrows: int 总的行数
    :return: np.array 行列号
    """
    if not isinstance(lons, np.ndarray):
        lons = np.array(lons)
    if not isinstance(lats, np.ndarray):
        lats = np.array(lats)
    # print(yllcorner, lats, res, nrows)
    rows = (yllcorner - lats) // res + nrows
    rows = rows.astype(np.int)

    cols = (lons - xllcorner) // res
    cols = cols.astype(np.int)
    return rows, cols


def get_shp_data(shp_file, out_file_path):
    """
    获取shp文件点集数据
    :param shp_file:shp文件位置及名称
    :param out_file_path:输出文件路径
    :return:
    """
    print(shp_file, out_file_path)
    shps = Reader(shp_file, encoding="gbk")
    # shp = shps.shapeRecords()[0].shape
    # [1845, 450122, '武鸣县', 'Wuming Xian', '南宁市', '广西壮族自治区', '530100', 2.84348057351, 0.298945041587]
    # {name:[points]}
    name_points_dic = defaultdict(list)
    # {name:code}
    name_code_dic = {}
    name_loa_min_max = {}
    for she in shps.shapeRecords():
        if '' not in she.record[1:6]:
            name_points_dic[she.record[2]].append(she.shape.points)
            name_code_dic[she.record[2]] = she.record[1]
            # print(she.record,she.shape.points)
            lon_min = None
            lon_max = None
            lat_min = None
            lat_max = None
            for lon, lat in she.shape.points:
                if lon_min:
                    if lon_min > lon:
                        lon_min = lon
                    if lon_max < lon:
                        lon_max = lon
                    if lat_min > lat:
                        lat_min = lat
                    if lat_max < lat:
                        lat_max = lat
                else:
                    lon_min = lon
                    lon_max = lon
                    lat_min = lat
                    lat_max = lat
            name_loa_min_max[she.record[2]] = {
                'lon_min': lon_min,
                'lon_max': lon_max,
                'lat_min': lat_min,
                'lat_max': lat_max,
            }
    # 生成对照表文件(CSV)
    keys = name_code_dic.keys()
    values = name_code_dic.values()
    duizhaobiao = pd.DataFrame({'name': keys, 'code': values})
    duizhaobiao_path = DUIZHAOBIAO_CSV
    duizhaobiao.to_csv(duizhaobiao_path, index=False)

    shp_to_hdf(name_points_dic, name_code_dic, out_file_path, name_loa_min_max)


def shp_to_hdf(name_points_dic, name_code_dic, out_put_file, name_loa_min_max):
    """

    :param name_points_dic:
    :param name_code_dic:
    :param out_put_file:
    :return:
    """
    time_start = datetime.now()
    print('开始时间：', time_start)
    # 缓存文件路径
    cache_path = os.path.join(AID_PATH, 'shp_cache/cache.csv')
    # 文件是否存在 若存在 获取其数据
    file_data = None
    if os.path.isfile(out_put_file):
        file_data = get_hdf5_data(out_put_file, 'data', 1, 0, [0, np.inf], np.nan)
    # 坐标点获取及对应
    print('开始获取坐标数据', datetime.now())
    file_path = DEM_HDF
    lons = get_hdf5_data(file_path, 'lon', 1, 0, [0, np.inf], np.nan)
    lats = get_hdf5_data(file_path, 'lat', 1, 0, [0, np.inf], np.nan)
    lon_lat_group_dic = defaultdict(list)
    x_y_group_dic = defaultdict(list)
    for name, min_max_dic in name_loa_min_max.items():

        if os.path.isfile(cache_path):
            cache_data = pd.read_csv(cache_path)
            cache_data_name = cache_data['name']
            # print('已完成：', list(cache_data_name))
            if name in list(cache_data_name):
                print('{}的数据已存在，跳过'.format(name))
                continue

        row_min, col_min = get_point_index_by_lon_lat(min_max_dic['lon_min'], min_max_dic['lat_max'])
        row_max, col_max = get_point_index_by_lon_lat(min_max_dic['lon_max'], min_max_dic['lat_min'])
        # print('-------------------')
        # print(min_max_dic['lon_min'], min_max_dic['lon_max'])
        # print(lons[0][col_min], lons[0][col_max+3])
        # print(min_max_dic['lat_max'], min_max_dic['lat_min'])
        # print(lats[row_min][0], lats[row_max+1][0])
        # print('-------------------')
        x = row_min
        nb0 = None
        if row_min == 0:
            row_min = 1
        if col_min == 0:
            col_min = 1
        if row_max >= 4500:
            row_max = 4499
        if col_max > 6997:
            col_max = 6997

        for lat in lats[row_min - 1:row_max + 1, 0]:
            y = col_min
            for lon in lons[0][col_min - 1:col_max + 3]:
                # 文件数据不存在 或 文件数据此坐标数值为0
                if isinstance(file_data, np.ndarray):
                    nb0 = int(file_data[x][y])
                if (not nb0) or nb0 == 0:
                    lon_lat_group_dic[name].append((lon, lat))
                    x_y_group_dic[name].append((x, y))
                y += 1
            x += 1
        # print(name, 'end')
        # print(datetime.now(), datetime.now() - time_start)

    print('坐标数据获取完毕', datetime.now())
    print('开始进行处理')

    time_end = None
    i = 0
    cache_list = []
    for name_p, point_p in name_points_dic.items():
        i += 1
        print('开始处理{}的数据'.format(name_p))
        # 检测是否存在缓存并进行处理
        if os.path.isfile(cache_path):
            cache_data = pd.read_csv(cache_path)
            cache_data_name = cache_data['name']
            print('已完成：', len(cache_data_name))
            if name_p in list(cache_data_name):
                print('{}的数据已存在，跳过'.format(name_p))
                continue
        # 数据处理
        if isinstance(file_data, np.ndarray):
            print('data_out从file_data中获取数据')
            data_out = file_data
        else:
            print('生成data_out数据')
            data_out = np.zeros((len(lons), len(lons[0])))
        a = 0
        lon_lat_list = np.array(lon_lat_group_dic[name_p])
        x_y_list = np.array(x_y_group_dic[name_p])
        print('坐标数量：', len(lon_lat_list))
        print('开始获取pts', datetime.now())
        pts = gpd.GeoSeries([Point(li[0], li[1]) for li in lon_lat_list])
        print('pts获取完毕', datetime.now())
        print(len(point_p))
        for points_one in point_p:
            a += 1
            print('points', a, len(points_one), datetime.now())
            poly = Polygon(points_one)
            print('poly', a, datetime.now())
            data_bool = pts.within(poly)
            print('bool数据获取完毕', datetime.now())
            # 筛选符合条件的坐标 写入输出数据 data_out
            x_y_list_cp = copy.deepcopy(x_y_list)
            for x_y in x_y_list_cp[data_bool]:
                x = x_y[0]
                y = x_y[1]
                data_out[x][y] = name_code_dic[name_p]
            pts = pts[~data_bool]
            print('pts len :', len(pts))
            x_y_list = x_y_list[~data_bool]
            print('x_y_list len :', len(x_y_list))
        # 生成文件
        data_out = data_out.astype(int)
        file_data = data_out
        cache_list.append(name_p)
        if i in range(0, 3000, 50):
            while True:
                try:
                    # 生成文件
                    datas = {'data': data_out}
                    write_hdf5_and_compress(datas, out_put_file)
                    while True:
                        try:
                            # 生成缓存文件
                            if os.path.isfile(cache_path):
                                data_a = pd.DataFrame(data=cache_list)
                                data_a.to_csv(cache_path, mode='a', header=False, index=False)
                            else:
                                data_a = pd.DataFrame(data=cache_list, columns=['name'])
                                data_a.to_csv(cache_path, mode='a', index=False)
                            cache_list = []
                            break
                        except:
                            print('!!!!生成缓存失败，重试')
                            time.sleep(30)
                            continue
                    break
                except:
                    print('!!!!生成文件失败，重试')
                    time.sleep(30)
                    continue

                print('时间：', datetime.now())
        if time_end:
            print('此区域用时：', datetime.now() - time_end)
            print('此次总共用时：', datetime.now() - time_start)
            # time.sleep(15)
        print('已处理区域数量:', i)
        time_end = datetime.now()

    time_end = datetime.now()
    print('结束时间：', time_end)
    print('共用时：', time_end - time_start)


if __name__ == '__main__':
    file_xian = os.path.join(ROOT_PATH, 'DV/SHP/中国县市.shp')
    out_file_path = 'aid/xian_mask.h5'

    get_shp_data(file_xian, out_file_path)
