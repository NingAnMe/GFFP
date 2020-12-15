#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020-12-07 09:57:06
# @Author  : YanKaijie

import sys
import os

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from shapefile import Reader
from shapely.geometry import Point, Polygon
import numpy as np
from utils.path import ROOT_PATH, AID_PATH
from utils.config import PRO_MASK_HDF, DEM_HDF
from utils.hdf5 import get_hdf5_data, write_hdf5_and_compress
# import multiprocessing
import geopandas as gpd
import copy
import time
import h5py

# sheng_dic = {'北京市': 11, '湖南省': 43,
#              '天津市': 12, '广东省': 44,
#              '河北省': 13, '广西壮族自治区': 45,
#              '山西省': 14, '海南省': 46,
#              '内蒙古自治区': 15, '重庆市': 50,
#              '辽宁省': 21, '四川省': 51,
#              '吉林省': 22, '贵州省': 52,
#              '黑龙江省': 23, '云南省': 53,
#              '上海市': 31, '西藏自治区': 54,
#              '江苏省': 32, '陕西省': 61,
#              '浙江省': 33, '甘肃省': 62,
#              '安徽省': 34, '青海省': 63,
#              '福建省': 35, '宁夏回族自治区': 64,
#              '江西省': 36, '新疆维吾尔自治区': 65,
#              '山东省': 37, '台湾省': 71,
#              '河南省': 41, '香港特别行政区': 81,
#              '湖北省': 42, '澳门特别行政区': 82, }
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
file_p = os.path.join(ROOT_PATH, 'DV/SHP/中国省级行政区.shp')
shps = Reader(file_p, encoding="gbk")
shp = shps.shapeRecords()[0].shape
pyo_lon_lat_dic = {}
for she in shps.shapeRecords():
    pyo_lon_lat_dic[she.record[0]] = []
# print(pyo_lon_lat_dic)
print(len(pyo_lon_lat_dic))
for she in shps.shapeRecords():
    # print(she.record[0], len(she.shape.points), )
    pyo_lon_lat_dic[she.record[0]].append(she.shape.points)
    # print(len(pyo_lon_lat_dic[she.record[0]]))


def get_data(lon_lat_list):
    # name = multiprocessing.current_process().name  # 获取当前进程的名字
    pts = lon_lat_list[0]
    i = 0
    ke_n, ll = lon_lat_list[1], lon_lat_list[2]
    print(ke_n, len(ll), '开始')
    data_b_sun = np.ndarray
    for li in ll:
        poly = Polygon(li)
        print('poly')
        data_b = pts.within(poly)
        data_b[data_b == False] = 0
        data_b[data_b == True] = sheng_dic[ke_n]
        if i == 0:
            data_b_sun = data_b
        else:
            data_b_sun = data_b_sun + data_b
        print(i)
        i += 1
    data_b_sun = data_b_sun.astype(int)
    print(data_b_sun)
    print('data_b_sun')
    print(ke_n, '生成')
    print('------------------------------------------------------------')
    return data_b_sun


def main():
    file_path = DEM_HDF
    lons = get_hdf5_data(file_path, 'lon', 1, 0, [0, np.inf], np.nan)
    lats = get_hdf5_data(file_path, 'lat', 1, 0, [0, np.inf], np.nan)
    lon_lat_list = []
    x_y_list = []
    x = 0
    for lat in lats[..., 0]:
        y = 0
        for lon in lons[0]:
            lon_lat_list.append((lon, lat))
            x_y_list.append((x, y))
            y += 1
        x += 1
    print(len(lon_lat_list))
    lon_lat_list = np.array(lon_lat_list)
    x_y_list = np.array(x_y_list)
    # 31511501
    pro_name_list = []
    x_y_pro_list = []
    bool_list = []
    path_cache = os.path.join(AID_PATH, 'shp_cache')
    if not os.path.exists(path_cache):
        os.makedirs(path_cache)
    else:
        file_list = []
        for file_ in os.listdir(path_cache):
            file_list.append(file_)
        if not file_list:
            print('No File')
        else:
            for i in range(0, len(file_list)):
                path = 'cache_{}.hdf'.format(i)
                print('从缓存文件{}获取缓存数据'.format(path))
                cache_path_file = os.path.join(path_cache, path)
                with h5py.File(cache_path_file, 'r') as f:
                    for ke, da in f.items():
                        da = np.array(da)
                        bool_list.append(da)
                        pro_name_list.append(ke)
    fig = 0
    pts = gpd.GeoSeries([Point(li[0], li[1]) for li in lon_lat_list])
    print('pts获取完毕')
    x_y_li = np.ndarray
    x_y_pro = np.ndarray
    lo_la_li = np.ndarray
    # 跳过标志
    tg = 0
    for ke_n, ll in pyo_lon_lat_dic.items():
        print('start', )
        t1 = time.time()
        print('fig:', fig)
        if fig != 0:
            print('未缓存数据处理')
            if fig == 1:
                lo_la_li = lon_lat_list[bool_list[0].astype(bool)]
                pts = gpd.GeoSeries([Point(li[0], li[1]) for li in lo_la_li])
                print('pts获取完毕')
            else:
                lo_la_li = lo_la_li[bool_list[-1].astype(bool)]
                pts = gpd.GeoSeries([Point(li[0], li[1]) for li in lo_la_li])
                print('pts获取完毕')
                if tg == -1:
                    x_y_pro = x_y_li[~bool_list[-1].astype(bool)]
                    x_y_li = x_y_li[bool_list[-1].astype(bool)]
                    x_y_pro_list.append(x_y_pro)
                    print('筛选后的点数量', len(x_y_li))
                    tg = 0
        # 存在缓存文件
        elif bool_list:
            print('已缓存数据处理')
            if tg == 0:
                for i in range(0, len(bool_list) - 1):
                    print()
                    bool_ = bool_list[i].astype(bool)
                    print('bool_', len(bool_))
                    print('~bool_', len(~bool_))
                    if i == 0:
                        lo_la_li = lon_lat_list[bool_]
                        pts = gpd.GeoSeries([Point(li[0], li[1]) for li in lo_la_li])
                        print('pts获取完毕')
                        x_y_li = x_y_list[bool_]
                        x_y_pro = x_y_list[~bool_]
                    else:
                        lo_la_li = lo_la_li[bool_]
                        pts = gpd.GeoSeries([Point(li[0], li[1]) for li in lo_la_li])
                        print('pts获取完毕！')
                        x_y_pro = x_y_li[~bool_]
                        x_y_li = x_y_li[bool_]
                    print('筛选后的点数量', len(x_y_li))
                    x_y_pro_list.append(x_y_pro)
                tg = len(bool_list) - 1
                print('tg0:', tg)
                continue
            else:
                if tg == 1:
                    fig = len(bool_list)
                    print('fig:', fig)
                print('tg:', tg)
                tg -= 1
                if tg == 0:
                    tg = -1
                continue
        print(ke_n, 'start', )
        if fig == 0:
            print('坐标数量', len(lon_lat_list))
        else:
            print('坐标数量', len(lo_la_li))
        # 检查点是否在范围内
        boo_ken = get_data([pts, ke_n, ll])
        print('bool获取完毕')
        # 通过bool去除检查通过的点
        bo = boo_ken
        bo_pro = copy.deepcopy(boo_ken)
        bo[bo == 0] = 1
        bo[bo != 1] = 0
        bo = bo.astype(bool)
        bool_list.append(bo)
        # 返回省全部行列值，添加到x_y_pro_list
        bo_pro = bo_pro.astype(bool)
        if fig == 0:
            x_y_li = x_y_list[bo]
            x_y_pro = x_y_list[bo_pro]
        else:
            x_y_pro = copy.deepcopy(x_y_li)
            x_y_li = x_y_li[bo]
            x_y_pro = x_y_pro[bo_pro]
        path = 'cache_{}.hdf'.format(fig)
        cache_path_dir = os.path.join(path_cache, path)
        hdf_data = {ke_n: bo, }
        write_hdf5_and_compress(hdf_data, cache_path_dir)
        print('缓存文件{}写入成功'.format(path))
        x_y_pro_list.append(x_y_pro)
        pro_name_list.append(ke_n)
        print(pro_name_list)
        print(len(x_y_pro_list))
        # datas = {}
        data = np.zeros((4501, 7001))
        for i in range(0, len(pro_name_list)):
            for x_y in x_y_pro_list[i]:
                x = x_y[0]
                y = x_y[1]
                data[x][y] = sheng_dic[pro_name_list[i]]
            # data = data.astype(int)
            print(pro_name_list[i], len(x_y_pro_list[i]))
            # datas[pro_name_list[i]] = data
        # print(data.shape)
        data = data.astype(int)
        datas = {'province_mask': data}
        write_hdf5_and_compress(datas, PRO_MASK_HDF)

        t2 = time.time()
        print(ke_n, 'end', t2 - t1)
        fig += 1

    # print('l_list 就绪')
    # with Pool(1) as poo:
    #     print('进程池启动')
    #     da = poo.map(get_data, l_list)
    # print(da)
    # data = np.zeros((4501, 7001))
    # print(data.shape)


if __name__ == '__main__':
    main()
    # python3 shp_province.py 17816
    # nohup  python3  -u shp_province.py  &
    # tail -f nohup.out
