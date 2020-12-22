#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-06-15 17:04
# @Author  : NingAnMe <ninganme@qq.com>
import os
from utils.path import AID_PATH

LONGITUDE_RANGE_China = [70, 140]
LATITUDE_RANGE_China = [10, 55]

DB_PATH = os.path.join(AID_PATH, 'db.db')
SQLLITE_DB = r'sqlite:///{}'.format(DB_PATH)

# 辅助文件
DEM_TXT = os.path.join(AID_PATH, 'D_DEM.txt')
DEM_HDF = os.path.join(AID_PATH, 'D_DEM.hdf')
PRO_MASK_HDF = os.path.join(AID_PATH, 'province_mask_old.hdf')
PRO_MASK_HDF_1KM = os.path.join(AID_PATH, 'province_mask_1km.hdf')

PROVINCE_MASK = {'北京市': 2, '湖南省': 14,
                 '天津市': 27, '广东省': 6,
                 '河北省': 10, '广西': 7,
                 '山西省': 25, '海南省': 9,
                 '内蒙古': 19, '重庆市': 3,
                 '辽宁省': 18, '四川省': 26,
                 '吉林省': 17, '贵州省': 8,
                 '黑龙江省': 11, '云南省': 30,
                 '上海市': 24, '西藏': 29,
                 '江苏省': 15, '陕西省': 22,
                 '浙江省': 31, '甘肃省': 5,
                 '安徽省': 1, '青海省': 21,
                 '福建省': 4, '宁夏': 20,
                 '江西省': 16, '新疆': 28,
                 '山东省': 23, '台湾省': 32,
                 '河南省': 12, '香港': 33,
                 '湖北省': 13, '澳门': 34, }

COEF_TXT = os.path.join(AID_PATH, 'sta_mon_a_b_ok.txt')
POOR_XLSX = os.path.join(AID_PATH, '贫困村站点经纬度.xlsx')


def print_config():
    print('SQLLITE_DB == {}'.format(SQLLITE_DB))
    print('DEM_TXT == {}'.format(DEM_TXT))
    print('DEM_HDF == {}'.format(DEM_HDF))
    print('COEF_TXT == {}'.format(COEF_TXT))
    print('POOR_XLSX == {}'.format(POOR_XLSX))


def get_datatype():
    datatype = ['GHI', 'DBI', 'DHI', 'GTI', 'H0', 'H20', 'H25']
    return datatype
