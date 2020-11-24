#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-06-15 17:04
# @Author  : NingAnMe <ninganme@qq.com>
import os
from utils.path import AID_PATH


DB_PATH = os.path.join(AID_PATH, 'db.db')
SQLLITE_DB = r'sqlite:///{}'.format(DB_PATH)

# 辅助文件
DEM_TXT = os.path.join(AID_PATH, 'D_DEM.txt')
DEM_HDF = os.path.join(AID_PATH, 'D_DEM.hdf')

COEF_TXT = os.path.join(AID_PATH, 'sta_mon_a_b_ok.txt')
POOR_XLSX = os.path.join(AID_PATH, '贫困村站点经纬度.xlsx')


def print_config():
    print('SQLLITE_DB == {}'.format(SQLLITE_DB))
    print('DEM_TXT == {}'.format(DEM_TXT))
    print('DEM_HDF == {}'.format(DEM_HDF))
    print('COEF_TXT == {}'.format(COEF_TXT))
    print('POOR_XLSX == {}'.format(POOR_XLSX))


def get_datatype():
    datatype = ['GHI', 'DBI', 'DHI', 'GTI']
    return datatype
