#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-11-23 16:27
# @Author  : NingAnMe <ninganme@qq.com>
import os

DATA_ROOT = '/DISK/DATA02/PROJECT/SourceData/GFFP_DATA'  # 数据根目录
# DATA_ROOT = './DATA/'  # 数据根目录

# 原始数据


# 中间结果
DATA_1KM = os.path.join(DATA_ROOT, '1KM')  # 插值后的1KM数据
DATA_1KM_MONTH = os.path.join(DATA_1KM, 'MONTH')  # 插值后的1KM，月数据
DATA_1KM_SEASON = os.path.join(DATA_1KM, 'SEASON')  # 插值后的1KM，季节数据
DATA_1KM_QUARTER = os.path.join(DATA_1KM, 'QUARTER')  # 插值后的1KM，季度数据
DATA_1KM_YEAR = os.path.join(DATA_1KM, 'YEAR')  # 插值后的1KM，年数据


# 用户数据
DATA_PICTURE = os.path.join(DATA_ROOT, 'PICTURE')  # 图片结果
DATA_STAT = os.path.join(DATA_ROOT, 'STAT')  # 统计结果
DATA_DOC = os.path.join(DATA_ROOT, 'DOC')  # 报告结果
