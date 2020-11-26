#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-11-23 16:27
# @Author  : NingAnMe <ninganme@qq.com>
import os

DATA_ROOT = '/home/kts_project_v1/gffp/DATA/'  # 数据根目录
# DATA_ROOT = './DATA/'  # 数据根目录
DATA_1KM = os.path.join(DATA_ROOT, '1KM')  # 插值后的1KM数据
DATA_PICTURE = os.path.join(DATA_ROOT, 'PICTURE')  # 图片结果
DATA_STAT = os.path.join(DATA_ROOT, 'STAT')  # 统计结果
DATA_DOC = os.path.join(DATA_ROOT, 'DOC')  # 报告结果
