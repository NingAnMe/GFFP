#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2019/8/12
@Author  : AnNing
"""
import os
import pickle

import numpy as np
from scipy.spatial import cKDTree
# from pykdtree.kdtree import KDTree  # 使用这个库没有办法保存kdtree


def make_point_kdtree_lut(lons_data, lats_data, out_file):
    condition = np.logical_and(np.isfinite(lons_data), np.isfinite(lats_data))
    idx = np.where(condition)
    lon_new = lons_data[idx].reshape(-1, 1)
    lat_new = lats_data[idx].reshape(-1, 1)
    # lons_lats = zip(lon_new.reshape(-1, ), lat_new.reshape(-1, ))
    lons_lats = np.concatenate((lon_new, lat_new), axis=1)
    print(lons_lats.shape)
    data = lons_lats
    print('start cKDTree')
    ck = cKDTree(data)

    out_dir = os.path.dirname(out_file)
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)

    with open(out_file, 'wb') as fp:
        pickle.dump((idx, ck), fp)
        print('生成KDtree查找表:{}'.format(out_file))


def get_point_index_kdtree(lon, lat, idx, ck, pre_dist=0.04):
    fix_point = (lon, lat)
    dist, index = ck.query([fix_point], 1)
    dist = dist[0]
    index = index[0]
    print('---Query INFO---dist: {}  index: {}'.format(dist, index))

    if dist <= pre_dist:
        fix_point_index = (idx[0][index], idx[1][index])
        print("---INFO---Nearest fix point index={}".format(fix_point_index))
        return fix_point_index
    else:
        print('***WARNING*** dist > {}, Dont extract.'.format(pre_dist))
        return


def get_point_index_by_lon_lat(lons, lats, xllcorner=69.9951, yllcorner=9.995, res=0.01, nrows=4501):
    """
    通过经纬度获取点的index
    :param lons: np.array 点的经度
    :param lats: np.array 点的纬度
    :param xllcorner:  float 列起始位置经度
    :param yllcorner: float 行起始位置纬度
    :param res: float 分辨率
    :param nrows: int 总的行数
    :return: np.array 行列号
    """
    rows = (yllcorner - lats) // res + nrows - 1
    rows.astype(np.int)

    cols = (lons - xllcorner) // res
    cols.astype(np.int)
    return rows, cols


def get_point_data_by_index(data, rows, cols):
    if data is not None:
        return data[rows, cols]


def get_area_index_by_lon_lat(lons=None, lats=None, left_up_lon=None, left_up_lat=None,
                              right_down_lon=None, right_down_lat=None):
    """
    :param lons: 整个区域的经度
    :param lats: 整个区域的纬度
    :param left_up_lon: 左上角经度
    :param left_up_lat: 左上角纬度
    :param right_down_lon: 右下角经度
    :param right_down_lat: 右下角纬度
    :return:
    """
    lon_min = left_up_lon
    lon_max = right_down_lon
    lat_min = right_down_lat
    lat_max = left_up_lat
    index = np.where(np.logical_and.reduce((lons > lon_min, lons < lon_max, lats > lat_min, lats < lat_max)))
    row_min = np.min(index[0])
    row_max = np.max(index[0])
    col_min = np.min(index[1])
    col_max = np.max(index[1])
    return (row_min, row_max), (col_min, col_max)


def get_area_data_by_index(data=None, row_min=237, row_max=1373, col_min=497, col_max=2262):
    """
    :param data:
    :param row_min:
    :param row_max:
    :param col_min:
    :param col_max:
    :return:
    """
    if data is not None:
        return data[row_min:row_max+1, col_min:col_max+1]
