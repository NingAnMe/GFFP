#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-09-01 10:25
# @Author  : NingAnMe <ninganme@qq.com>
from utils.model import Coef, Station, Ssi
from utils.database import session_scope
from utils.path import make_sure_path_exists, AID_PATH
from utils.hdf5 import write_hdf5_and_compress
from utils.data import DemLoader
from utils.config import COEF_TXT, print_config
from user_config import DATA_1KM_MONTH

import os
from datetime import datetime, date
from collections import defaultdict
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd
from scipy.interpolate import griddata

PI = 3.1415926
E = 1366.1


def modiGHI(a, b, r):
    coef = (r[0] * b / 1000 + r[1]) * 0.01
    c = a * (1 + coef)
    return c


def topoCorrection(radiaArray, deltHgt, latitude):
    rr = [[2.6036, 0.0365], [2.6204, 0.0365], [2.6553, 0.0362], [2.6973, 0.0356], [2.7459, 0.0343],
          [2.8012, 0.0324], [2.8616, 0.0299], [2.9236, 0.0257], [2.9870, 0.0204]]

    idx = np.logical_and(latitude >= 52.5, latitude <= 90)
    radiaArray[idx] = modiGHI(radiaArray[idx], deltHgt[idx], rr[8])

    idx = np.logical_and(latitude >= 47.5, latitude < 52.5)
    radiaArray[idx] = modiGHI(radiaArray[idx], deltHgt[idx], rr[7])

    idx = np.logical_and(latitude >= 42.5, latitude < 47.5)
    radiaArray[idx] = modiGHI(radiaArray[idx], deltHgt[idx], rr[6])

    idx = np.logical_and(latitude >= 37.5, latitude < 42.5)
    radiaArray[idx] = modiGHI(radiaArray[idx], deltHgt[idx], rr[5])

    idx = np.logical_and(latitude >= 32.5, latitude < 37.5)
    radiaArray[idx] = modiGHI(radiaArray[idx], deltHgt[idx], rr[4])

    idx = np.logical_and(latitude >= 27.5, latitude < 32.5)
    radiaArray[idx] = modiGHI(radiaArray[idx], deltHgt[idx], rr[3])

    idx = np.logical_and(latitude >= 22.5, latitude < 27.5)
    radiaArray[idx] = modiGHI(radiaArray[idx], deltHgt[idx], rr[2])

    idx = np.logical_and(latitude >= 17.5, latitude < 22.5)
    radiaArray[idx] = modiGHI(radiaArray[idx], deltHgt[idx], rr[1])

    idx = np.logical_and(latitude >= 17.5, latitude < 22.5)
    radiaArray[idx] = modiGHI(radiaArray[idx], deltHgt[idx], rr[1])

    idx = np.logical_and(latitude >= 0, latitude < 17.5)
    radiaArray[idx] = modiGHI(radiaArray[idx], deltHgt[idx], rr[0])

    return radiaArray


def get_EHR(lat, j):
    """
    计算一段时间的EHR
    :param lat: 纬度
    :param j: 第几天
    :return:
    """
    EDNI = E * (1 + 0.033 * np.cos(360.0 * j / 365.0 * PI / 180.0))
    yita = 23.45 * np.sin(360.0 * (284.0 + j) / 365.0 * PI / 180.0)
    ws = np.arccos(-np.tan(lat * PI / 180) * np.tan(yita * PI / 180)) * 180 / PI
    EHR = 24 * 3600 / PI * EDNI \
          * (np.cos(lat * PI / 180) * np.cos(yita * PI / 180) * np.sin(ws * PI / 180)
             + PI * ws / 180.0 * np.sin(lat * PI / 180) * np.sin(yita * PI / 180)) \
          * 10 ** (-6.0)
    return np.sum(EHR) / 3.6


def get_GHR(EHR, a, b, sr):
    GHR = EHR * (a + b * sr * 0.01)
    return GHR


def get_Db(EHR, c, d, sr):
    ss = sr * 0.01
    Db = EHR * (c * ss ** 2 + d * ss)
    return Db


def get_Sdif(GHR, Db):
    Sdif = GHR - Db
    return Sdif


def get_EHR_monthly_pd(pd_data):
    lat = pd_data['lat']  # float
    year = pd_data['year']  # str
    month = pd_data['month']  # str

    return get_EHR_monthly(lat, year, month)


def get_EHR_monthly(latitude: float, year: str, month: str):
    """
    计算一个月的EHR
    :param latitude: 纬度
    :param year: 年份
    :param month: 月份
    :return:
    """
    ym_start = datetime.strptime(year + month, '%Y%m')
    ym_end = ym_start + relativedelta(months=1)
    days = list()
    while ym_start < ym_end:
        days.append(int(ym_start.strftime('%j')))
        ym_start += relativedelta(days=1)
    days = np.array(days)
    return get_EHR(latitude, days)


def get_GHR_Db_Sdif(EHR, a, b, c, d, sr):
    GHR = get_GHR(EHR, a, b, sr)
    Db = get_Db(EHR, c, d, sr)
    Sdif = get_Sdif(GHR, Db)
    return GHR, Db, Sdif


def get_station_info(station_info_file):
    """
    获取 经度、纬度、海拔高度
    :param station_info_file:
    :return:
    """
    station_data = pd.read_csv(station_info_file, sep='\\s+')
    station_data_id = station_data.groupby('id')
    station_info = defaultdict(list)
    for station in station_data_id:
        station_info['id'].append(int(station[1].iloc[0]['id']))
        station_info['lat'].append(station[1].iloc[0]['lat'])
        station_info['lon'].append(station[1].iloc[0]['lon'])
        station_info['hgt'].append(station[1].iloc[0]['hgt'])
    station_info = pd.DataFrame(station_info)
    print(len(station_info['id']))
    return station_info


def get_rizhao_monthly(rizhao_monthly_file):
    """
    读取日照数据
    :param rizhao_monthly_file:
    :return:
    """
    rizhao_data = pd.read_csv(rizhao_monthly_file, sep='\\s+')
    return rizhao_data


def get_GTI_OA(GHR, Db, Sdif, lat, y, m):  # 目前只能计算平年
    glo = GHR.reshape(-1, 1)  # 总
    dir_ = Db.reshape(-1, 1)  # 直
    dif = Sdif.reshape(-1, 1)  # 散
    latid = lat.reshape(-1, 1)
    m = m.reshape(-1, 1)
    m = m.astype(np.int)

    days = np.array([17, 47, 75, 105, 135, 162, 198, 228, 258, 288, 318, 344])
    dmonths = np.array([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
    delts = np.array([-21.1, -13.3, -2.5, 9.1, 18.4, 22.9, 21.5, 14.6, 3.3, -8.2, -18.0, -22.8])

    day = days[m - 1]
    dmonth = dmonths[m - 1]
    delt = delts[m - 1]

    angle = np.arange(0, 91) * PI / 180
    angle = angle.reshape(1, -1)

    delt1 = delt * PI / 180  # delt 太阳赤纬
    dir1 = latid * PI / 180  # dir1  纬度
    ws = np.arccos(-np.tan(dir1) * np.tan(delt1)) * np.ones_like(angle)  # ws   时角
    set_ = -np.tan(dir1 - angle) * np.tan(delt1)
    ws0 = np.full_like(set_, np.nan)
    ws0[set_ > 1] = 0
    idx = np.logical_and(set_ >= -1, set_ <= 1)
    ws0[idx] = np.arccos(set_[idx])
    ws0[set_ < -1] = PI
    idx = ws > ws0
    ws[idx] = ws0[idx]
    ws1 = ws

    # 月地外水平面太阳辐照量  天文辐射
    H0 = 24 * 3600 * E / PI * \
         (1 + 0.033 * np.cos(2 * PI * day / 365)) * \
         (np.cos(dir1) * np.cos(delt1) * np.sin(ws1) +
          ws1 * np.sin(dir1) * np.sin(delt1)) / 1000000 / 3.6 * dmonth  # 月总量 用当月代表日的日辐照*当月日数   !转单位kwh/m2

    rb = (np.cos(dir1 - angle) * np.cos(delt1) * np.sin(ws1) +
          ws1 * np.sin(dir1 - angle) * np.sin(delt1)) / \
         (np.cos(dir1) * np.cos(delt1) * np.sin(ws1) +
          ws1 * np.sin(dir1) * np.sin(delt1))

    sdir = dir_ * rb  # Ds=Dh*Rb  sdir即斜面直接辐射

    sdif = dif * (dir_ * rb / H0 + (1 - dir_ / H0) * ((1 + np.cos(angle)) / 2))  # sdif即倾斜面散射辐射

    sref = glo * (1 - np.cos(angle)) / 2 * 0.2  # sref即倾斜面反射辐射

    sglo = sdir + sdif + sref  # sglo即倾斜面总辐射

    index_max = np.nanargmax(sglo, axis=1)
    index_max_row = np.arange(0, len(index_max), dtype=np.int)
    sglo = sglo[index_max_row, index_max]
    sdir = sdir[index_max_row, index_max]
    sdif = sdif[index_max_row, index_max]
    sref = sref[index_max_row, index_max]
    OA = index_max
    return OA, sglo, sdir, sdif, sref


def set_fillvalue(station):
    """
    将无效的日照数据计算的值填充为 -9999
    :param station:
    :return:
    """
    idx = (station['sr'] < 0) | (station['sh'] < 0)
    for data_name in ['GHI', 'DBI', 'DHI', 'GTI', 'sdir', 'sdif', 'sref', 'OA']:
        station[data_name][idx] = -9999
    return station


def cal_stations(file_rz):
    def __ym2date(pd_data):
        y = pd_data['year']
        m = pd_data['month']
        return date(int(y), int(m), 1)

    # 获取日照数据  station_id year month sh sr
    ssi = Ssi.get_ssi_info_by_file(file_rz)
    if ssi is None:
        return
    ssi = ssi.iloc[0:]  # 测试少量数据

    # 获取站点数据和系数数据 lats lons height a b c d
    # 'station', 'lat', 'lon', 'height', 'm', 'a', 'b', 'c', 'd'
    coef = Coef.get_coef_info_by_file(COEF_TXT)

    # 匹配站点的信息 station_id year month sr sh lats lons height a b c d
    station = pd.merge(ssi, coef, on=['station', 'month'])
    # print(station.head())

    station['date'] = station[['year', 'month']].apply(__ym2date, axis=1)

    # 计算数据
    station['EHR'] = station[['lat', 'year', 'month']].apply(get_EHR_monthly_pd, axis=1)
    # print(station.head())

    EHR = station['EHR'].to_numpy()
    a = station['a'].to_numpy()
    b = station['b'].to_numpy()
    c = station['c'].to_numpy()
    d = station['d'].to_numpy()
    sr = station['sr'].to_numpy()
    GHR, Db, Sdif = get_GHR_Db_Sdif(EHR, a, b, c, d, sr)
    station['GHI'] = GHR
    station['DBI'] = Db
    station['DHI'] = Sdif
    # print(station.head())

    lat = station['lat'].to_numpy()
    year = station['year'].to_numpy()
    month = station['month'].to_numpy()
    OA, sglo, sdir, sdif, sref = get_GTI_OA(GHR, Db, Sdif, lat, year, month)
    station['GTI'] = sglo
    station['sdir'] = sdir
    station['sdif'] = sdif
    station['sref'] = sref
    station['OA'] = OA

    station = set_fillvalue(station)

    dates = station['date'].drop_duplicates().tolist()

    data = list()
    for i, s in station.iterrows():
        data.append(s.to_dict())

    with session_scope() as session:
        Station.delete_by_date(session, dates)
        Station.add(session, data=data)

    return station['date'].min(), station['date'].max()


def get_station_data(ym, session):
    dt = date(int(ym[:4]), int(ym[4:6]), 1)
    data = Station.query_by_date(session, dt)
    data_list = [row.to_dict() for row in data]
    if data_list:
        return pd.DataFrame(data_list)
    return


def add_station_value(datas, data_name):
    """
    补点
    :param datas:
    :param data_name:
    :return:
    """
    # 左点向左3度
    index_lon_min = datas['lon'].idxmin()
    lon = datas['lon'][index_lon_min] - 3
    lat = datas['lat'][index_lon_min]
    points_add = np.array((lon, lat)).reshape(1, 2)
    datas_add = datas[data_name][index_lon_min]

    # 左上点向左上1度
    index = (datas['lon'] < 87) & (datas['lat'] > 48)
    lon = datas['lon'][index].iloc[0]
    lat = datas['lat'][index].iloc[0]
    lon = lon - 1
    lat = lat + 1
    data_ = datas[data_name][index].iloc[0]
    point_ = np.array((lon, lat)).reshape(1, 2)
    points_add = np.concatenate((points_add, point_), axis=0)
    datas_add = np.append(datas_add, data_)

    # 左下点向左下1度
    index = (datas['lon'] < 83) & (datas['lat'] < 31)
    lon = datas['lon'][index].iloc[0]
    lat = datas['lat'][index].iloc[0]
    lon = lon - 1
    lat = lat - 1
    data_ = datas[data_name][index].iloc[0]
    point_ = np.array((lon, lat)).reshape(1, 2)
    points_add = np.concatenate((points_add, point_), axis=0)
    datas_add = np.append(datas_add, data_)

    # 右点向右
    index_lon_max = datas['lon'].idxmax()
    lon = datas['lon'][index_lon_max] + 1.5
    lat = datas['lat'][index_lon_max]
    point_ = np.array((lon, lat)).reshape(1, 2)
    data_ = datas[data_name][index_lon_max]
    points_add = np.concatenate((points_add, point_), axis=0)
    datas_add = np.append(datas_add, data_)

    # 下点向下
    index_lat_min = datas['lat'].idxmin()
    lon = datas['lon'][index_lat_min]
    lat = datas['lat'][index_lat_min] - 0.8
    point_ = np.array((lon, lat)).reshape(1, 2)
    data_ = datas[data_name][index_lat_min]
    points_add = np.concatenate((points_add, point_), axis=0)
    datas_add = np.append(datas_add, data_)

    # 上点向上
    index_lat_max = datas['lat'].idxmax()
    lon = datas['lon'][index_lat_max]
    lat = datas['lat'][index_lat_max] + 1.5
    point_ = np.array((lon, lat)).reshape(1, 2)
    data_ = datas[data_name][index_lat_max]
    points_add = np.concatenate((points_add, point_), axis=0)
    datas_add = np.append(datas_add, data_)
    return points_add, datas_add


def grid_data(datas, lons_grid, lats_grid, data_name):
    points = datas[['lon', 'lat']].to_numpy()
    data = datas[data_name].to_numpy()

    # 剔除无效值
    idx = np.logical_and(data > 0, data < 200)
    points = points[idx]
    data = data[idx]
    print('station data：', data.min(), data.max(), data.mean())

    # 补点
    # points_add, data_add = add_station_value(datas, data_name)
    # points = np.concatenate((points, points_add), axis=0)
    # data = np.append(data, data_add)

    data_grid = griddata(points, data, (lons_grid, lats_grid), method='linear')
    data_grid[data_grid < 0] = 0
    data_grid[data_grid > 200] = 200

    print('data grid：', np.nanmin(data_grid), np.nanmax(data_grid), np.nanmean(data_grid))

    return data_grid


def drop_fillvalue(data_month):
    """
    剔除包含无效值的数据
    :param data_month:
    :return:
    """
    idx = data_month[(data_month['sr'] < 0)].index
    data_month.drop(idx, inplace=True)
    return data_month


def cal_1km(date_min, date_max):
    date_tmp = date_min
    demloder = DemLoader()
    dem = demloder.get_data()
    lons_grid, lats_grid = demloder.get_lon_lat()
    while date_tmp <= date_max:
        ym = date_tmp.strftime("%Y%m")

        with session_scope() as session:
            data_month = get_station_data(ym, session)  # pd.DataFrame
            if data_month is None:
                print('没有数据: {}'.format(ym))
                return
            else:
                data_month = drop_fillvalue(data_month)
                # print(data_month.head())
        for data_name in ['GHI', 'DBI', 'DHI', 'GTI']:
            out_dir = os.path.join(DATA_1KM_MONTH, data_name)
            make_sure_path_exists(out_dir)
            filename = '{}_{}.hdf'.format(data_name, ym)
            out_file = os.path.join(out_dir, filename)
            # if os.path.isfile(out_file):
            #     print('already exist {}'.format(out_file))
            #     continue

            data_grid = grid_data(data_month, lons_grid, lats_grid, data_name)

            dem[dem == -9999] = np.nan

            data_grid = topoCorrection(data_grid, dem, lats_grid)
            print('data_modi：', np.nanmin(data_grid), np.nanmax(data_grid), np.nanmean(data_grid))

            out_data = {
                data_name: data_grid,
                'lon': lons_grid,
                'lat': lats_grid,
            }

            write_hdf5_and_compress(out_data, out_file)
        date_tmp += relativedelta(months=1)


def get_h0_h20_h25():
    """
    # !!!!首年利用小时数=30年平均的最佳斜面总辐射*0.8
    # !!!!20年平均利用小时数=30年平均的最佳斜面总辐射*0.8*0.92
    # !!!!25年平均利用小时数=30年平均的最佳斜面总辐射*0.8*0.9(暂定)
    snly = out_sglo * 0.8
    pjly20 = out_sglo * 0.8 * 0.92
    pjly25 = out_sglo * 0.8 * 0.9
    :return:
    """
    pass


def t_station_info():
    station_info_file = 'slope.txt'
    get_station_info(station_info_file)


# def t_():
#     lat = 52.35
#     lon = 124.72
#     h = 361.90
#     y = '2019'
#     m = '01'
#     a = 4.1571
#     b = 3.8568
#     c = 2.1423
#     d = 4.9584
#     s = 20
#     for m in range(1, 12 + 1):
#         m = str(m)
#         ehr = get_EHR_monthly(lat, y, m)
#         print(ehr)
#         ghr, db, sdif = get_GHR_Db_Sdif(ehr, a, b, c, d, s)
#         print(ghr, db, sdif)
#         lat = np.array(lat)
#         lon = np.array(lon)
#         h = np.array(h)
#         OA, sglo, sdir, sdif, sref = get_GTI_OA(ghr, db, sdif, lat, y, m)
#         print(OA, sglo, sdir, sdif, sref)


def t_cal_station():
    file_rz = os.path.join(AID_PATH, '2019_rz.txt')

    cal_stations(file_rz)


if __name__ == '__main__':
    # t_station_info()
    # t_()
    # t_cal_station()
    cal_1km(date(1990, 1, 1), date(2019, 12, 1))
    # cal_1km(date(1990, 1, 1), date(2019, 12, 1))
    """
    stationInfoFile : str 站点信息
    stationSolarFile :str 光伏数据
    stationAbFile : str ab系数
    demFile : str 1kmDEM
    lonLatFile :str 贫困村经纬度"""
    import argparse

    parser = argparse.ArgumentParser(description='参数计算')
    parser.add_argument('--stationSolarFile', '-m', help='光伏数据', required=False)
    args = parser.parse_args()

    print_config()
    print('stationSolarFile == {}'.format(args.stationSolarFile))
    if os.path.isfile(args.stationSolarFile):
        print('开始计算光伏参数')
        date_min_date_max = cal_stations(args.stationSolarFile)

        print('date_min_date_max == {}'.format(date_min_date_max))
        if date_min_date_max is not None:
            print('开始生成1KM文件')
            cal_1km(*date_min_date_max)
            print('finish')
            exit(0)
        else:
            exit(-1)
    else:
        print('ERROR: 输入参数错误，文件不存在：{}'.format(args.stationSolarFile))
        exit(-1)
