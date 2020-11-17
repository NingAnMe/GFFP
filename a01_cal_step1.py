#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-09-01 10:25
# @Author  : NingAnMe <ninganme@qq.com>

from datetime import datetime
from collections import defaultdict
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd

PI = 3.1415926
E = 1366.1


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
    return np.sum(EHR)


def get_GHR(EHR, a, b, s):
    GHR = EHR * (a + b * s * 0.01)
    return GHR


def get_Db(EHR, c, d, s):
    ss = s * 0.01
    Db = EHR * (c * ss ** 2 + d * ss)
    return Db


def get_Sdif(GHR, Db):
    Sdif = GHR - Db
    return Sdif


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


def get_GHR_Db_Sdif(EHR, a, b, c, d, s):
    GHR = get_GHR(EHR, a, b, s)
    Db = get_Db(EHR, c, d, s)
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


def get_GTI_OA(GHR, Db, Sdif, lat, lon, hgt, y, m):
    glo = GHR.reshape(-1, 1)  # 总
    dir_ = Db.reshape(-1, 1)  # 直
    dif = Sdif.reshape(-1, 1)  # 散
    latid = lat.reshape(-1, 1)
    lonid = lon.reshape(-1, 1)
    hgtid = hgt.reshape(-1, 1)
    yyid = y
    m = int(m)

    days = [17, 47, 75, 105, 135, 162, 198, 228, 258, 288, 318, 344]
    dmonths = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    delts = [-21.1, -13.3, -2.5, 9.1, 18.4, 22.9, 21.5, 14.6, 3.3, -8.2, -18.0, -22.8]

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
    print('1', np.min(set_), np.max(set_), np.mean(set_))
    ws0[set_ > 1] = 0
    idx = np.logical_and(set_ >= -1, set_ <= 1)
    ws0[idx] = np.arccos(set_[idx])
    ws0[set_ < -1] = PI
    idx = ws > ws0
    ws[idx] = ws0[idx]
    ws1 = ws

    # 月地外水平面太阳辐照量  天文辐射
    H0 = 24 * 3600 * 1366.1 / PI * \
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
    sglo = sglo[:, index_max]
    sdir = sdir[:, index_max]
    sdif = sdif[:, index_max]
    sref = sref[:, index_max]
    OA = index_max
    return OA, sglo, sdir, sdif, sref


def cal_stations(file_rz):
    station_info = Coef.get_station_info()
    stations = Coef.get_stations()

    ssi = Ssi.get_ssi_info(file_rz)



def get_best_slope_monthly(GHR, Db, Sdif, lat, lon, hgt, yy, delt, day, dmonth, ):
    """
    :param GHR: GHI
    :param Db: DBI
    :param Sdif: DHI
    :param lat:
    :param lon:
    :param hgt:
    :param yy:
    :param delt:
    :param day:  一年中的第几天
    :param dmonth:  本月有多少天
    :return:
    """
    glo = GHR  # 总
    dir_ = Db  # 直
    dif = Sdif  # 散
    latid = lat
    lonid = lon
    hgtid = hgt
    yyid = yy

    out_sglo = np.array((lat.shape[0], 12))
    out_sdir = np.array((lat.shape[0], 12))
    out_sdif = np.array((lat.shape[0], 12))
    out_sref = np.array((lat.shape[0], 12))

    sgloy1 = 0
    for ag in range(0, 91):
        angle = ag * PI / 180
        sdiry = 0
        sdify = 0
        srefy = 0
        sgloy = 0

        if glo != 9999:
            delt1 = delt * PI / 180  # delt 太阳赤纬
            dir1 = latid * PI / 180  # dir1  纬度
            ws = np.arccos(-np.tan(dir1) * np.tan(delt1))  # ws   时角
            set_ = -np.tan(dir1 - angle) * np.tan(delt1)
            if set_ > 1:
                ws0 = 0
            elif 1 >= set_ >= -1:
                ws0 = np.arccos(set_)
            else:
                ws0 = PI
            if ws > ws0:
                ws = ws0
            ws1 = ws

            # 月地外水平面太阳辐照量  天文辐射
            H0 = 24 * 3600 * 1366.1 / PI * \
                 (1 + 0.033 * np.cos(2 * PI * day / 365)) * \
                 (np.cos(dir1) * np.cos(delt1) * np.sin(ws1) +
                  ws1 * np.sin(dir1) * np.sin(delt1)) / 1000000 / 3.6 * dmonth  # 月总量 用当月代表日的日辐照*当月日数   !转单位kwh/m2

            rb = (np.cos(dir1 - angle) * np.cos(delt1) * np.sin(ws1) +
                  ws1 * np.sin(dir1 - angle) * np.sin(delt1)) / \
                 (np.cos(dir1) * np.cos(delt1) * np.sin(ws1) +
                  ws1 * np.sin(dir1) * np.sin(delt1))

            sdir = dir_ * rb  # Ds=Dh*Rb  sdir即斜面直接辐射
            # todo 12个月的总和，需要处理一下计算的位置
            sdiry = sdiry + sdir  # sdiry     12个月sdir总和

            sdif = dif * (dir_ * rb / H0 + (1 - dir_ / H0) * ((1 + np.cos(angle)) / 2))  # sdif即倾斜面散射辐射
            # todo 12个月的总和，需要处理一下计算的位置
            sdify = sdify + sdif

            sref = glo * (1 - np.cos(angle)) / 2 * 0.2  # sref即倾斜面反射辐射
            # todo 12个月的总和，需要处理一下计算的位置
            srefy = srefy + sref

            sglo = sdir + sdif + sref  # sglo即倾斜面总辐射
            # todo 12个月的总和，需要处理一下计算的位置
            sgloy = sgloy + sglo

        else:
            sgloy = 0
            sglo = 9999
            sdir = 9999
            sdif = 9999
            sref = 9999

        # 计算最佳倾角和最佳斜面总辐射
        if sgloy > sgloy1:
            sgloy1 = sgloy
            sdiry1 = sdiry
            sdify1 = sdify
            srefy1 = srefy
            for i in range(1, 13):
                out_sglo[:, i] = sglo[:, i]
                out_sdir[:, i] = sdir[:, i]
                out_sdif[:, i] = sdif[:, i]
                out_sref[:, i] = sref[:, i]
            ag1 = ag  # 记录角度

    # !!!!首年利用小时数=30年平均的最佳斜面总辐射*0.8
    # !!!!20年平均利用小时数=30年平均的最佳斜面总辐射*0.8*0.92
    # !!!!25年平均利用小时数=30年平均的最佳斜面总辐射*0.8*0.9(暂定)
    snly = out_sglo * 0.8
    pjly20 = out_sglo * 0.8 * 0.92
    pjly25 = out_sglo * 0.8 * 0.9


def t_station_info():
    station_info_file = 'slope.txt'
    get_station_info(station_info_file)


def t_():
    lat = 52.35
    lon = 124.72
    h = 361.90
    y = '2019'
    m = '01'
    a = 4.1571
    b = 3.8568
    c = 2.1423
    d = 4.9584
    s = 20
    for m in range(1, 12 + 1):
        m = str(m)
        ehr = get_EHR_monthly(lat, y, m)
        print(ehr)
        ghr, db, sdif = get_GHR_Db_Sdif(ehr, a, b, c, d, s)
        print(ghr, db, sdif)
        lat = np.array(lat)
        lon = np.array(lon)
        h = np.array(h)
        OA, sglo, sdir, sdif, sref = get_GTI_OA(ghr, db, sdif, lat, lon, h, y, m)
        print(OA, sglo, sdir, sdif, sref)


if __name__ == '__main__':
    t_station_info()
    t_()
