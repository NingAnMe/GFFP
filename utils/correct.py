#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2021-02-08 10:12
# @Author  : NingAnMe <ninganme@qq.com>
import numpy as np


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


data_grid = None  # 待订正网格数据
dem = None  # dem网格数据
lats_grid = None  # 纬度网格数据
print('data correc：', np.nanmin(data_grid), np.nanmax(data_grid), np.nanmean(data_grid))
data_correct_grid = topoCorrection(data_grid, dem, lats_grid)
print('data correc：', np.nanmin(data_correct_grid), np.nanmax(data_correct_grid), np.nanmean(data_correct_grid))
