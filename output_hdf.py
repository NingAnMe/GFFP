import os
import os.path
from netCDF4 import Dataset
import matplotlib.pyplot as plt
import h5py
import math
import numpy as np
import datetime
import re


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


def getFiles(dir, suffix):  # 查找根目录，文件后缀
    res = []
    for root, directory, files in os.walk(dir):  # =>当前根,根下目录,目录下的文件
        for filename in files:
            name, suf = os.path.splitext(filename)  # =>文件名,文件后缀
            if suf == suffix:
                res.append(os.path.join(root, filename))  # =>吧一串字符串组合成路径
    return res


if __name__ == '__main__':
    nx, ny = (4501, 7001)
    wuri_file_path = r'H:\modi'
    wuri_files_ghi = getFiles(os.path.join(wuri_file_path, 'hghr'), '.txt')
    wuri_files_gti = getFiles(os.path.join(wuri_file_path, 'sglo'), '.txt')
    ghi_out_path = r'H:\cz1km\hdf_ghi'
    gti_out_path = r'H:\cz1km\hdf_gti'
    file_dem = r'H:\cz1km\D_DEM.HDF'
    # #总辐射
    with h5py.File(file_dem, 'r') as f:
        dem = f.get('dem')[:]
        lat = f.get('lat')[:]
        lon = f.get('lon')[:]
    for file in wuri_files_ghi[20:24]:
        data = np.loadtxt(file)
        print(file)
        # print(data[531, 3063])
        number = re.findall('\d+', os.path.basename(file))
        date = number[0] + number[1].zfill(2)
        path_out = os.path.join(ghi_out_path, 'GHI_' + date + '.HDF')
        print('GHI：', np.nanmin(data), np.nanmax(data), np.nanmean(data))
        data = topoCorrection(data, dem, lat)
        print('GHI correc：', np.nanmin(data), np.nanmax(data), np.nanmean(data))
        with h5py.File(path_out, "w") as f:
            d2 = f.create_dataset("GHI", (nx, ny), 'float', data=data, compression='gzip', compression_opts=5,
                                  shuffle=True)
            d3 = f.create_dataset("lon", (nx, ny), 'float', data=lon, compression='gzip', compression_opts=5,
                                  shuffle=True)
            d4 = f.create_dataset("lat", (nx, ny), 'float', data=lat, compression='gzip', compression_opts=5,
                                  shuffle=True)
    # #斜面
    for file in wuri_files_gti[20:24]:
        data = np.loadtxt(file)
        print(file)
        # print(data[531, 3063])
        number = re.findall('\d+', os.path.basename(file))
        date = number[0] + number[1].zfill(2)
        # date = os.path.basename(file)[4:8]+os.path.basename(file)[10:12]
        path_out = os.path.join(gti_out_path, 'GTI_' + date + '.HDF')
        print('GTI：', np.nanmin(data), np.nanmax(data), np.nanmean(data))
        data = topoCorrection(data, dem, lat)
        print('GTI correc：', np.nanmin(data), np.nanmax(data), np.nanmean(data))
        with h5py.File(path_out, "w") as f:
            d2 = f.create_dataset("GTI", (nx, ny), 'float', data=data, compression='gzip', compression_opts=5,
                                  shuffle=True)
            d3 = f.create_dataset("lon", (nx, ny), 'float', data=lon, compression='gzip', compression_opts=5,
                                  shuffle=True)
            d4 = f.create_dataset("lat", (nx, ny), 'float', data=lat, compression='gzip', compression_opts=5,
                                  shuffle=True)
