#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-11-17 9:29
# @Author  : NingAnMe <ninganme@qq.com>
from utils.path import *
from utils.config import DEM_TXT, DEM_HDF
from utils.hdf5 import get_hdf5_data, write_hdf5_and_compress

import os

import numpy as np


class DemLoader:

    def __init__(self):
        self.file_txt = os.path.join(DEM_TXT)
        self.file_hdf = os.path.join(DEM_HDF)
        if not os.path.isfile(self.file_hdf):
            self.to_hdf5()

    def get_data(self):
        return get_hdf5_data(self.file_hdf, 'dem', 1, 0, [-9000, 9000], np.nan)

    def get_lon_lat(self):
        lons = get_hdf5_data(self.file_hdf, 'lon', 1, 0, [-180, 180], np.nan)
        lats = get_hdf5_data(self.file_hdf, 'lat', 1, 0, [-90, 90], np.nan)
        return lons, lats

    def to_hdf5(self):
        # dem
        data = np.loadtxt(self.file_txt, skiprows=6)

        # lon, lat
        ncols = 7001
        nrows = 4501
        xllcorner = 69.9951
        yllcorner = 9.995
        cellsize = 0.01
        laty = np.arange(nrows - 1, 0 - 1, -1) * cellsize + yllcorner
        lonx = np.arange(0, ncols) * cellsize + xllcorner
        lons, lats = np.meshgrid(lonx, laty)

        d_hdf = {
            'dem': data,
            'lon': lons,
            'lat': lats,
        }
        write_hdf5_and_compress(d_hdf, self.file_hdf)


def t_DemLoader():
    dem_l = DemLoader()
    print(dem_l.get_data())


if __name__ == '__main__':
    t_DemLoader()
