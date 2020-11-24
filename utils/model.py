#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-07-14 21:23
# @Author  : NingAnMe <ninganme@qq.com>
import os
from datetime import date
from collections import defaultdict
import json

import pandas as pd
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, Date
from sqlalchemy.orm import relationship

from utils.database import *
from utils.path import *
from utils.config import DB_PATH


class Station(Base):
    __tablename__ = "station"

    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String)
    date = Column(Date)
    year = Column(String)
    month = Column(String)
    sh = Column(Integer)
    sr = Column(Integer)

    EHR = Column(Float)
    GHI = Column(Float)
    DBI = Column(Float)
    DHI = Column(Float)
    GTI = Column(Float)
    sdir = Column(Float)
    sdif = Column(Float)
    sref = Column(Float)

    lat = Column(Float)
    lon = Column(Float)
    height = Column(Float)
    a = Column(Float)
    b = Column(Float)
    c = Column(Float)
    d = Column(Float)

    @classmethod
    def str2datetime(cls, date_str):
        date_str = str(date_str)
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except Exception as why:
            str(why)
            return None

    @classmethod
    def add(cls, session, data):
        if isinstance(data, Station):
            session.add(data)
        elif isinstance(data, dict):
            data = Station(**data)
            session.add(data)
        elif isinstance(data, list):
            session.bulk_insert_mappings(Station, data)
        else:
            print('type error')

    @classmethod
    def update(cls, session, data):
        if isinstance(data, Station):
            session.update(data)
        elif isinstance(data, dict):
            data = Station(**data)
            session.update(data)
        elif isinstance(data, list):
            session.bulk_update_mappings(Station, data)
        else:
            print('type error')

    @classmethod
    def query(cls, session):
        return session.query(Station).all()

    @classmethod
    def query_by_station(cls, session, station):
        return session.query(Station).filter(Station.station == station.strip()).all()

    @classmethod
    def query_by_date(cls, session, dt):
        return session.query(Station).filter(Station.date == dt).all()


class Ssi(Base):
    __tablename__ = "ssi"

    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String)
    date = Column(Date)
    sh = Column(Integer)
    sr = Column(Integer)

    @classmethod
    def str2datetime(cls, date_str):
        date_str = str(date_str)
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except Exception as why:
            str(why)
            return None

    @classmethod
    def add(cls, session, data):
        if isinstance(data, Ssi):
            session.add(data)
        elif isinstance(data, dict):
            data = Ssi(**data)
            session.add(data)
        elif isinstance(data, list):
            session.bulk_insert_mappings(Ssi, data)
        else:
            print('type error')

    @classmethod
    def update(cls, session, data):
        if isinstance(data, Ssi):
            session.update(data)
        elif isinstance(data, dict):
            data = Ssi(**data)
            session.update(data)
        elif isinstance(data, list):
            session.bulk_update_mappings(Ssi, data)
        else:
            print('type error')

    @classmethod
    def query(cls, session):
        return session.query(Ssi).all()

    @classmethod
    def get_ssi_info_by_file(cls, file_in):
        """
        :param file_in: station_id year month sh sr
        :return:
        """
        dtype = {
            0: str,
            1: str,
            2: str,
            3: int,
            4: int,
        }
        try:
            data = pd.read_csv(file_in, sep=r'\s+', index_col=False, header=None, dtype=dtype)
        except pd.errors.EmptyDataError:
            return
        data.columns = ['station', 'year', 'month', 'sh', 'sr']
        return data


class Coef(Base):
    __tablename__ = "coef"

    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String)
    month = Column(Integer)
    lat = Column(Float)
    lon = Column(Float)
    hight = Column(Float)
    a = Column(Float)
    b = Column(Float)
    c = Column(Float)
    d = Column(Float)

    @classmethod
    def str2datetime(cls, date_str):
        date_str = str(date_str)
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except Exception as why:
            str(why)
            return None

    @classmethod
    def add(cls, session, data):
        if isinstance(data, Coef):
            session.add(data)
        elif isinstance(data, dict):
            data = Coef(**data)
            session.add(data)
        elif isinstance(data, list):
            session.bulk_insert_mappings(Coef, data)
        else:
            print('type error')

    @classmethod
    def update(cls, session, data):
        if isinstance(data, Coef):
            session.update(data)
        elif isinstance(data, dict):
            data = Coef(**data)
            session.update(data)
        elif isinstance(data, list):
            session.bulk_update_mappings(Coef, data)
        else:
            print('type error')

    @classmethod
    def query(cls, session):
        return session.query(Coef).all()

    @classmethod
    def get_coef_info_by_file(cls, file_in):
        """
        :param file_in:
        :return:
        """
        dtype = {
            0: str,
            1: float,
            2: float,
            3: float,
            4: str,
            5: float,
            6: float,
            7: float,
            8: float,
        }
        data = pd.read_csv(file_in, sep=r'\s+', index_col=False, header=None, dtype=dtype)
        data.columns = ['station', 'lat', 'lon', 'height', 'month', 'a', 'b', 'c', 'd']
        return data


class Poor(Base):
    __tablename__ = "poor"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String)
    sheng = Column(String)
    shi = Column(String)
    xian = Column(String)
    zhen = Column(String)
    cun = Column(String)
    lat = Column(Float)
    lon = Column(Float)

    @classmethod
    def str2datetime(cls, date_str):
        date_str = str(date_str)
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except Exception as why:
            str(why)
            return None

    @classmethod
    def add(cls, session, data):
        if isinstance(data, Poor):
            session.add(data)
        elif isinstance(data, dict):
            data = Poor(**data)
            session.add(data)
        elif isinstance(data, list):
            session.bulk_insert_mappings(Poor, data)
        else:
            print('type error')

    @classmethod
    def update(cls, session, data):
        if isinstance(data, Poor):
            session.update(data)
        elif isinstance(data, dict):
            data = Poor(**data)
            session.update(data)
        elif isinstance(data, list):
            session.bulk_update_mappings(Poor, data)
        else:
            print('type error')

    @classmethod
    def query(cls, session):
        return session.query(Poor).all()

    @classmethod
    def excil2db_poor(cls, ex):
        exop = pd.read_excel(ex)
        va = exop.values
        with session_scope() as session:
            ds = []
            for data_m in va:
                d = {
                    'code': str(data_m[1]),
                    'sheng': str(data_m[2]),
                    'shi': str(data_m[3]),
                    'xian': str(data_m[4]),
                    'zhen': str(data_m[5]),
                    'cun': str(data_m[6]),
                    'lat': float(data_m[7]),
                    'lon': float(data_m[8]),
                }
                ds.append(d)
            Poor.add(session, ds)
            rows = Poor.query(session)
            for row in rows:
                print(row)
                print(row.to_dict()
                      )


def txt2db_ssi(file_in):
    data = []
    with open(str(file_in), "r") as f:
        for i in f:
            data.append([j for j in i.split()])
    with session_scope() as session:
        ds = []
        for data_m in data:
            d = {
                'station': data_m[0],
                'date': date(int(data_m[1]), int(data_m[2]), 1),
                'sh': int(data_m[3]),
                'sr': int(data_m[4]),
            }
            ds.append(d)
        Ssi.add(session, ds)
        rows = Ssi.query(session)
        for row in rows:
            print(row)
            print(row.to_dict())


def txt2db_coef():
    data = []

    with open(os.path.join(AID_PATH, 'sta_mon_a_b_ok.txt'), "r") as f:
        for line in f.readlines():
            temp = line.split()
            data.append(temp)
    ds = []
    with session_scope() as session:
        for data_m in data:
            d = {
                'station': data_m[0],
                'lat': float(data_m[1]),
                'lon': float(data_m[2]),
                'hight': float(data_m[3]),
                'month': int(data_m[4]),
                'a': float(data_m[5]),
                'b': float(data_m[6]),
                'c': float(data_m[7]),
                'd': float(data_m[8]),
            }
            ds.append(d)
        Coef.add(session, ds)
        rows = Coef.query(session)
        for row in rows:
            print(row)
            print(row.to_dict()
                  )


def t_Station():
    with session_scope() as session:
        d = Station(
            station='50246',
            date=date(2020, 1, 1),
            GHI=1000,
            DBI=1000,
            DHI=1000,
            GTI=1000,
            H0=1000,
            H20=1000,
            H25=1000
        )
        Station.add(session, d)

        d = {
            'station': '50246',
            'date': date(2020, 1, 1),
            'GHI': 1000,
            'DBI': 1000,
            'DHI': 1000,
            'GTI': 1000,
            'H0': 1000,
            'H20': 1000,
            'H25': 1000
        }
        Station.add(session, d)

        ds = [
            {
                'station': '50247',
                'date': date(2020, 1, 1),
                'GHI': 1000,
                'DBI': 1000,
                'DHI': 1000,
                'GTI': 1000,
                'H0': 1000,
                'H20': 1000,
                'H25': 1000
            },
            {
                'station': '50248',
                'date': date(2020, 1, 1),
                'GHI': 1000,
                'DBI': 1000,
                'DHI': 1000,
                'GTI': 1000,
                'H0': 1000,
                'H20': 1000,
                'H25': 1000
            },
        ]
        Station.add(session, ds)

        rows = Station.query(session)
        for row in rows:
            print(row)
            print(row.to_dict())
        rows = Station.query_by_station(session, '50246')
        for row in rows:
            print(row)
            print(row.to_dict())


if not os.path.isfile(DB_PATH):
    print('数据库不存在，创建数据库:{}'.format(DB_PATH))
    Base.metadata.create_all(engine)
    if os.path.isfile(DB_PATH):
        print('成功创建数据库：{}'.format(DB_PATH))
    else:
        raise FileExistsError('创建数据库失败')


if __name__ == '__main__':
    Base.metadata.create_all(engine)
    # t_Station()
    # test_PddSku()
    # txt2db_coef()
    # file_in = 'D:\project\py\gz\ky\gffp\提供样例数据\提供样例数据\\2019_rz.txt'
    # txt2db_ssi(file_in)
