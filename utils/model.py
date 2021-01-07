#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-07-14 21:23
# @Author  : NingAnMe <ninganme@qq.com>
import os
from datetime import date, datetime

import pandas as pd
from sqlalchemy import Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base


from utils.database import session_scope, engine
from utils.path import AID_PATH
from utils.config import DB_PATH


Base = declarative_base()


def to_dict(self):
    model_dict = dict(self.__dict__)
    del model_dict['_sa_instance_state']
    for k, v in model_dict.items():
        if isinstance(v, datetime):
            model_dict[k] = model_dict[k].strftime('%Y-%m-%d %H:%M:%S')
    return model_dict


Base.to_dict = to_dict


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


class Village(Base):
    __tablename__ = "village"

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
        if isinstance(data, Village):
            session.add(data)
        elif isinstance(data, dict):
            data = Village(**data)
            session.add(data)
        elif isinstance(data, list):
            session.bulk_insert_mappings(Village, data)
        else:
            print('type error')

    @classmethod
    def update(cls, session, data):
        if isinstance(data, Village):
            session.update(data)
        elif isinstance(data, dict):
            data = Village(**data)
            session.update(data)
        elif isinstance(data, list):
            session.bulk_update_mappings(Village, data)
        else:
            print('type error')

    @classmethod
    def query(cls, session):
        return session.query(Village).all()

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
            Village.add(session, ds)
            rows = Village.query(session)
            for row in rows:
                print(row)
                print(row.to_dict()
                      )

    @classmethod
    def get_village_info_by_file(cls, file_in):
        """
        :param file_in:
        :return:
        """
        dtype = {
            0: str,
            1: str,
            2: str,
            3: str,
            4: str,
            5: str,
            6: str,
            7: float,
            8: float,
        }
        data = pd.read_excel(file_in, index_col=0, dtype=dtype)
        data.columns = ['code', 'sheng', 'shi', 'xian', 'xiang', 'cun', 'lat', 'lon']
        return data


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
    # from utils.config import POOR_XLSX
    # data = Poor.get_poor_info_by_file(POOR_XLSX)
    # print(data.head())
