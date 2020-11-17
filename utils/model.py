#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-07-14 21:23
# @Author  : NingAnMe <ninganme@qq.com>
from collections import defaultdict
import json

import pandas as pd
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, Date
from sqlalchemy.orm import relationship

from utils.database import *
from datetime import date


class Station(Base):
    __tablename__ = "station"

    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String)
    date = Column(Date)
    GHI = Column(Float)
    DBI = Column(Float)
    DHI = Column(Float)
    GTI = Column(Float)
    H0 = Column(Float)
    H20 = Column(Float)
    H25 = Column(Float)

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

    with open('D:\project\py\gz\ky\gffp\提供样例数据\提供样例数据\sta_mon_a_b_ok.txt', "r") as f:
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


if __name__ == '__main__':
    Base.metadata.create_all(engine)
    # t_Station()
    # test_PddSku()
    # txt2db_coef()
    file_in = 'D:\project\py\gz\ky\gffp\提供样例数据\提供样例数据\\2019_rz.txt'
    txt2db_ssi(file_in)
