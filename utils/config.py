#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-06-15 17:04
# @Author  : NingAnMe <ninganme@qq.com>

from utils.path import AID_PATH

sqlite_db = r'sqlite:///{}\db.db'.format(AID_PATH)
order_file_path = r'C:\D\OneDrive\Business\库存处理订单'
json_file_path = r'C:\D\OneDrive\Business\json数据'


def get_datatype():
    datatype = ['GHI', 'DBI', 'DHI', 'GTI']
    return datatype
