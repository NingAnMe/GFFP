#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-06-13 11:50
# @Author  : NingAnMe <ninganme@qq.com>
"""
初始化数据库表：https://www.jianshu.com/p/af070e872d00
字段设置默认值：https://www.jianshu.com/p/6d3ec5851f3a
一对多：https://blog.csdn.net/chenmozhe22/article/details/95607372
批量更新：https://blog.csdn.net/aaaaaaazhaofeng/article/details/99670667
多对多中间表添加额外信息：https://docs.sqlalchemy.org/en/13/orm/basic_relationships.html#association-object
    https://segmentfault.com/q/1010000022270316/a-1020000022274515
"""
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from utils.config import SQLLITE_DB as database_url

print("数据库engine：{database_url}".format(database_url=database_url))
# engine = create_engine(database_url, echo=True, pool_size=10, max_overflow=5)
engine = create_engine(database_url, echo=False)
Session = sessionmaker(engine)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
