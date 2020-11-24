#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-06-15 17:05
# @Author  : NingAnMe <ninganme@qq.com>
import os


def make_sure_path_exists(path):
    if not os.path.isdir(path):
        os.makedirs(path)
        print('创建文件夹：{path}'.format(path=path))


# 如果需要编译，使用下面的方法获取根目录
#  执行 python3 a.py ,:::: sys.argv[0] 就是a.py的路径，无论a.py在哪个目录，如果写sys.argv[0]，只能运行顶层文件
# import sys
# ROOT_PATH = os.path.dirname(os.path.realpath(sys.argv[0]))

# 如果不需要编译，使用下面的方法获取根目录
ROOT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

# 其他目录
LIB_PATH = os.path.join(ROOT_PATH, 'utils')
AID_PATH = os.path.join(ROOT_PATH, 'aid')
TEST_PATH = os.path.join(ROOT_PATH, 'test')
