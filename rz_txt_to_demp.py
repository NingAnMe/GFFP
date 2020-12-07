#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020-12-04 16:12:24
# @Author  : YanKaijie

import numpy as np

# open_file = '/home/kts_project_v1/gffp/aid/2019_rz.txt'
open_file = 'D:\project\py\gz\ky\gffp\\aid\\2019_rz.txt'
# out_dir = '/home/kts_project_v1/gffp/DATA/DEMP'
out_dir = 'D:/project/py/gz/ky/gffp/aid/outdata'
def rz_txt_to_demp():
    with open(open_file) as f:
        lines = f.readlines()
        for i in np.arange(1961, 2019):
            print(i)
            new_lines = ''
            for line in lines:
                line_li = list(filter(None, line.split(" ")))
                print(line_li)
                kg = '  '
                li4 = int(line_li[4]) + 2019 - i
                new_line = '{}{}{}{}{}{}{}{}{}\n'.format(line_li[0], kg, i, kg, line_li[2], kg, line_li[3], kg, li4)
                print(new_line)
                new_lines = new_lines + new_line
            out_file = '{}/{}_rz.txt'.format(out_dir, i)
            with open(out_file, "w", encoding='utf-8') as f:
                f.write(new_lines)
if __name__ == '__main__':
    rz_txt_to_demp()

# python3 rz_txt_to_demp.py