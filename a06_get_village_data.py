#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2020-12-23 09:48:35
# @Author  : Yan Kaijie

from utils.model import Village
from utils.config import POOR_XLSX
from a04_data_statistics import data_statistics
import argparse
import numpy as np
import json

'''
全国太阳能资源偏高的贫困村占比约${villagehighper}，
偏高百分比超过5%的贫困村占比约${ willagehighfiveper}，
其中最高值出现在${ willagehigh}，距平百分率为${xillagehighanomaly}
全国太阳能资源偏低的贫困村占比约$ {villagelowper}，
偏低百分比低于-5%的贫困村占比约$ { villagelowfiveper}，
其中最低值出现在${villagelow}，距平百分率为${villagelowanomaly}
'''


def get_village_data(date_start, date_end, date_choice):
    village_info = Village.get_village_info_by_file(POOR_XLSX)
    lon = village_info['lon'].to_numpy()
    lat = village_info['lat'].to_numpy()
    sheng = village_info['sheng'].to_numpy()
    xian = village_info['xian'].to_numpy()
    xiang = village_info['xiang'].to_numpy()
    nov = 0  # 村庄数量
    h_v = 0  # 偏高
    l_v = 0  # 偏低
    h_v_5 = 0  # 高 5
    l_v_5 = 0  # 低 5
    data_village_dic, lon, lat = data_statistics(data_type='GHI',
                                                 mode_type='point',
                                                 task_choice='yearAnomaly',
                                                 date_start=date_start,
                                                 date_end=date_end,
                                                 date_choice=str(int(date_choice) - 1),
                                                 left_longitude=lon,
                                                 left_latitude=lat,
                                                 )
    data_village = None

    for ke, data in data_village_dic.items():
        data_village = data
    for da in data_village:
        if da != 'nan':
            nov += 1
            if da > 0:
                h_v += 1
                if da > 5:
                    h_v_5 += 1
            else:
                l_v += 1
                if da < 5:
                    l_v_5 += 1
    villagehighper = '{}%'.format(round(h_v / nov * 100, 2))
    villagelowper = '{}%'.format(round(l_v / nov * 100, 2))
    willagehighfiveper = '{}%'.format(round(h_v_5 / nov * 100, 2))
    villagelowfiveper = '{}%'.format(round(l_v_5 / nov * 100, 2))
    xillagehighano = np.nanmax(data_village)
    villagelowano = np.nanmin(data_village)
    xillagehighanomaly = '{}%'.format(round(float(xillagehighano), 2))
    villagelowanomaly = '{}%'.format(round(float(villagelowano), 2))
    xillagehighanomaly_choice = int(np.argwhere(data_village == xillagehighano))
    villagelowanomaly_choice = int(np.argwhere(data_village == villagelowano))
    willagehigh_sheng = sheng[xillagehighanomaly_choice]
    willagehigh_xian = xian[xillagehighanomaly_choice]
    willagehigh_xiang = xiang[xillagehighanomaly_choice]
    villagelow_sheng = sheng[villagelowanomaly_choice]
    villagelow_xian = xian[villagelowanomaly_choice]
    villagelow_xiang = xiang[villagelowanomaly_choice]
    willagehigh = willagehigh_sheng + willagehigh_xian + willagehigh_xiang
    villagelow = villagelow_sheng + villagelow_xian + villagelow_xiang
    print(
        '全国太阳能资源偏高的贫困村占比约{}，偏高百分比超过5%的贫困村占比约{}，其中最高值出现在{}，距平百分率为{}全国太阳能资源偏低的贫困村占比约{}，偏低百分比低于-5%的贫困村占比约{}，其中最低值出现在${}，距平百分率为{}'.format(
            villagehighper, willagehighfiveper, willagehigh, xillagehighanomaly, villagelowper, villagelowfiveper,
            villagelow, villagelowanomaly))
    dat_rt_dic = {'villagehighper': villagehighper, 'willagehighfiveper': willagehighfiveper,
                  'willagehigh': willagehigh, 'xillagehighanomaly': xillagehighanomaly,
                  'villagelowper': villagelowper, 'villagelowfiveper': villagelowfiveper,
                  'villagelow': villagelow, 'villagelowanomaly': villagelowanomaly}
    data_rt = json.dumps(dat_rt_dic)
    print('finish{}'.format(data_rt))
    return data_rt


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GFSSI Schedule')
    parser.add_argument('--dateStart', '-s', help='开始年份，YYYY(2019)', required=False)
    parser.add_argument('--dateEnd', '-e', help='结束年份，YYYY(2019)', required=False)
    parser.add_argument('--dateChoice', '-o', help='指定计算年份，YYYY(2019)', required=False)
    args = parser.parse_args()
    get_village_data(args.dateStart, args.dateEnd, args.dateChoice, )
    # python3 a06_get_village_data.py -s 2009 -e 2018 -o 2020
