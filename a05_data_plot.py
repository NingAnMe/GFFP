#!/usr/bin/env python 
# -*- coding: utf-8 -*-
# @Time    : 2020-11-24 17:45
# @Author  : NingAnMe <ninganme@qq.com>
"""第五步：基于上述统计结果，绘制图形
1）全国/各省/任意方框区域上述统计值的空间分布图（连续）（类似于模板
中的图2-3、图6-9、图11-12）；
2）全国贫困村上述统计值的空间分布图（离散）（类似于模板中的图14-15）；
3）全国/各省平均值年际变化直方图（类似于模板中图1、图10）；全国/各
省平均值年变化直方图（类似于模板中图5）；
4）各省平均距平排序直方图（模板中图4、图13、图16）；
5）任意点年际变化直方图（类似于模板中图18-22）；任意点年变化直方图
（类似于模板中图23-27）；
功能需求：能够导出常见格式的图片，JPG、PNG等。"""

import argparse
import os
from datetime import datetime, date

import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd

from pylab import subplots_adjust
from DV.dv_map import dv_map

from user_config import DATA_1KM, DATA_STAT, DATA_PICTURE
from utils.config import LATITUDE_RANGE_China, LONGITUDE_RANGE_China, POOR_XLSX
from utils.model import Poor
from a04_data_statistics import num_area, num_point


# def get_poor_data(data, lons, lats, lon_poor, lat_poor):
#     index =


def plot_map(data, lons, lats, title='', vmin=-np.inf, vmax=np.inf, areas=None, box=None, ticks=None, file_out=None,
             ptype='pcolormesh', mksize=5):
    if file_out is None:
        print('没有指定输出文件：file_out is None')
        return

    print(np.nanmin(data), np.nanmax(data), np.nanmean(data))

    latitude = lats
    longitude = lons
    value = data
    out_file = file_out
    if vmin is not None and vmax is not None:
        valid = np.logical_and(value > vmin, value < vmax)
        value[~valid] = np.nan
    else:
        value[value == 0] = np.nan
        vmin = np.nanmin(value)
        vmax = np.nanmax(value)

    # 开始画图-----------------------

    fig = plt.figure(figsize=(9., 8.))  # 图像大小

    p = dv_map(fig=fig)

    subplots_adjust(left=0.07, right=0.98, top=0.90, bottom=0.15)
    p.show_colorbar = False
    p.show_countries = False
    p.show_coastlines = False
    # p.show_line_of_latlon = False
    # p.show_china = True
    p.show_china_province = True
    p.show_inside_china = True
    p.show_inside_china_mini = False

    if box:
        if abs(box[1] - box[0]) < 10:
            p.delat = 2
        else:
            p.delat = 5
        if abs(box[2] - box[3]) < 10:
            p.delon = 2
        elif abs(box[2] - box[3]) < 40:
            p.delon = 5
        else:
            p.delon = 10
    # else:
    #     p.delat = 2  # 纬度刻度线分辨率
    #     p.delon = 2  # 经度刻度线分辨率
    p.color_coast = "#3a3a3a"  # 海岸线颜色
    p.color_contry = "#3a3a3a"  # 国家颜色
    p.fontsize_tick = 15

    # set color map
    print('vmin == {}'.format(vmin))
    print('vmax == {}'.format(vmax))
    p.valmin = vmin
    p.valmax = vmax
    p.colormap = plt.get_cmap('jet')  # mpl.cm.rainbow, summer, jet, bwr
    # p.colorbar_extend = "max"

    # plot
    p.easyplot(latitude, longitude, value, vmin=vmin, vmax=vmax, box=box, markersize=mksize, ptype=ptype)

    # TODO 增加设置省份的功能
    if areas is not None:
        print('设置地区 ：{}'.format(areas))
        # aeres = ["江苏省", "安徽省", "浙江省", "上海市"]
        for aere in areas:
            p.city_boundary(aere, linewidth=1.2, shape_name='中国省级行政区')

    # 色标 ---------------------------
    cb_loc = [0.12, 0.07, 0.76, 0.03]
    # unit = r"$\mathregular{(10^{15}\/\/molec/cm^2)}$"
    fontsize = 16
    # p.add_custom_colorbar(cb_loc, p.valmin, p.valmax,
    #                       fmt="%d",
    #                       unit="(1E16 molec/cm^2)",
    #                       fontsize=fontsize)
    c_ax = fig.add_axes(cb_loc)
    # cbar = fig.colorbar(p.cs, cax=c_ax, ticks=np.arange(0, 1.6, 0.3), orientation='horizontal')
    fig.colorbar(p.cs, cax=c_ax, ticks=ticks, orientation='horizontal')
    for l in c_ax.xaxis.get_ticklabels():
        l.set_fontproperties(p.font_mid)
        l.set_fontsize(fontsize)
        l.set_color(p.color_ticker)
    # cbar写单位
    # cbar.ax.set_title(unit, x=1.0382, y=0, color=p.color_ticker,
    #                   ha='left', va='center',
    #                   fontproperties=p.font_mid, fontsize=fontsize)

    # 标题 ---------------------------
    p.w_title = p.suptitle(title, fontsize=14, y=0.97)

    # save
    p.savefig(out_file, dpi=300)
    print(">>> {}".format(out_file))
    p.clean()


def plot_data_map(data_type=None,
                  taskChoice='',
                  dateStart=None,
                  dateEnd=None,
                  area_value=None,
                  left_longitude=None,
                  left_latitude=None,
                  right_longitude=None,
                  right_latitude=None,
                  ):
    print('开始获取数据：num_area')

    if area_value == 'poor':  # 绘制全国的贫困村
        poor_info = Poor.get_poor_info_by_file(POOR_XLSX)
        lons = poor_info['lon'].to_numpy()
        lats = poor_info['lat'].to_numpy()

        datas, _, _ = num_point(dataType=data_type,
                                taskChoice=taskChoice,
                                dateStart=dateStart,
                                dateEnd=dateEnd,
                                leftLongitude=lons,
                                leftLatitude=lats,
                                out_fi=0)
        mksize = 1
    else:
        datas, lons, lats = num_area(dataType=data_type,
                                     taskChoice=taskChoice,
                                     dateStart=dateStart,
                                     dateEnd=dateEnd,
                                     leftLongitude=left_longitude,
                                     leftLatitude=left_latitude,
                                     rightLongitude=right_longitude,
                                     rightLatitude=right_latitude,
                                     out_fi=0)
        mksize = 5

    if lons is None or not np.any(lons):
        print(lons)
        raise BufferError('没有数据： lons')

    if lats is None or not np.any(lats):
        print(lats)
        raise BufferError('没有数据： lats')

    if datas is None or len(datas) == 0:
        print(datas)
        raise BufferError('没有数据： datas')

    out_date_str = datetime.now().strftime("%Y%m%d%H%M%S")
    box = [left_latitude, right_latitude, left_longitude, right_longitude]  # nlat, slat, wlon, elon:北（小），南（大），东（大），西（小）
    for time, data in datas.items():
        print(time)
        print(data)
        print('data', np.nanmin(data), np.nanmean(data), np.nanmax(data), data.shape)
        print('lons', np.nanmin(lons), np.nanmean(lons), np.nanmax(lons), lons.shape)
        print('lats', np.nanmin(lats), np.nanmean(lats), np.nanmax(lats), lats.shape)

        title = '{}'.format(data_type)
        vmin = None
        vmax = None
        ticks = None
        # ticks = np.arange(-0.5, 0.51, 0.1)
        aeres = None

        filename_out = '{}_{}_{}.png'.format(taskChoice, data_type, time)
        dir_out = os.path.join(DATA_PICTURE, out_date_str)
        file_out = os.path.join(dir_out, filename_out)

        # 是否重处理
        # if os.path.isfile(file_out):
        #     print('already exist {}'.format(file_out))
        #     continue

        plot_map(data, lons, lats, title=title, vmin=vmin, vmax=vmax,
                 areas=aeres, box=box, ticks=ticks, file_out=file_out,
                 mksize=mksize)
        return dir_out


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GFSSI Schedule')
    parser.add_argument('--dataType', '-t', help='数据类型(GHI/DBI/DHI/GTI/...)', required=True)
    parser.add_argument('--plotType', '-y', help='绘图类型(map column)', required=True)
    parser.add_argument('--modeType', '-m', help='单点or范围(point、area、all、poor、province)', required=True)
    parser.add_argument('--taskChoice', '-c',
                        help='时间：year, month, season, quarter   '
                             '任务: sum, mean, anomaly  '
                             'yearSum, yearMean, yearAnomaly    ',
                        required=False)
    parser.add_argument('--dateStart', '-s', help='开始年份，YYYY(2019)', required=True)
    parser.add_argument('--dateEnd', '-e', help='结束年份，YYYY(2019)', required=True)
    parser.add_argument('--area', '-z', help='区域值', required=False, default='全国')
    parser.add_argument('--leftLongitude', '-l', help='经度或左上角经度，47.302235', required=False)
    parser.add_argument('--leftLatitude', '-a', help='纬度或左上角纬度，85.880519', required=False)
    parser.add_argument('--rightLongitude', '-r', help='右下角经度，47.302235', required=False)
    parser.add_argument('--rightLatitude', '-i', help='右下角经度，85.880519', required=False)
    args = parser.parse_args()

    year_start = args.dateStart
    year_end = args.dateEnd

    dataType = args.dataType
    modeType = args.modeType
    plotType = args.plotType
    area = args.area

    DATA_TYPE = ['map', 'column']
    # ###################################### 分布图 ##############
    if args.plotType == 'map':
        MODE_TYPE = ['area', 'all', 'poor', 'province']  # 区域类型

        if modeType == 'all' or modeType == 'poor' or modeType == 'province':
            leftLongitude = LONGITUDE_RANGE_China[0]
            leftLatitude = LATITUDE_RANGE_China[1]
            rightLongitude = LONGITUDE_RANGE_China[1]
            rightLatitude = LATITUDE_RANGE_China[0]

            if modeType == 'province':
                assert area is not None, '输入的 area 为空值: {}'.format(area)
            elif modeType == 'poor':
                area = 'poor'

        elif modeType == 'area':
            leftLongitude = args.leftLongitude
            leftLatitude = args.leftLatitude
            rightLongitude = args.rightLongitude
            rightLatitude = args.rightLatitude
        else:
            raise ValueError('modeType 参数错误 :{}'.format(MODE_TYPE))

        print('leftLongitude  === {}'.format(leftLongitude))
        print('leftLatitude   === {}'.format(leftLatitude))
        print('rightLongitude === {}'.format(rightLongitude))
        print('rightLatitude  === {}'.format(rightLatitude))

        leftLongitude = float(leftLongitude)
        leftLatitude = float(leftLatitude)
        rightLongitude = float(rightLongitude)
        rightLatitude = float(rightLatitude)
        assert leftLongitude >= LONGITUDE_RANGE_China[0]
        assert leftLatitude <= LATITUDE_RANGE_China[1]
        assert rightLongitude <= LONGITUDE_RANGE_China[1]
        assert rightLatitude >= LATITUDE_RANGE_China[0]

        if leftLongitude >= rightLongitude:
            raise ValueError('leftLongitude >= rightLongitude')

        if rightLatitude >= leftLatitude:
            raise ValueError('rightLatitude >= leftLatitude')

        dir_ = plot_data_map(
            data_type=args.dataType,
            taskChoice=args.taskChoice,
            dateStart=year_start,
            dateEnd=year_end,
            area_value=area,
            left_longitude=leftLongitude,
            left_latitude=leftLatitude,
            right_longitude=rightLongitude,
            right_latitude=rightLatitude,
        )
        print('finish{}'.format(dir_))

    elif plotType == 'column':  # 柱状图
        MODE_TYPE = ['point']
    else:
        raise ValueError('dataType 参数错误 :{}'.format(DATA_TYPE))
