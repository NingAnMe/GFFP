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
from utils import plot_stats as plts

from user_config import DATA_1KM, DATA_STAT, DATA_PICTURE
from utils.config import LATITUDE_RANGE_China, LONGITUDE_RANGE_China, POOR_XLSX, PRO_MASK_HDF
from utils.model import Village
from a04_data_statistics import num_area, num_point, num_province


# def get_village_data(data, lons, lats, lon_village, lat_village):
#     index =


def plot_map(data, lons, lats, title='', vmin=-np.inf, vmax=np.inf, areas=None, box=None, ticks=None, file_out=None,
             ptype='pcolormesh', mksize=5, nanhai=False):
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
    if nanhai:
        p.nanhai_loc = [0.83, 0.25, 0.15, 0.17]
        p.nanhai_minimap()
    p.show_china_province = True
    p.show_inside_china = True
    p.show_inside_china_mini = True

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
                  task_choice='',
                  date_start=None,
                  date_end=None,
                  area_value=None,
                  left_longitude=None,
                  left_latitude=None,
                  right_longitude=None,
                  right_latitude=None,
                  ):
    print('开始获取数据：num_area')

    if area_value == 'village':  # 绘制全国的贫困村
        village_info = Village.get_village_info_by_file(POOR_XLSX)
        lons = village_info['lon'].to_numpy()
        lats = village_info['lat'].to_numpy()

        datas, _, _ = num_point(dataType=data_type,
                                taskChoice=task_choice,
                                dateStart=date_start,
                                dateEnd=date_end,
                                leftLongitude=lons,
                                leftLatitude=lats,
                                out_fi=0)
        mksize = 1
    else:
        datas, lons, lats = num_area(dataType=data_type,
                                     taskChoice=task_choice,
                                     dateStart=date_start,
                                     dateEnd=date_end,
                                     leftLongitude=left_longitude,
                                     leftLatitude=left_latitude,
                                     rightLongitude=right_longitude,
                                     rightLatitude=right_latitude,
                                     out_fi=0)
        mksize = 5

    # 是否绘制南海
    if area_value == 'China':
        nanhai = True
    else:
        nanhai = False

    if lons is None or not np.any(lons):
        print(lons)
        raise BufferError('没有数据： lons')

    if lats is None or not np.any(lats):
        print(lats)
        raise BufferError('没有数据： lats')

    if datas is None or len(datas) == 0:
        print(datas)
        raise BufferError('没有数据： datas')

    box = [left_latitude, right_latitude, left_longitude, right_longitude]  # nlat, slat, wlon, elon:北（小），南（大），东（大），西（小）
    out_date_str = datetime.now().strftime("%Y%m%d%H%M%S")
    dir_out = os.path.join(DATA_PICTURE, out_date_str)
    for time, data in datas.items():
        print(time)
        print(data)
        print('data', np.nanmin(data), np.nanmean(data), np.nanmax(data), data.shape)
        print('lons', np.nanmin(lons), np.nanmean(lons), np.nanmax(lons), lons.shape)
        print('lats', np.nanmin(lats), np.nanmean(lats), np.nanmax(lats), lats.shape)

        title = '{}  {}'.format(data_type, time)
        vmin = None
        vmax = None
        ticks = None
        # ticks = np.arange(-0.5, 0.51, 0.1)
        aeres = None

        filename_out = '{}_{}_{}.png'.format(task_choice, data_type, time)
        file_out = os.path.join(dir_out, filename_out)

        # 是否重处理
        # if os.path.isfile(file_out):
        #     print('already exist {}'.format(file_out))
        #     continue

        plot_map(data, lons, lats, title=title, vmin=vmin, vmax=vmax,
                 areas=aeres, box=box, ticks=ticks, file_out=file_out,
                 mksize=mksize, nanhai=nanhai)
    return dir_out


def plot_data_column(data_type=None,
                     task_choice='',
                     date_start=None,
                     date_end=None,
                     mode_type=None,
                     area_value=None,
                     left_longitude=None,
                     left_latitude=None,
                     right_longitude=None,
                     right_latitude=None,
                     ):
    print('开始获取数据：{}'.format(area_value))

    if mode_type == 'all' or mode_type == 'province':
        datas, _, _ = num_province(dataType=data_type, province=area_value, taskChoice=task_choice,
                                   dateStart=date_start, dateEnd=date_end, out_fi=0, avg=True)
        print(datas)
        x = list()  # 年份
        y = list()  # 值
        for k, v in datas.items():
            x.append(k)
            y.append(v)
        # x = np.arange(2010, 2021)
        # y = np.full_like(x, 13119.997)
        out_date_str = datetime.now().strftime("%Y%m%d%H%M%S")
        dir_out = os.path.join(DATA_PICTURE, out_date_str)
        filename_out = '{}_{}_{}_{}.png'.format(task_choice, data_type, date_start, date_end)
        file_out = os.path.join(dir_out, filename_out)
        title = '{}'.format(data_type)
        x_label = ' '
        y_label = '{}'.format(data_type)
        plts.plot_bar(x, y, out_file=file_out, title=title, x_label=x_label,
                      y_label=y_label)
    else:
        raise ValueError(mode_type)

    return ''


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Schedule')
    parser.add_argument('--dataType', '-t', help='数据类型(GHI/DBI/DHI/GTI/H0/H20/H25)', required=True)
    parser.add_argument('--plotType', '-y', help='绘图类型(map column)', required=True)
    parser.add_argument('--modeType', '-m', help='单点or范围(point、area、all、village、province)', required=True)
    parser.add_argument('--taskChoice', '-c',
                        help='时间：year, month, season, quarter'
                             '任务: sum, mean, anomaly'
                             'yearSum, yearMean, yearAnomaly,'
                             'monthSum, monthMean, monthAnomaly,'
                             'seasonSum, seasonMean, seasonAnomaly,'
                             'quarterSum, quarterMean, quarterAnomaly,',
                        required=False)
    parser.add_argument('--dateStart', '-s', help='开始年份，YYYY(2019)', required=True)
    parser.add_argument('--dateEnd', '-e', help='结束年份，YYYY(2019)', required=True)
    parser.add_argument('--area', '-z', help='区域值', required=False, default='全国')
    parser.add_argument('--leftLongitude', '-l', help='经度或左上角经度，47.302235', required=False)
    parser.add_argument('--leftLatitude', '-a', help='纬度或左上角纬度，85.880519', required=False)
    parser.add_argument('--rightLongitude', '-r', help='右下角经度，47.302235', required=False)
    parser.add_argument('--rightLatitude', '-i', help='右下角经度，85.880519', required=False)
    args = parser.parse_args()

    yearStart = args.dateStart
    yearEnd = args.dateEnd

    dataType = args.dataType
    modeType = args.modeType
    plotType = args.plotType
    areaValue = args.area

    DATA_TYPE = ['map', 'column']
    # ###################################### 分布图 ##############
    if args.plotType == 'map':
        print('绘制分布图')
        MODE_TYPE = ['area', 'all', 'village', 'province']  # 区域类型

        if modeType == 'all' or modeType == 'village' or modeType == 'province':
            leftLongitude = LONGITUDE_RANGE_China[0]
            leftLatitude = LATITUDE_RANGE_China[1]
            rightLongitude = LONGITUDE_RANGE_China[1]
            rightLatitude = LATITUDE_RANGE_China[0]

            if modeType == 'province':
                assert areaValue is not None, '输入的 area 为空值: {}'.format(areaValue)
            elif modeType == 'village':
                areaValue = 'village'
            elif modeType == 'all':
                areaValue = 'China'

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
            task_choice=args.taskChoice,
            date_start=yearStart,
            date_end=yearEnd,
            area_value=areaValue,
            left_longitude=leftLongitude,
            left_latitude=leftLatitude,
            right_longitude=rightLongitude,
            right_latitude=rightLatitude,
        )
        print('finish{}'.format(dir_))

    elif plotType == 'column':  # 柱状图
        print('绘制柱状图')
        MODE_TYPE = ['point', 'all', 'province']

        if modeType == 'all' or modeType == 'village' or modeType == 'province':
            if modeType == 'province':
                assert areaValue is not None, '输入的 area 为空值: {}'.format(areaValue)
            elif modeType == 'all':
                areaValue = 'all'
        else:
            raise ValueError('modeType 参数错误 :{}'.format(MODE_TYPE))

        dir_ = plot_data_column(
            data_type=args.dataType,
            task_choice=args.taskChoice,
            date_start=yearStart,
            date_end=yearEnd,
            mode_type=modeType,
            area_value=areaValue,
        )

    else:
        raise ValueError('dataType 参数错误 :{}'.format(DATA_TYPE))

    """
    图1
    python3 a05_data_plot.py -t GHI -y column -m all -c yearSum -s 2010 -e 2020
    
    图2
    python3 a05_data_plot.py -t GHI -y map -m all -c yearSum -s 2020 -e 2020
    
    图3
    python3 a05_data_plot.py -t GHI -y map -m all -c yearAnomaly -s 2020 -e 2020
    """
