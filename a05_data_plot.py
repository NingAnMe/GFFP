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
import numpy as np
import pandas as pd

from pylab import subplots_adjust
from DV.dv_map import dv_map

from user_config import DATA_PICTURE

from utils import plot_stats as plts
from utils.hdf5 import get_hdf5_data
from utils.config import LATITUDE_RANGE_China, LONGITUDE_RANGE_China, POOR_XLSX, PRO_MASK_HDF, PROVINCE_MASK
from utils.model import Village, Station, session_scope

from a04_data_statistics import num_area, num_point

PICTURE_RANGE = {
    '1': [0, 1750],
    '2': [1050, 1400, 1750],
    '3': [-5, -2, 0, 2, 5],
    '4': [-10, 10],
    '5': [-10, 10],
    '6': [-5, -2, 0, 2, 5],
    '7': [-5, -2, 0, 2, 5],
    '8': [-5, -2, 0, 2, 5],
    '9': [-5, -2, 0, 2, 5],
    '10': [1000, 1200, 1400, 1600, 1800, 2000, 2200],
    '11': [800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800],
    '12': [-5, -2, 0, 2, 5],
    '13': [-10, 10],
    '14': [800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800],
    '15': [-5, -2, 0, 2, 5],
    '16': [-10, 10],
    '17': None,
    '18': [0, 1750],
    '19': [0, 1750],
    '20': [0, 1750],
    '21': [0, 1750],
    '22': [0, 1750],
    # '23': [-20, 20],
    # '24': [-20, 20],
    # '25': [-20, 20],
    # '26': [-20, 20],
    # '27': [-20, 20],
}


def plot_map(data, lons, lats, title=None, vmin=-np.inf, vmax=np.inf, areas=None, box=None, ticks=None,
             file_out=None, ptype='contourf', mksize=5, nanhai=False):
    if file_out is None:
        print('没有指定输出文件：file_out is None')
        return

    print(np.nanmin(data), np.nanmax(data), np.nanmean(data))

    latitude = lats
    longitude = lons
    value = data
    out_file = file_out
    if vmin is not None and vmax is not None:
        pass
        # valid = np.logical_and(value > vmin, value < vmax)
        # value[~valid] = np.nan
    else:
        value[value == 0] = np.nan
        vmin = np.nanmin(value)
        vmax = np.nanmax(value)

    # 开始画图-----------------------

    fig = plt.figure(figsize=(9, 6.5))  # 图像大小

    p = dv_map(fig=fig)

    subplots_adjust(left=0.07, right=0.96, top=1, bottom=0.15)
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
    # p.colormap = plt.get_cmap('jet')  # mpl.cm.rainbow, summer, jet, bwr
    p.colorbar_extend = "both"  # both

    # plot
    print(latitude.shape, longitude.shape, value.shape)
    # ptype = 'contourf' pcolormesh
    if ptype == 'contourf':
        if ticks:
            p.colorbar_bounds = ticks
            # value[value > ticks[-2]] = ticks[-1]  # 去除对最大最小值的限制
            # value[value < ticks[1]] = ticks[0]

    print(ptype)
    print(p.colorbar_bounds)
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
    fig.colorbar(p.cs, cax=c_ax, ticks=ticks, orientation='horizontal', extend=p.colorbar_extend)
    for l in c_ax.xaxis.get_ticklabels():
        l.set_fontproperties(p.font_mid)
        l.set_fontsize(fontsize)
        l.set_color(p.color_ticker)
    # cbar写单位
    # cbar.ax.set_title(unit, x=1.0382, y=0, color=p.color_ticker,
    #                   ha='left', va='center',
    #                   fontproperties=p.font_mid, fontsize=fontsize)

    # 标题 ---------------------------
    if title is not None:
        p.w_title = p.suptitle(title, fontsize=14, y=0.97)

    # save
    p.savefig(out_file, dpi=300)
    print(">>> {}".format(out_file))
    p.clean()


def plot_data_map(data_type=None,
                  task_choice='',
                  date_start=None,
                  date_end=None,
                  date_choice=None,
                  area_value=None,
                  left_longitude=None,
                  left_latitude=None,
                  right_longitude=None,
                  right_latitude=None,
                  picture_number=None,
                  ):
    print('data_type == : {}'.format(data_type))
    print('task_choice == : {}'.format(task_choice))
    print('date_start == : {}'.format(date_start))
    print('date_end == : {}'.format(date_end))
    print('date_choice == : {}'.format(date_choice))
    print('area_value == : {}'.format(area_value))
    print('left_longitude == : {}'.format(left_longitude))
    print('left_latitude == : {}'.format(left_latitude))
    print('right_longitude == : {}'.format(right_longitude))
    print('right_latitude == : {}'.format(right_latitude))
    print('picture_number == : {}'.format(picture_number))

    if area_value == 'village':  # 绘制全国的贫困村
        village_info = Village.get_village_info_by_file(POOR_XLSX)
        lons = village_info['lon'].to_numpy()
        lats = village_info['lat'].to_numpy()

        datas, _, _ = num_point(dataType=data_type,
                                taskChoice=task_choice,
                                dateStart=date_start,
                                dateEnd=date_end,
                                dateChoice=date_choice,
                                left_longitude=lons,
                                left_latitude=lats,
                                out_fi=0)
        mksize = 1
    elif area_value == 'station':  # 绘制站点分布图
        with session_scope() as session:
            result = Station.query_by_date_range(session=session, dt_s=date(2019, 1, 1), dt_e=date(2019, 1, 1))
            result = [i.to_dict() for i in result]
            df = pd.DataFrame(result)
            year_ghi = df.groupby('station').agg({
                data_type: 'sum',
                'lon': 'min',
                'lat': 'min',
            })
            datas = year_ghi.GHI.to_numpy()
            lons = year_ghi.lon.to_numpy()
            lats = year_ghi.lat.to_numpy()
            mksize = 3
    else:
        result = num_area(dataType=data_type,
                          taskChoice=task_choice,
                          dateStart=date_start,
                          dateEnd=date_end,
                          dateChoice=date_choice,
                          leftLongitude=left_longitude,
                          leftLatitude=left_latitude,
                          rightLongitude=right_longitude,
                          rightLatitude=right_latitude,
                          out_fi=0)
        print(result)
        datas, lons, lats = result
        mksize = 5
    print(datas)
    print(datas.keys())

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

        title = None
        vmin = None
        vmax = None
        ticks = None
        picture_range = PICTURE_RANGE.get(picture_number)
        print('picture_range', picture_range)
        if picture_range:
            if len(picture_range) != 2:
                ticks = picture_range
                vmin = picture_range[0]
                vmax = picture_range[-1]
            else:
                vmin = picture_range[0]
                vmax = picture_range[-1]

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


def get_y_label(data_type, an):
    y_labels_sum = {
        "GHI": "水平面总辐照量 $KWh/m^2$",
        "GTI": "最佳斜面总辐照量 $KWh/m^2$",
    }
    y_labels_an = {
        "GHI": "水平面总辐照量距平百分率 %",
        "GTI": "最佳斜面总辐照距平百分率 %",
    }
    if an:
        y_labels = y_labels_an
    else:
        y_labels = y_labels_sum
    if data_type in y_labels:
        return y_labels[data_type]
    return None


def plot_data_column(data_type=None,
                     task_choice='',
                     date_start=None,
                     date_end=None,
                     date_choice=None,
                     mode_type=None,
                     area_value=None,
                     left_longitude=None,
                     left_latitude=None,
                     right_longitude=None,
                     right_latitude=None,
                     picture_number=None,
                     ):
    print('data_type == : {}'.format(data_type))
    print('task_choice == : {}'.format(task_choice))
    print('date_start == : {}'.format(date_start))
    print('date_end == : {}'.format(date_end))
    print('date_choice == : {}'.format(date_choice))
    print('mode_type == : {}'.format(mode_type))
    print('area_value == : {}'.format(area_value))
    print('left_longitude == : {}'.format(left_longitude))
    print('left_latitude == : {}'.format(left_latitude))
    print('right_longitude == : {}'.format(right_longitude))
    print('right_latitude == : {}'.format(right_latitude))
    print('picture_number == : {}'.format(picture_number))

    if mode_type == 'all':  # 绘制所有省份的柱状图

        datas, _, _ = num_area(dataType=data_type, taskChoice=task_choice,
                               dateStart=date_start, dateEnd=date_end,
                               dateChoice=date_choice,
                               leftLongitude=float(LONGITUDE_RANGE_China[0]),
                               leftLatitude=float(LATITUDE_RANGE_China[1]),
                               rightLongitude=float(LONGITUDE_RANGE_China[1]),
                               rightLatitude=float(LATITUDE_RANGE_China[0]),
                               out_fi=0)
        print(datas)
        pro_mask_data = get_hdf5_data(PRO_MASK_HDF, 'province_mask', 1, 0, [0, 255], 0)
        if area_value == 'province':  # 按省份分组
            x_y = dict()
            if date_choice:
                datas = {date_choice: datas[date_choice]}
            for time, data in datas.items():
                x_y[time] = dict()
                x = list()
                y = list()
                for sheng in PROVINCE_MASK:
                    if sheng in {'台湾省', '香港', '澳门'}:
                        continue
                    data_mean = np.nanmean(data[pro_mask_data == PROVINCE_MASK[sheng]])
                    x.append(sheng[:3])
                    y.append(data_mean)
                y, x = zip(*sorted(zip(y, x), reverse=False))
                x_y[time]['y'] = y
                x_y[time]['x'] = x

        else:  # 按时间分组
            x_y = {'time': {'x': list(),
                            'y': list()}}
            times = datas.keys()
            times = sorted(times)
            for time in times:
                x_y['time']['x'].append(time)
                x_y['time']['y'].append(np.nanmean(datas[time][pro_mask_data != 0]))

    elif mode_type == 'village' and area_value == 'province':  # 绘制每个省贫困村的平均值的柱状图
        village_info = Village.get_village_info_by_file(POOR_XLSX)
        lons = village_info['lon'].to_numpy()
        lats = village_info['lat'].to_numpy()
        datas, _, _ = num_point(dataType=data_type,
                                taskChoice=task_choice,
                                dateStart=date_start,
                                dateEnd=date_end,
                                dateChoice=date_choice,
                                left_longitude=lons,
                                left_latitude=lats,
                                out_fi=0)
        print(datas)
        x_y = dict()
        if date_choice:
            datas = {date_choice: datas[date_choice]}
        for time, data in datas.items():
            x_y[time] = dict()
            village_info[data_type] = datas[date_choice]
            values = village_info.groupby('sheng')[data_type].agg('mean')
            values = values.sort_values()
            x = pd.Series(values.index)
            x[x == '广西壮族自治区'] = '广西'
            x[x == '宁夏回族自治区'] = '宁夏'
            x[x == '西藏自治区'] = '西藏'
            x[x == '内蒙古自治区'] = '内蒙古'
            x[x == '新疆维吾尔自治区'] = '新疆'
            x[x == '黑龙江省'] = '黑龙江'
            x_y[time]['x'] = x.to_numpy()
            x_y[time]['y'] = values.values
    elif mode_type == 'point':
        datas, _, _ = num_point(dataType=data_type,
                                taskChoice=task_choice,
                                dateStart=date_start,
                                dateEnd=date_end,
                                dateChoice=date_choice,
                                left_longitude=left_longitude,
                                left_latitude=left_latitude,
                                out_fi=0)
        print(datas)
        x_y = dict()
        x_y['point'] = dict()
        x = list()
        y = list()
        for k, v in datas.items():
            x.append(k)
            y.append(v)
        x, y = zip(*sorted(zip(x, y), reverse=False))
        x_y['point']['x'] = np.array(x)
        x_y['point']['y'] = np.array(y)

    else:
        raise ValueError(mode_type)

    if 'Anomaly' in task_choice:
        mean_line = False
    else:
        mean_line = True

    y_range = None

    picture_range = PICTURE_RANGE.get(picture_number)
    print('picture_range', picture_range)
    if picture_range:
        if len(picture_range) == 2:
            y_range = picture_range

    out_date_str = datetime.now().strftime("%Y%m%d%H%M%S")
    dir_out = os.path.join(DATA_PICTURE, out_date_str)
    for group, x_y_ in x_y.items():
        x = x_y_['x']
        y = x_y_['y']
        filename_out = '{}_{}_{}_{}_{}.png'.format(task_choice, data_type, date_start, date_end, group)
        file_out = os.path.join(dir_out, filename_out)
        # title = '{}'.format(data_type)
        title = None
        x_label = ' '

        if 'Anomaly' in task_choice:
            y_label_t = get_y_label(data_type, True)
        else:
            y_label_t = get_y_label(data_type, False)
        if y_label_t:
            y_label = y_label_t
        else:
            y_label = '{}'.format(data_type)
        if area_value == 'time':  # 如果x轴是年份
            if 'year' in task_choice:
                x_label = '年'
            elif 'month' in task_choice:
                x_label = '月'
            elif 'season' in task_choice:
                x_label = '季节'
            elif 'quarter' in task_choice:
                x_label = '季度'
        elif area_value == 'province':  # 如果x轴是省份
            x_label = '省（市、区）'

        plts.plot_bar(x, y, out_file=file_out, title=title, x_label=x_label,
                      y_label=y_label, y_range=y_range, data_type=area_value, mean_line=mean_line)

    return dir_out


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
    parser.add_argument('--dateChoice', '-o', help='距平年份，YYYY(2019)', required=False)
    parser.add_argument('--area', '-z', help='区域值', required=False, default='全国')
    parser.add_argument('--leftLongitude', '-l', help='经度或左上角经度，47.302235', required=False)
    parser.add_argument('--leftLatitude', '-a', help='纬度或左上角纬度，85.880519', required=False)
    parser.add_argument('--rightLongitude', '-r', help='右下角经度，47.302235', required=False)
    parser.add_argument('--rightLatitude', '-i', help='右下角经度，85.880519', required=False)
    parser.add_argument('--pictureNumber', '-p', help='绘图编号', required=False)
    args = parser.parse_args()

    yearStart = args.dateStart
    yearEnd = args.dateEnd
    yearChoice = args.dateChoice

    dataType = args.dataType
    modeType = args.modeType
    plotType = args.plotType
    areaValue = args.area

    pictureNumber = args.pictureNumber

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
            date_choice=yearChoice,
            area_value=areaValue,
            left_longitude=leftLongitude,
            left_latitude=leftLatitude,
            right_longitude=rightLongitude,
            right_latitude=rightLatitude,
            picture_number=pictureNumber,
        )
        print('finish{}'.format(dir_))

    elif plotType == 'column':  # 柱状图
        print('绘制柱状图')
        MODE_TYPE = ['point', 'all', 'province']

        if modeType == 'all':
            AREA_TYPE = {'province', 'time'}
            assert areaValue is not None, '输入的 area 需要为 {}'.format(AREA_TYPE)
            assert areaValue in AREA_TYPE, '输入的 area 需要为 {}'.format(AREA_TYPE)
        elif modeType == 'village':
            AREA_TYPE = {'province', 'time'}
            assert areaValue is not None, '输入的 area 需要为 {}'.format(AREA_TYPE)
            assert areaValue in AREA_TYPE, '输入的 area 需要为 {}'.format(AREA_TYPE)
        elif modeType == 'point':
            areaValue = 'time'
        else:
            raise ValueError('modeType 参数错误 :{}'.format(MODE_TYPE))

        if modeType == 'all':
            leftLongitude = LONGITUDE_RANGE_China[0]
            leftLatitude = LATITUDE_RANGE_China[1]
            rightLongitude = LONGITUDE_RANGE_China[1]
            rightLatitude = LATITUDE_RANGE_China[0]
            leftLongitude = float(leftLongitude)
            leftLatitude = float(leftLatitude)
            rightLongitude = float(rightLongitude)
            rightLatitude = float(rightLatitude)
            assert leftLongitude >= LONGITUDE_RANGE_China[0]
            assert leftLatitude <= LATITUDE_RANGE_China[1]
            assert rightLongitude <= LONGITUDE_RANGE_China[1]
            assert rightLatitude >= LATITUDE_RANGE_China[0]
        elif modeType == 'point':
            leftLongitude = args.leftLongitude
            leftLatitude = args.leftLatitude
            rightLongitude = None
            rightLatitude = None
        else:
            leftLongitude = None
            leftLatitude = None
            rightLongitude = None
            rightLatitude = None

        dir_ = plot_data_column(
            data_type=args.dataType,
            task_choice=args.taskChoice,
            date_start=yearStart,
            date_end=yearEnd,
            date_choice=yearChoice,
            mode_type=modeType,
            area_value=areaValue,
            left_longitude=leftLongitude,
            left_latitude=leftLatitude,
            right_longitude=rightLongitude,
            right_latitude=rightLatitude,
            picture_number=pictureNumber,
        )
        print('finish{}'.format(dir_))
    else:
        raise ValueError('dataType 参数错误 :{}'.format(DATA_TYPE))

    """
    图1
    python3 a05_data_plot.py -t GHI -y column -m all -z time -c yearSum -s 2009 -e 2019 -p 1

    图2
    python3 a05_data_plot.py -t GHI -y map -m all -c yearSum -s 2019 -e 2019 -p 2

    图3
    python3 a05_data_plot.py -t GHI -y map -m all -c yearAnomaly -s 2009 -e 2018 -o 2019 -p 3

    图4
    python3 a05_data_plot.py -t GHI -y column -m all -z province -c yearAnomaly -s 2009 -e 2018 -o 2019 -p 4
    
    图5
    python3 a05_data_plot.py -t GHI -y column -m all -z time -c monthAnomaly -s 2009 -e 2018 -o 2019 -p 5
    
    图6、7、8、9
    python3 a05_data_plot.py -t GHI -y map -m all -c quarterAnomaly -s 2009 -e 2018 -o 2019 -p 6
    
    图10
    python3 a05_data_plot.py -t GTI -y map -m all -c yearSum -s 2019 -e 2019 -p 10
    
    图11
    python3 a05_data_plot.py -t H0 -y map -m all -c yearSum -s 2019 -e 2019 -p 11
    
    图12
    python3 a05_data_plot.py -t GTI -y map -m all -c yearAnomaly -s 2009 -e 2018 -o 2019 -p 12
    
    图13
    python3 a05_data_plot.py -t GTI -y column -m all -z province -c yearAnomaly -s 2009 -e 2018 -o 2019 -p 13

    图14
    python3 a05_data_plot.py -t GHI -y map -m village -c yearSum -s 2019 -e 2019 -p 14
    
    图15
    python3 a05_data_plot.py -t GHI -y map -m village -c yearAnomaly -s 2009 -e 2018 -o 2019 -p 15
    
    图16
    python3 a05_data_plot.py -t GHI -y column -m village -z province -c yearAnomaly -s 2009 -e 2018 -o 2019 -p 16
    
    图18、19、20、21、22
    python3 a05_data_plot.py -t GHI -y column -m point -z time -c yearSum -s 2009 -e 2019 -l 115.0 -a 31.2 -p 18
    
    图23、24、25、26、27
    python3 a05_data_plot.py -t GHI -y column -m point -z time -c monthAnomaly -s 2009 -e 2018 -o 2019 -l 115.0 -a 31.2 -p 23
    """
