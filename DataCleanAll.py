"""
0. 多份文件名日期一一对应的P文件和Y文件
1. Merge文件P和文件Y
2. 对TradingDay做一分钟resample，循环改为（0，60）可改为连续每秒做一分钟resample
3. 结果拼接到一个文件

"""

import pandas as pd
from datetime import timedelta
import ipdb
import os
import re


# 获取目录下所有文件名，分为P和Y两个dict
def get_filename(path, filetype):
    dict1 = {}
    dict2 = {}
    for root, dirs, files in os.walk(path):
        for file in files:
            if filetype in file:
                splitted = re.split('[_.]', file)
                if splitted[0] == 'p1901':
                    dict1[splitted[1]] = file
                elif splitted[0] == 'y1901':
                    dict2[splitted[1]] = file
    return dict1, dict2


path = 'D:\\PITT\\data analytics\\pycharm work\\intern work\\StocksDataClean\\data\\Archive'
filetype = '.csv'
dict_P, dict_Y = get_filename(path, filetype)

# 从小到大排序的TradeDay
TradeDays = list(dict_P.keys())
TradeDays.sort()

# 空白字典存放dataframe
df_merge_dict = {}
df_resample_dict = {}
keys = TradeDays
values = range(len(TradeDays))
dict_index = dict(zip(keys, values))

for TradeDay in TradeDays:
    # 文件位置
    file1 = dict_P[TradeDay]
    file2 = dict_Y[TradeDay]

    # 读取文件
    in1_df = pd.read_csv(file1, encoding="gbk")
    in2_df = pd.read_csv(file2, encoding="gbk")

    # 按时间排序
    in1_df.sort_values(by=['TradingDay', 'UpdateTime', 'UpdateMillisec'], inplace=True)
    in2_df.sort_values(by=['TradingDay', 'UpdateTime', 'UpdateMillisec'], inplace=True)

    # 添加用于观察时间来源的时间列
    in1_df['UpdateTime_P'] = in1_df['UpdateTime']
    in1_df['UpdateMillisec_P'] = in1_df['UpdateMillisec']
    in2_df['UpdateTime_Y'] = in2_df['UpdateTime']
    in2_df['UpdateMillisec_Y'] = in2_df['UpdateMillisec']

    # 合并P和Y
    merge_df = pd.merge(in1_df, in2_df, on=['TradingDay', 'UpdateTime', 'UpdateMillisec'],
                        suffixes=('_P', '_Y'), how='outer')

    # P和Y时间总体排序
    merge_df.sort_values(by=['TradingDay', 'UpdateTime', 'UpdateMillisec'], inplace=True)

    # 填充缺失数据
    merge_df.fillna(method='pad', inplace=True)

    # 得到merge的最终结果merge_df
    df_merge_dict[dict_index[TradeDay]] = merge_df

    # 开始resample的处理过程
    # 将时间转换为datetime格式存入新列
    merge_df['TradingDay'] = pd.to_datetime(merge_df['TradingDay'], format='%Y-%m-%d')
    merge_df['UpdateTimeCombineMS'] = pd.to_datetime(merge_df['TradingDay'].apply(lambda x: x.strftime('%Y%m%d'))
                                                     + ' ' + merge_df['UpdateTime'], format='%Y%m%d %H:%M:%S')
    merge_df['UpdateTimeCombineMS'] = merge_df['UpdateTimeCombineMS'] + pd.to_timedelta(merge_df['UpdateMillisec'],
                                                                                        unit='ms')

    # 将datetime列设为index
    pre_resample_df = merge_df.set_index('UpdateTimeCombineMS')
    pre_resample_df = pre_resample_df[['BidPrice1_P', 'BidVolume1_P', 'AskPrice1_P', 'AskVolume1_P',
                                       'BidPrice1_Y', 'BidVolume1_Y', 'AskPrice1_Y', 'AskVolume1_Y',
                                       'UpdateTime_P', 'UpdateMillisec_P', 'UpdateTime_Y', 'UpdateMillisec_Y']]

    for t_delta in range(0, 1):
        # 设定delta时间
        delta = timedelta(seconds=t_delta)
        # 复制原始数据
        copy_delta_df = pre_resample_df.copy()
        # 取出时间列
        copy_delta_df.reset_index(inplace=True)
        # 时间加上delta秒数求得新时间
        copy_delta_df['UpdateTimeCombineMS'] = copy_delta_df['UpdateTimeCombineMS'] + delta
        # 设定新时间为index
        copy_delta_df = copy_delta_df.set_index('UpdateTimeCombineMS')

        # first
        # 新时间的一分钟聚合
        copy_first_df = copy_delta_df.resample(rule='1T', closed='left', loffset=delta).first()
        # 取出新时间列
        copy_first_df.reset_index(inplace=True)
        # 新时间减去delta秒数回归真实时间
        copy_first_df['UpdateTimeCombineMS'] = copy_first_df['UpdateTimeCombineMS'] - delta
        # 真实时间的一分钟聚合
        copy_first_df = copy_first_df.set_index('UpdateTimeCombineMS')
        # 列名后缀'_first'
        copy_first_df = copy_first_df.add_suffix('_first')

        # last
        # 新时间的一分钟聚合
        copy_last_df = copy_delta_df.resample(rule='1T', closed='left', loffset=delta).last()
        # 取出新时间列
        copy_last_df.reset_index(inplace=True)
        # 新时间减去delta秒数回归真实时间
        copy_last_df['UpdateTimeCombineMS'] = copy_last_df['UpdateTimeCombineMS'] - delta
        # 真实时间的一分钟聚合
        copy_last_df = copy_last_df.set_index('UpdateTimeCombineMS')
        # 列名后缀'_last'
        copy_last_df = copy_last_df.add_suffix('_last')

        # 合并
        copy_final_df = pd.concat([copy_first_df, copy_last_df], axis=1)
        # 新增delta列
        copy_final_df['delta'] = -t_delta
        # 更改index名
        copy_final_df.index.names = ['TradeTime']

        df_resample_dict[dict_index[TradeDay]] = copy_final_df

# 输出两个文件
out1_file = 'output/2018-09-P&Y-merge.csv'
df_merge = pd.concat(df_merge_dict)
df_merge.to_csv(out1_file, encoding="gbk")

out2_file = 'output/2018-09-P&Y-resample.csv'
df_resample = pd.concat(df_resample_dict)
df_resample.to_csv(out2_file, encoding="gbk")

# print()
# ipdb.set_trace()
