"""
1. Merge文件P和文件Y
2. 对TradingDay的连续每一秒做一分钟resample
"""

import pandas as pd
from datetime import timedelta
import ipdb

# 文件位置
file1 = "data/p1901_20181101.csv"
file2 = "data/y1901_20181101.csv"

# 读取文件
in1_df = pd.read_csv(file1, encoding="gbk")
in2_df = pd.read_csv(file2, encoding="gbk")

# 中英文名称转换
in1_df.rename(columns={'交易日': 'TradingDay',
                       '合约代码': 'InstrumentID',
                       '交易所代码': 'ExchangeID',
                       '合约在交易所的代码': 'ExchangeInstID',
                       '最新价': 'LastPrice',
                       '上次结算价': 'PreSettlementPrice',
                       '昨收盘': 'PreClosePrice',
                       '昨持仓量': 'PreOpenInterest',
                       '今开盘': 'OpenPrice',
                       '最高价': 'HighestPrice',
                       '最低价': 'LowestPrice',
                       '数量': 'Volume',
                       '成交金额': 'Turnover',
                       '持仓量': 'OpenInterest',
                       '今收盘': 'ClosePrice',
                       '本次结算价': 'SettlementPrice',
                       '涨停板价': 'UpperLimitPrice',
                       '跌停板价': 'LowerLimitPrice',
                       '昨虚实度': 'PreDelta',
                       '今虚实度': 'CurrDelta',
                       '最后修改时间': 'UpdateTime',
                       '最后修改毫秒': 'UpdateMillisec',
                       '申买价一': 'BidPrice1',
                       '申买量一': 'BidVolume1',
                       '申卖价一': 'AskPrice1',
                       '申卖量一': 'AskVolume1',
                       '申买价二': 'BidPrice2',
                       '申买量二': 'BidVolume2',
                       '申卖价二': 'AskPrice2',
                       '申卖量二': 'AskVolume2',
                       '申买价三': 'BidPrice3',
                       '申买量三': 'BidVolume3',
                       '申卖价三': 'AskPrice3',
                       '申卖量三': 'AskVolume3',
                       '申买价四': 'BidPrice4',
                       '申买量四': 'BidVolume4',
                       '申卖价四': 'AskPrice4',
                       '申卖量四': 'AskVolume4',
                       '申买价五': 'BidPrice5',
                       '申买量五': 'BidVolume5',
                       '申卖价五': 'AskPrice5',
                       '申卖量五': 'AskVolume5',
                       '当日均价': 'AveragePrice',
                       '业务日期': 'ActionDay'}, inplace=True)

in2_df.rename(columns={'交易日': 'TradingDay',
                       '合约代码': 'InstrumentID',
                       '交易所代码': 'ExchangeID',
                       '合约在交易所的代码': 'ExchangeInstID',
                       '最新价': 'LastPrice',
                       '上次结算价': 'PreSettlementPrice',
                       '昨收盘': 'PreClosePrice',
                       '昨持仓量': 'PreOpenInterest',
                       '今开盘': 'OpenPrice',
                       '最高价': 'HighestPrice',
                       '最低价': 'LowestPrice',
                       '数量': 'Volume',
                       '成交金额': 'Turnover',
                       '持仓量': 'OpenInterest',
                       '今收盘': 'ClosePrice',
                       '本次结算价': 'SettlementPrice',
                       '涨停板价': 'UpperLimitPrice',
                       '跌停板价': 'LowerLimitPrice',
                       '昨虚实度': 'PreDelta',
                       '今虚实度': 'CurrDelta',
                       '最后修改时间': 'UpdateTime',
                       '最后修改毫秒': 'UpdateMillisec',
                       '申买价一': 'BidPrice1',
                       '申买量一': 'BidVolume1',
                       '申卖价一': 'AskPrice1',
                       '申卖量一': 'AskVolume1',
                       '申买价二': 'BidPrice2',
                       '申买量二': 'BidVolume2',
                       '申卖价二': 'AskPrice2',
                       '申卖量二': 'AskVolume2',
                       '申买价三': 'BidPrice3',
                       '申买量三': 'BidVolume3',
                       '申卖价三': 'AskPrice3',
                       '申卖量三': 'AskVolume3',
                       '申买价四': 'BidPrice4',
                       '申买量四': 'BidVolume4',
                       '申卖价四': 'AskPrice4',
                       '申卖量四': 'AskVolume4',
                       '申买价五': 'BidPrice5',
                       '申买量五': 'BidVolume5',
                       '申卖价五': 'AskPrice5',
                       '申卖量五': 'AskVolume5',
                       '当日均价': 'AveragePrice',
                       '业务日期': 'ActionDay'}, inplace=True)

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
# 删除无用数据列
merge_df.drop(['BidPrice2_P', 'BidVolume2_P', 'AskPrice2_P', 'AskVolume2_P',
               'BidPrice3_P', 'BidVolume3_P', 'AskPrice3_P', 'AskVolume3_P',
               'BidPrice4_P', 'BidVolume4_P', 'AskPrice4_P', 'AskVolume4_P',
               'BidPrice5_P', 'BidVolume5_P', 'AskPrice5_P', 'AskVolume5_P',
               'BidPrice2_Y', 'BidVolume2_Y', 'AskPrice2_Y', 'AskVolume2_Y',
               'BidPrice3_Y', 'BidVolume3_Y', 'AskPrice3_Y', 'AskVolume3_Y',
               'BidPrice4_Y', 'BidVolume4_Y', 'AskPrice4_Y', 'AskVolume4_Y',
               'BidPrice5_Y', 'BidVolume5_Y', 'AskPrice5_Y', 'AskVolume5_Y'
               ], axis=1, inplace=True)
# 输出文件
out1_file = 'output/2018-11-01-P&Y-merge.csv'
merge_df.to_csv(out1_file, encoding="gbk")

# 开始resample的处理过程
# 将时间转换为datetime格式存入新列
merge_df['TradingDay'] = pd.to_datetime(merge_df['TradingDay'], format='%Y%m%d')
merge_df['UpdateTimeCombineMS'] = pd.to_datetime(merge_df['TradingDay'].apply(lambda x: x.strftime('%Y%m%d'))
                                                 + ' ' + merge_df['UpdateTime'], format='%Y%m%d %H:%M:%S')
merge_df['UpdateTimeCombineMS'] = merge_df['UpdateTimeCombineMS'] + pd.to_timedelta(merge_df['UpdateMillisec'],
                                                                                    unit='ms')
# print(merge_df[:10])

# 将datetime列设为index
pre_resample_df = merge_df.set_index('UpdateTimeCombineMS')
pre_resample_df = pre_resample_df[['BidPrice1_P', 'BidVolume1_P', 'AskPrice1_P', 'AskVolume1_P',
                                   'BidPrice1_Y', 'BidVolume1_Y', 'AskPrice1_Y', 'AskVolume1_Y',
                                   'UpdateTime_P', 'UpdateMillisec_P', 'UpdateTime_Y', 'UpdateMillisec_Y']]
# print(pre_resample_df[:10])

# 连续每秒做一分钟resample
for t_delta in range(0, 60):
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
    # 输出文件位置
    out2_file = 'output/2018-11-01-P&Y-resample-{0}.csv'.format(t_delta)
    copy_final_df.to_csv(out2_file, encoding="gbk")

    # debug
    # print(copy_final_df.head())
    # ipdb.set_trace()
