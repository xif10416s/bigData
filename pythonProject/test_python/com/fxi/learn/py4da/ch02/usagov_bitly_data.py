#coding=utf-8
from pandas import DataFrame,Series
import matplotlib
import matplotlib.pyplot as plt
from sys import path
path.append('../')
from const import *
print base_path_02_gov
import json

import pandas as pd
import numpy as np
#加载数据变成json对象
records = [json.loads(line) for line in open(base_path_02_gov)]
#转化成dataframe
frame = DataFrame(records)
#tz 地区 前10的记录
print frame['tz'][:10]
#tz 前10的 地区和 数量
print frame['tz'].value_counts()[:10]
#过滤缺失值/空值
clean_tz = frame['tz'].fillna('Missing')
clean_tz[clean_tz == ''] = 'Unkown'
tz_counts = clean_tz.value_counts()
tz_counts[:10].plot(kind='barh',rot=0)
# plt.show()


print frame.head(1)
print frame.a[:3]

#过滤代理为空的
cframe = frame[frame.a.notnull()]
#删选出window的记录
op_sys = np.where(cframe['a'].str.contains('Windows'),'Windows','Not Windows')
print op_sys[:5]

#根据时区和操作系统分组
by_tz_os = cframe.groupby(['tz',op_sys])
agg_counts= by_tz_os.size().unstack().fillna(0)
print agg_counts[:10]

#选取最长出现的时区,根据行数构建一个间接索引数组
indexer = agg_counts.sum(1).argsort()
print indexer[:10]

#通过take按照这个顺序截取最后10行
count_subSet = agg_counts.take(indexer)[-10:]
count_subSet.div(count_subSet.sum(1),axis=0).plot(kind='barh',stacked =True)
plt.show()
