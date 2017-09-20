#coding=utf-8
#csv 名字记录
import matplotlib.pyplot as plt
import numpy as np
from sys import path

path.append('../')
from const import *
print base_path_02_yous

import pandas as pd

names1880 =pd.read_csv(base_path_02_yous+"yob1880.txt",names=['name','sex','births'])
print names1880[:2]

mames1880=names1880.groupby('sex').births.sum();
print mames1880[:2]

years = range(1880,2011)
pieces =[]
columns = ['name','sex','births']

for year in years:
    path = base_path_02_yous+"yob%d.txt" % year
    frame = pd.read_csv(path,names=columns)

    frame['year'] = year
    pieces.append(frame)

#连接多个frame
names = pd.concat(pieces,ignore_index=True)

print names[:2]
#按年份性别分组
total_births = names.pivot_table('births', index='year', columns='sex', aggfunc='sum')
print total_births.tail()
total_births.plot(title='total births by sex animport numpy as npd year')

#添加一列表示总量的百分比
def add_prop(group):
    births = group.births.astype(float)
    group['prop'] = births / births.sum()
    return group

names = names.groupby(['year','sex']).apply(add_prop)
print names[:2]
#验证百分比之和是否为1
print np.allclose(names.groupby(['year','sex']).prop.sum(), 1)

#性别时间分组的钱1000个选取,年份+性别分组
def get_top1000(group):
    # print group[:1]
    return group.sort_values(by='births',ascending=False)[:1000]

grouped = names.groupby(['year', 'sex'])
top1000 = grouped.apply(get_top1000)


boys = top1000[top1000.sex == 'M']
girls = top1000[top1000.sex == 'F']

total_births = top1000.pivot_table('births', index='year', columns='name', aggfunc='sum')
subset = total_births[['John', 'Harry', 'Mary', 'Marilyn']]
subset.plot(subplots=True,figsize=(12,10),grid=False,title='number of births per year')

#验正常见名字越来越少
table = top1000.pivot_table('prop', index='year', columns='sex', aggfunc='sum')
table.plot(title='sum of table1000.prop by year and sex',yticks=np.linspace(0,1.2,13),xticks=range(1880,2020,10))


#筛选2010年男生
df = boys[boys.year == 2010]

#有多少名字加起来才能50%
prop_cumsum = df.sort_values(by='prop',ascending=False).prop.cumsum()
print prop_cumsum[:3]
print prop_cumsum.searchsorted(0.5)

plt.show();

