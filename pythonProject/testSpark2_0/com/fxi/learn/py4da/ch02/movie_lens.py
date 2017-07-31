#coding=utf-8
#电影评分数据
from sys import path

path.append('../')
from const import *
print base_path_02_move_users

import pandas as pd
#加载数据
unmaes =['user_id','gender','age','occupation','zip']
users = pd.read_table(base_path_02_move_users, sep='::', header=None, names=unmaes,engine = "python")

rnames = ['user_id','movie_id','rating', 'timestamp']
ratings = pd.read_table(base_path_02_move_rate, sep='::', header=None, names=rnames,engine = "python")

mnames= ['movie_id', 'title', 'genres']
movies = pd.read_table(base_path_02_move_movie, sep='::', header=None, names=mnames,engine = "python")

print users[:1]
print ratings[:1]
print movies[:1]
#合并数据
data = pd.merge(pd.merge(ratings, users), movies)
print data[:2]


#根据title和性别分组
mean_ratings = data.pivot_table('rating', index='title', columns='gender', aggfunc='mean')
print mean_ratings[:2]

#根据title分组
ratings_by_title = data.groupby('title').size()
print ratings_by_title[:1]

#过滤评分数据超过250条的记录,并且创建索引
active_titles= ratings_by_title.index[ratings_by_title > 250]
print active_titles[:1]

#选取匹配的行
mean_ratings = mean_ratings.ix[active_titles]
print mean_ratings[:1]

#对于女性降序排序
top_fe_ratings = mean_ratings.sort_values(by='F', ascending=False)
print top_fe_ratings[:2]

#找出男女最大分歧
#新加入一列
mean_ratings['df']=mean_ratings['M']-mean_ratings['F']
print mean_ratings.sort_values(by='df')[:3]

#标准差
rating_std_by_title = data.groupby('title')['rating'].std()
rating_std_by_title = rating_std_by_title.ix[active_titles]
print rating_std_by_title.sort_values(ascending=False)[:3]



