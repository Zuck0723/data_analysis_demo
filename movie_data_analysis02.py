# -*- coding:utf-8 -*-
import sqlite3
import pandas as pd
import jieba
import pyecharts as pec
import math

FILTER_WORDS = ['知道','电影','影评','影片','这个','那个','怎么','如果',
				'是','的','这个','一个','这种','时候','什么','\n',
				'一部','这部','没有','还有','我们','觉得','感觉','片子',
				'因为','东西','看到','可能','豆瓣','to','of','the','is','maybe','不但','而且','这么','那么','好像','Hope']

# 设计函数
def get_movie_id_list(min_comment_count):
# 统计各个电影的评论数，并且返回Series类型
	movie_list = comment_data['MOVIEID'].value_counts()
# 筛选评论数量大于100的电影
	movie_list = movie_list[movie_list.values > min_comment_count]

	return movie_list.index

def get_comment_keywords_counts(movie_id, count):
# 获取某个电影的全部评论内容
# 根据ID筛选评论
	comment_list_of_the_movie = comment_data[comment_data['MOVIEID']==movie_id]['CONTENT']
	# 整合全部评论为一个字符串，以换行隔开
	comment_str_all = ''
	for comment_str in comment_list_of_the_movie:
		comment_str_all += comment_str + '\n'
	seg_list = list(jieba.cut(comment_str_all))
	# 获取分词后的关键词列表
	keyword_counts = pd.Series(seg_list)
	keyword_counts = keyword_counts[keyword_counts.str.len() > 1]
	keyword_counts = keyword_counts[~keyword_counts.str.contains('|'.join(FILTER_WORDS))]
	# 统计每个词的出现次数，利用str.contains()筛选数据
	keyword_counts = keyword_counts.value_counts()[:count]

	return keyword_counts

def get_movie_name_and_score(movie_id):
	movie_link = 'https://movie.douban.com/subject/{}/'.format(movie_id)
	search_results = movie_data[movie_data['链接']==movie_link]

# 此处douban_movie_data中体现的ID的是链接
	if len(search_results) > 0:
		search_results = search_results.iloc[0]
		movie_name = search_results['电影名']
		movie_score = search_results['评分']
	else:
		movie_name = '未找到'
		movie_score = -1

	# 以元组的形式返回多个数据
	return (movie_name, movie_score)

def save_keywords_counts(datazoom_count):
	# 循环所有得分区域，统计各得分区域关键词词频
	for i in range(datazoom_count):
		if kw_list_by_scorezoom[i]:
			keywords_counts_by_datazoom = pd.DataFrame({
					'kw': kw_list_by_scorezoom[i],
					'counts': kw_counts_list_by_scorezoom[i]
				})
			keywords_counts_by_datazoom = keywords_counts_by_datazoom.groupby('kw').sum()
			keywords_counts_by_datazoom = keywords_counts_by_datazoom.sort_values(by='counts', ascending=False)
			#keywords_counts_by_datazoom['kw'] = keywords_counts_by_datazoom.index
			counts_sum = keywords_counts_by_datazoom['counts'].sum()
			keywords_counts_by_datazoom['percentage'] = keywords_counts_by_datazoom['counts'] / counts_sum
			keywords_counts_by_datazoom.to_csv(r'movie_keywords_by_score/{}_score_movie_keywords.csv'.format(i))

scorezoom_count = 20
min_comment_count = 50
count = 50

# 创建连接
conn = sqlite3.connect(r'数据分析/data/douban_comment_data.db')
# 通过连接执行sql语句，获取查询结果，DataFrame类型
comment_data = pd.read_sql_query('select * from comment;', conn)
# 获取电影数据
movie_data = pd.read_excel(r'数据分析/data/douban_movie_data.xlsx')

# 将评分0-10以0.5为一个区域，划分为20个区域
kw_list_by_scorezoom = [[] for i in range(scorezoom_count)]
kw_counts_list_by_scorezoom = [[] for i in range(scorezoom_count)]
# 评论数至少100条的电影的ID列表
movie_list = get_movie_id_list(min_comment_count)
for movie_id in movie_list:
	keywords_counts_of_movie_by_limit = get_comment_keywords_counts(movie_id, count)
	# 以逗号隔开的多个变量可以接收元组形式的返回值
	movie_name, movie_score = get_movie_name_and_score(movie_id)
	try:
		kw_list_by_scorezoom[math.floor(movie_score*2)].extend(keywords_counts_of_movie_by_limit.index)
		kw_counts_list_by_scorezoom[math.floor(movie_score*2)].extend(keywords_counts_of_movie_by_limit.values)
	except:
		pass

save_keywords_counts(scorezoom_count)

keywords_counts = []
for i in range(scorezoom_count):
	try:
		keywords_counts.append(pd.read_csv(r'movie_keywords_by_score/{}_score_movie_keywords.csv'.format(i)))
	except:
		keywords_counts.append(pd.DataFrame())
# 以9分区域电影前30个高频关键词为准来统计其他得分区域的情况
top_kw_percentage_df = pd.DataFrame(
		[],
		columns=list(range(scorezoom_count)),
		index=keywords_counts[scorezoom_count-1]['kw'][:30]
	)
for i in range(scorezoom_count):
	kw = keywords_counts[i]
	if not kw.empty:
		kw = kw[kw['kw'].isin(top_kw_percentage_df.index)]
		top_kw_percentage_df[i] = pd.Series(list(kw['percentage']), index=kw['kw'])
top_kw_percentage_df.fillna(0, inplace=True)
top_kw_percentage_df.to_csv('top_kw_percentage_df.csv')

data = []
i = 0
for index in top_kw_percentage_df.index:
	j = 0
	for score in top_kw_percentage_df.columns:
		data.append([j,i,top_kw_percentage_df[score][index]*100])
		j += 1
	i += 1

# 创建热力图展示结果
heatmap = pec.HeatMap()
heatmap.add("",
	top_kw_percentage_df.columns,
	top_kw_percentage_df.index,
	data,
	is_visualmap=True,
	visual_text_color='#000',
	visual_range=[0,10],
	visual_orient='horizontal'
	)
heatmap.render(path='top_heatmap.html')

# 创建柱状图展示结果
bar3d = pec.Bar3D("3D 柱状图示例", width=1200, height=600)
range_color = ['#313695', '#4575b4', '#74add1', '#abd9e9', '#e0f3f8', '#ffffbf',
				'#fee090', '#fdae61', '#f46d43', '#d73027', '#a50026']
bar3d.add("", 
	top_kw_percentage_df.columns,
	top_kw_percentage_df.index,
	data,
	is_visualmap=True,
	visual_range=[0, 10],
	visual_range_color=range_color,
	grid3d_width=160,
	grid3d_depth=80
)
bar3d.render(path='top_bar3d.html')

# 以3分区域电影前50个高频关键词为准来统计其他得分区域的情况
low_kw_percentage_df = pd.DataFrame(
		[],
		columns=list(range(scorezoom_count)),
		index=keywords_counts[5]['kw'][:20]
	)
for i in range(scorezoom_count):
	kw = keywords_counts[i]
	if not kw.empty:
		kw = kw[kw['kw'].isin(low_kw_percentage_df.index)]
		low_kw_percentage_df[i] = pd.Series(list(kw['percentage']), index=kw['kw'])
low_kw_percentage_df.fillna(0, inplace=True)
low_kw_percentage_df.to_csv('low_kw_percentage_df.csv')

data = []
i = 0
for index in low_kw_percentage_df.index:
	j = 0
	for score in low_kw_percentage_df.columns:
		data.append([j,i,low_kw_percentage_df[score][index]*100])
		j += 1
	i += 1

#创建热力图展示结果
heatmap = pec.HeatMap()
heatmap.add("",
	low_kw_percentage_df.columns,
	low_kw_percentage_df.index,
	data,
	is_visualmap=True,
	visual_text_color='#000',
	visual_range=[0,10],
	visual_orient='horizontal'
	)
heatmap.render(path='low_heatmap.html')

#创建柱状图展示结果
bar3d = pec.Bar3D("3D柱状图", width=1200, height=600)
range_color = ['#313695', '#4575b4', '#74add1', '#abd9e9', '#e0f3f8', '#ffffbf',
				'#fee090', '#fdae61', '#f46d43', '#d73027', '#a50026']
bar3d.add("", 
	low_kw_percentage_df.columns,
	low_kw_percentage_df.index,
	data,
	is_visualmap=True,
	visual_range=[0, 10],
	visual_range_color=range_color,
	grid3d_width=160,
	grid3d_depth=80
)
bar3d.render(path='low_bar3d.html')
