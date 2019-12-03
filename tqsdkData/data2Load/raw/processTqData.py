"""
将原始的天勤bar数据改为vnpy可导入的csv格式数据
"""

import os
import pandas as pd

# 找到parse_tick文件夹下，日期子文件夹下的rb2001合约路径
# https://www.jb51.net/article/155347.htm


def path_name(path, filename):
    """
    os.walk(path)返回三个tuple：
    root：当前目录路径
    dirs：当前路径下所有子目录
    files：当前路径下所有非目录子文件
    """
    L = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if os.path.splitext(file)[0] == filename:
                L.append(os.path.join(root, file))
    return L


def find_csv_names(path):
    names = []
    for i, j, k in os.walk(path):
        names = [kk for kk in k if kk.split(".")[-1] == "csv"]
    return names

# file = os.path.join(path,'SHFE.bu1912.1min.csv')
# df = pd.read_csv(file)
# print(df.columns)
# df.columns = df.columns.map(lambda x: x.split(".")[-1].capitalize())
# df = df.drop(['Open_oi', 'Close_oi'], axis=1)
# print(df.head())


def process_bar_df(df):
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.columns = df.columns.map(lambda x: x.split(".")[-1].capitalize())
    df = df.drop(['Open_oi', 'Close_oi'], axis=1)
    return df


def process_tick_df(df):
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['symbol'] = df.columns[1].split(".")[1]
    df['exchange'] = df.columns[1].split(".")[0]
    df.columns = df.columns.map(lambda x: x.split(".")[-1])
    df = df.dropna()
    return df


# if __name__ == '__main__':
# Python获取文件夹的上一级路径:https://blog.csdn.net/qq_29592829/article/details/83151499
currentPath = os.getcwd()
destinationPath = os.path.abspath(os.path.join(os.path.dirname("__file__"), os.path.pardir)+"\\processed")
names = find_csv_names(currentPath)

for name in names:
    raw = pd.read_csv(os.path.join(currentPath, name))
    # process_bar_df(raw).to_csv(os.path.join(destinationPath, name))  # bar data
    process_tick_df(raw).to_csv(os.path.join(destinationPath, name))  # tick data


