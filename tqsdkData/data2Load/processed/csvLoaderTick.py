# https://www.vnpy.com/forum/topic/1421-zai-ru-tickshu-ju-csvge-shi-dao-shu-ju-ku-zhong

"""
基于csv格式的特点，开发载入tick数据到数据库的脚本，脚本功能如下：
在同一文件夹下，用for循环读取csv文件并载入到数据库
合成时间字符串，并且最终转换为datetime格式
通过datetime来判断非交易时间段，剔除垃圾数据的载入
"""

import os
import csv
from datetime import datetime, time

from vnpy.trader.constant import Exchange
from vnpy.trader.database import database_manager
from vnpy.trader.object import TickData


def run_load_csv():
    """
    遍历同一文件夹内所有csv文件，并且载入到数据库中
    """
    for file in os.listdir("."):
        if not file.endswith(".csv"):
            continue

        print("载入文件：", file)
        csv_load(file)


def csv_load(file):
    """
    读取csv文件内容，并写入到数据库中
    """
    with open(file, "r") as f:
        reader = csv.DictReader(f)

        ticks = []
        start = None
        count = 0

        for item in reader:

            # generate datetime
            # date = item["交易日"]
            # second = item["最后修改时间"]
            # millisecond = item["最后修改毫秒"]

            # standard_time = date + " " + second + "." + millisecond
            standard_time = item["datetime"]
            dt = datetime.strptime(standard_time, "%Y-%m-%d %H:%M:%S.%f")

            # temp = Exchange
            # for k, v in Exchange.__members__.items():
            #     if item["exchange"][0] == k:
            #         temp = v

            # filter
            if dt.time() > time(15, 1) and dt.time() < time(20, 59):
                continue

            tick = TickData(
                symbol=item["symbol"],
                datetime=dt,
                # exchange=filter(lambda v: v for k, v in Exchange.__members__.items() if item["exchange"][0] == k),
                exchange=Exchange[item["exchange"]],
                last_price=float(item["last_price"]),
                volume=float(item["volume"]),
                bid_price_1=float(item["bid_price1"]),
                bid_volume_1=float(item["bid_volume1"]),
                ask_price_1=float(item["ask_price1"]),
                ask_volume_1=float(item["ask_volume1"]),
                gateway_name="DB",
            )
            ticks.append(tick)

            # do some statistics
            count += 1
            if not start:
                start = tick.datetime

        end = tick.datetime
        database_manager.save_tick_data(ticks)

        print("插入数据", start, "-", end, "总数量：", count)


if __name__ == "__main__":
    run_load_csv()