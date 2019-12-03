# https://doc.shinnytech.com/pysdk/latest/reference/tqsdk.tools.download.html?tdsourcetag=s_pctim_aiomsg

from datetime import datetime, date
from contextlib import closing
from tqsdk import TqApi, TqSim
from tqsdk.tools import DataDownloader

start_dt = datetime(2019, 8, 15, 6, 0, 0)
end_dt = datetime(2019, 11, 15, 16, 0, 0)
symbols = ['DCE.a2001', 'DCE.b2001', 'DCE.c2001', 'DCE.eg2001', 'DCE.l2001', 'DCE.m2001', 'DCE.p2001', 'DCE.pp2001', \
              'DCE.v2001', 'DCE.y2001', 'SHFE.ag1912', 'SHFE.bu1912', 'SHFE.fu2001', 'SHFE.hc2001', 'SHFE.rb2001', \
              'SHFE.sp2001', 'CZCE.CF001', 'CZCE.FG001', 'CZCE.MA001', 'CZCE.OI001', 'CZCE.RM001', 'CZCE.SF001', \
              'CZCE.SM001', 'CZCE.SR001', 'CZCE.TA001', 'CZCE.UR001', 'CZCE.ZC001']

api = TqApi(TqSim())
download_tasks = {}
# 下载从 2018-01-01 到 2018-09-01 的 SR901 日线数据
# download_tasks["SR_daily"] = DataDownloader(api, symbol_list="CZCE.SR901", dur_sec=24*60*60,
#                     start_dt=date(2018, 1, 1), end_dt=date(2018, 9, 1), csv_file_name="SR901_daily.csv")
# 下载从 2017-01-01 到 2018-09-01 的 rb主连 5分钟线数据
# download_tasks["rb_5min"] = DataDownloader(api, symbol_list="KQ.m@SHFE.rb", dur_sec=5*60,
#                     start_dt=date(2017, 1, 1), end_dt=date(2018, 9, 1), csv_file_name="rb_5min.csv")
# 下载从 2018-01-01凌晨6点 到 2018-06-01下午4点 的 cu1805,cu1807,IC1803 分钟线数据，所有数据按 cu1805 的时间对齐
# 例如 cu1805 夜盘交易时段, IC1803 的各项数据为 N/A
# 例如 cu1805 13:00-13:30 不交易, 因此 IC1803 在 13:00-13:30 之间的K线数据会被跳过
# for symbol in symbols:
#     download_tasks[symbol] = DataDownloader(api, symbol_list=symbol, dur_sec=60, start_dt=start_dt, end_dt=end_dt,
#                                             csv_file_name=symbol+".1min.csv")

# download_tasks["cu_min"] = DataDownloader(api, symbol_list=["SHFE.cu1805", "SHFE.cu1807", "CFFEX.IC1803"], dur_sec=60,
#                     start_dt=datetime(2018, 1, 1, 6, 0 ,0), end_dt=datetime(2018, 6, 1, 16, 0, 0), csv_file_name="cu_min.csv")
# 下载从 2018-05-01凌晨0点 到 2018-06-01凌晨0点 的 T1809 盘口Tick数据
download_tasks["T_tick"] = DataDownloader(api, symbol_list=["CFFEX.IH1912"], dur_sec=0,
                    start_dt=datetime(2019, 10, 8), end_dt=datetime(2019, 11, 28), csv_file_name="IH1912_tick_20191128.csv")
# 使用with closing机制确保下载完成后释放对应的资源
with closing(api):
    while not all([v.is_finished() for v in download_tasks.values()]):
        api.wait_update()
        print("progress: ", { k:("%.2f%%" % v.get_progress()) for k,v in download_tasks.items() })