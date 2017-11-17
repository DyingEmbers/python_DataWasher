# coding=utf-8

import json
from collections import OrderedDict
import urllib
import datetime
import time

'''
数据库相关常用函数
'''

# json转成redis key，
def TaskDict2RedisKey(json_str):
    out_put = json.loads(json_str, object_pairs_hook=OrderedDict)
    out_str = ""
    first = True
    for key in out_put.keys():
        if not first: out_str += "_"
        out_str += urllib.urlencode(out_put[key])
        first = False
    return out_str


def CheckTaskState(redis_conn, redis_key):
    # 检查任务执行状态
    result = redis_conn.get(redis_key + "result")
    if result == "0" or result == "2":
        return False  # 任务处于完成，或者执行中的状态，则跳过

    # 清理现场
    while redis_conn.llen(redis_key) != 0:
        redis_conn.lpop(redis_key)

    return True


# 讲字符串时间转换成datetime
def ParseDateTime(input_str, tm_format="%Y-%m-%d %H:%M:%S"):
    return datetime.datetime.fromtimestamp(time.mktime(time.strptime(input_str, tm_format)))

# 将字符串格式的时间转换成下划线日期
def DateFormat(time):
    date_time = ParseDateTime(time)
    return date_time.strftime("%Y_%m_%d")

