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
        val = out_put[key]
        if isinstance(val, basestring):
            out_str += urllib.quote_plus(val)
        elif isinstance(val, int):
            out_str += str(val)
        elif isinstance(val, list):
            for i in val:
                out_str += urllib.quote(i)
        else:
            print "ERRO: unsupport type" + str(type(val))
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
def ParseDateTime(input_str, tm_format):
    return datetime.datetime.fromtimestamp(time.mktime(time.strptime(input_str, tm_format)))

# 将字符串格式的时间转换成下划线日期
def DateFormat(str_time, tm_format="%Y-%m-%d %H:%M:%S"):
    date_time = ParseDateTime(str_time, tm_format)
    return date_time.strftime("%Y_%m_%d")


def Test():
    task_json = '{"task_name":"123","server":["ssal022","ssal025","ssal027"],' \
                '"sdf":"","rew":"11/16/2017","vsd":"11/20/2017 15:31:42"}'
    key = TaskDict2RedisKey(task_json)
    print key

    test_tm = "11/16/2017"
    date_format = DateFormat(test_tm, "%m/%d/%Y")
    print date_format

if __name__ == "__main__":
    Test()
