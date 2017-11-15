# coding=utf-8

import json
from collections import OrderedDict

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
        out_str += out_put[key]
        first = False
    return out_put
