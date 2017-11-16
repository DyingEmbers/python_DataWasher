# coding=utf-8
import json, task_utils

'''
任务描述：
测试任务
'''


'''
需实现Task函数
@param redis_conn redis连接
@param json_task 任务参数
'''

def Task(redis_conn, task_json):
    redis_key = task_utils.TaskDict2RedisKey(task_json)
    if not task_utils.CheckTaskState(redis_conn, redis_key): return

    show = [{"1": 1, "2": 2}, {"1": 3, "2": 4}]
    for line in show:
        redis_conn.rpush(redis_key, json.dumps(line))

    redis_conn.setex(redis_key + "_result", 1, 60)
    redis_conn.expire(redis_key, 60)

'''
可选的：任务执行后调用
@param server 数据源服务器
@param date_time 执行时间点
'''
def AfterProcess(server, date_time):

    pass
