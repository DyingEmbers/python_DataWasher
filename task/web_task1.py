# coding=utf-8
import json

'''
任务描述：
测试任务
'''


'''
需实现Task函数
@param conn 目标数据库连接
@param date_time 执行时间点
@return 返回提取出来的数据
@return 返回数据属于哪一天(字符串格式)
'''

def Task(redis_conn, task_json):
    print task_json
    show = [{"1":1,"2":2}, {"1":3,"2":4}]
    redis_conn.set(task_json, json.dumps(show))
    pass


'''
可选的：任务执行后调用
@param server 数据源服务器
@param date_time 执行时间点
'''
def AfterProcess(server, date_time):

    pass
