# coding=utf-8

import washer_utils
'''
任务描述：
测试任务
'''


'''
需实现Task函数
@param server_id 目标服务器
@param db_type 目标数据库类型
@param date_time 执行时间点
@return 返回提取出来的数据
@return 返回数据属于哪一天(字符串格式)
'''

def Task(server_id, db_type, date_time):
    conn = washer_utils.GetServerConn(server_id, db_type)
    cur = conn.cursor()
    cur.execute("select * from src_1")
    ret = cur.fetchall()
    arr = date_time.split(" ")
    conn.close()
    return ret, arr[0]


'''
可选的：任务执行后调用
@param server 数据源服务器
@param date_time 执行时间点
'''
def AfterProcess(server, date_time):

    pass
