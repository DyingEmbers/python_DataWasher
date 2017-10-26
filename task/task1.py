# coding=utf-8
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

def Task(conn, date_time):
    cur = conn.cursor()
    cur.execute("select * from src_1")
    ret = cur.fetchall()
    arr = date_time.split(" ")
    return ret, arr[0]

'''
可选的：任务执行前调用
@param server 数据源服务器
@param date_time 执行时间点
'''
def BeforeProcess(server, date_time):

    pass

'''
可选的：任务执行后调用
@param server 数据源服务器
@param date_time 执行时间点
'''
def AfterProcess(server, date_time):

    pass
