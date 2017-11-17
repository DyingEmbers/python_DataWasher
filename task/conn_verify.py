# coding=utf-8

import washer_utils, MySQLdb
'''
任务描述：
测试任务
'''

def TestDBConn(ip, port, user, pwd, db_name):
    try:
        conn = MySQLdb.connect(
            host=ip,
            port=port,
            user=user,
            passwd=pwd,
            db=db_name,
            cursorclass=MySQLdb.cursors.DictCursor,
        )
    except Exception,e:
        return e
    conn.close()
    return "OK"


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
    cur.execute("select * from config_server_list")
    conn_list = cur.fetchall()
    ret = []
    for line in conn_list:
        tmp = {"server_id":line["server_id"], "db_type":line["db_type"]}
        tmp["state"] = TestDBConn(line["ip"], line["port"], line["user"], line["password"], line["database"])
        ret.append(tmp)

    arr = date_time.split(" ")
    conn.close()

    # 手动清空之前的数据
    ana_conn = washer_utils.GetServerConn("ana", "ana_db")
    cur = ana_conn.cursor()
    cur.execute("delete from ana_conn_stat")
    ana_conn.commit()
    return ret, arr[0]


'''
可选的：任务执行后调用
@param server 数据源服务器
@param date_time 执行时间点
'''
def AfterProcess(server, date_time):

    pass
