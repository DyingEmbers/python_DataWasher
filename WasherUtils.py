# coding=utf-8

'''
获取查询结果中的字段描述
index = cur.description

(name,
type_code,
display_size,
internal_size,
precision,
scale,
null_ok)
'''


import MySQLdb,time

def CPU_STAT(func, *args, **kwargs):
    def wrapper(*args, **kwargs):
        start = time.time()
        ret = func(*args, **kwargs)
        end = time.time()
        timestrap = end -start
        print('function %s running time is %s'%(func.__name__,timestrap))
        return ret
    return wrapper


# 配置库配置
__CFG_WAHSER_CFG_IP = "127.0.0.1"
__CFG_WASHER_CFG_PORT = 3306
__CFG_WASHER_CFG_USER = "root"
__CFG_WASHER_CFG_PWD = "123456"
__CFG_WASHER_CFG_DB_NAME = "washer_cfg"

def GetWasherCfgConn():
    global __CFG_WAHSER_CFG_IP, __CFG_WASHER_CFG_PORT, __CFG_WASHER_CFG_USER, __CFG_WASHER_CFG_PWD, __CFG_WASHER_CFG_DB_NAME
    conn = MySQLdb.connect(
        host=__CFG_WAHSER_CFG_IP,
        port=__CFG_WASHER_CFG_PORT,
        user=__CFG_WASHER_CFG_USER,
        passwd=__CFG_WASHER_CFG_PWD,
        db=__CFG_WASHER_CFG_DB_NAME,
    )

    return conn


# 获取数据源连接
def GetServerConn(game, server_id):
    conn = GetWasherCfgConn()
    cur = conn.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("select `ip`, `port`, `user`, `password`, `database` from server_list where game = '" + game + "' and server_id = '" + server_id + "'" )
    result = cur.fetchone()

    conn = MySQLdb.connect(
        host=result["ip"],
        port=result["port"],
        user=result["user"],
        passwd=result["password"],
        db=result["database"],
    )
    cur.close()
    return conn