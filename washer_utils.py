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

import MySQLdb.cursors
import MySQLdb,time

def CPU_STAT(func, *args, **kwargs):
    def wrapper(*args, **kwargs):
        start = time.time()
        ret = func(*args, **kwargs)
        end = time.time()
        timestrap = end - start
        print('function %s running time is %s' % (func.__name__, timestrap))
        return ret
    return wrapper


# 配置库配置
__CFG_WAHSER_IP = "127.0.0.1"
__CFG_WASHER_PORT = 3306
__CFG_WASHER_USER = "root"
__CFG_WASHER_PWD = "123456"
__CFG_WASHER_CFG_DB_NAME = "washer_cfg"
__CFG_WASHER_DATA_DB_NAME = "washer_mgr"


def GetWasherCfgConn():
    global __CFG_WAHSER_IP, __CFG_WASHER_PORT, __CFG_WASHER_USER, __CFG_WASHER_PWD, __CFG_WASHER_CFG_DB_NAME
    conn = MySQLdb.connect(
        host=__CFG_WAHSER_IP,
        port=__CFG_WASHER_PORT,
        user=__CFG_WASHER_USER,
        passwd=__CFG_WASHER_PWD,
        db=__CFG_WASHER_CFG_DB_NAME,
        cursorclass=MySQLdb.cursors.DictCursor,
    )

    return conn

def GetWasherDataConn():
    global __CFG_WASHER_DATA_DB_NAME
    return GetServerConn(__CFG_WASHER_DATA_DB_NAME)

# 获取数据源连接
def GetServerConn(server_id):
    conn = GetWasherCfgConn()
    cur = conn.cursor()
    cur.execute("select `ip`, `port`, `user`, `password`, `database` from server_list where server_id = '" + server_id + "'" )
    result = cur.fetchone()

    conn = MySQLdb.connect(
        host=result["ip"],
        port=result["port"],
        user=result["user"],
        passwd=result["password"],
        db=result["database"],
        cursorclass=MySQLdb.cursors.DictCursor,
    )
    if not conn:
        print "taget db[%s] not found " % server_id
    cur = conn.cursor()

    return conn

# 获取任务列表
def GetActiveTask():
    washer_conn = GetWasherCfgConn()
    cur = washer_conn.cursor()
    cur.execute("select task_id, game, py_name, exec_tm, last_tm from task_list where active = 1")
    task_list = cur.fetchall()
    cur.close()
    washer_conn.close()
    return task_list

# 获取任务列表
def GetServerList(game):
    washer_conn = GetWasherCfgConn()
    cur = washer_conn.cursor()
    cur.execute("select server_id from server_list where game = '" + game + "'")
    server_list = cur.fetchall()
    cur.close()
    washer_conn.close()
    return server_list

def GetTaskConfig(task_id):
    conn = GetWasherCfgConn()
    cur = conn.cursor()
    cur.execute("select game, py_name, save_name, day_one, unique_key, exec_tm from task_list where task_id = '" + str(task_id) + "'")
    task = cur.fetchone()
    cur.close()
    conn.close()
    return task

def GetZoneByIP(ip):
    conn = GetWasherCfgConn()
    cur = conn.cursor()
    cur.execute("select zone from zone_cfg where ip = '" + str(ip) + "'")
    zone = cur.fetchone()
    cur.close()
    conn.close()
    if not zone:
        print "can not get washer zone by ip " + ip
        return None
    return zone["zone"]

def GetZoneByServerID(server_id):
    conn = GetWasherCfgConn()
    cur = conn.cursor()
    cur.execute("select zone from server_list where server_id = '" + server_id + "'")
    zone = cur.fetchone()
    cur.close()
    conn.close()
    if not zone:
        print "can not get washer zone by server id " + server_id
        return None
    return zone["zone"]

# tasktype	int	任务类型(1来自自动任务,2来自…)
# taskid	str	任务名
# dsSrc	    str	来源数据源
# dsTar	    str	目标数据源
# params	json	执行参数
# scriptID	str	执行脚本
# startTm	datetime	开始执行时间
# stopTm	datetime	结束执行时间
# status	int	执行状态
# len	    int	任务长度
# pos	    int	任务位置
# code	    int	失败code
# msg	    str	失败msg
def InsertTaskData(taskid, src_db, tar_db, param, script_id, len=0):

    create_sql = """
    CREATE TABLE IF NOT EXISTS`task_state_log` (
    PRIMARY KEY (`_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """
    conn = GetWasherDataConn()
    cursor = conn.cursor()

    # 创建表格
    cursor.execute(create_sql)
    conn.commit()

    # 插入任务记录
    sql = "INSERT INTO task_state_log(task_id, ds_src, ds_tar, " \
          "params, script_id, len, start_tm) value (%s,%s,%s,%s,%s,%d, now())"
    sql = sql % (taskid, src_db, tar_db, param, script_id, len)
    cursor.execute(sql)
    conn.commit()
    conn.close()

# state 执行状态
# 0 准备
# 1 执行中
# 2 执行成功
# 3 执行失败
def UpdateTaskState(idx, state, code = 0, msg = ""):
    conn = GetWasherDataConn()
    cursor = conn.cursor()
    sql = "update task_state_log set state = %d, set code=%d, set msg = %s " % (state, code, msg)
    if state == 2 or state == 3:
        sql += " set end_tm = now() "
    sql += " where _id = %d" % idx
    cursor.execute(sql)
    conn.commit()

def UpdateTaskPos(idx, pos):
    conn = GetWasherDataConn()
    cursor = conn.cursor()
    sql = "update task_state_log set pso= %d" % pos
    sql += " where _id = %d" % idx
    cursor.execute(sql)
    conn.commit()


