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


__CFG_WASHER_SERVER_ID = "wash_mgr"       # 清洗中心服务器标志服
__CFG_WASHER_DB_TYPE = "wash_db"            # 清洗中心数据类型


def GetWasherCfgConn():
    global __CFG_WAHSER_IP, __CFG_WASHER_PORT, __CFG_WASHER_USER, __CFG_WASHER_PWD, __CFG_WASHER_CFG_DB_NAME
    conn = MySQLdb.connect(
        host=__CFG_WAHSER_IP,
        port=__CFG_WASHER_PORT,
        user=__CFG_WASHER_USER,
        passwd=__CFG_WASHER_PWD,
        db=__CFG_WASHER_CFG_DB_NAME,
        cursorclass=MySQLdb.cursors.DictCursor,
        # use_unicode=True,
        charset="utf8",
    )

    cur = conn.cursor()
    cur.execute("SET NAMES utf8")
    cur.execute('SET character_set_connection=utf8;')
    conn.commit()
    return conn

def GetWasherDataConn():
    global __CFG_WASHER_SERVER_ID, __CFG_WASHER_DB_TYPE
    return GetServerConn(__CFG_WASHER_SERVER_ID, __CFG_WASHER_DB_TYPE)

# 获取数据源连接
def GetServerConn(server_id, db_type, time_node=None):
    conn = GetWasherCfgConn()
    cur = conn.cursor()
    sql = "select `ip`, `port`, `user`, `password`, `database` from config_server_list" \
          " where server_id = '%s' and db_type = '%s'" % (server_id, db_type)
    # time_node 预留以支持数据库优先时间段， 避免game_log无法重建

    cur.execute(sql)
    result = cur.fetchone()
    if not result:
        print "taget db[%s] type[%s] not found " % (server_id, db_type)
        return

    conn = MySQLdb.connect(
        host=result["ip"],
        port=result["port"],
        user=result["user"],
        passwd=result["password"],
        db=result["database"],
        cursorclass=MySQLdb.cursors.DictCursor,
        # use_unicode=True,
        charset="utf8",
    )
    if not conn:
        print "taget db[%s] not found " % server_id
        return

    cur = conn.cursor()
    cur.execute("SET NAMES utf8")
    cur.execute('SET character_set_connection=utf8;')
    conn.commit()
    return conn

# 获取任务列表
def GetActiveTask():
    washer_conn = GetWasherCfgConn()
    cur = washer_conn.cursor()
    cur.execute("select task_id, db_type, py_name, exec_tm, last_tm from config_task_list where active = 1")
    task_list = cur.fetchall()
    cur.close()
    washer_conn.close()
    return task_list

# 获取任务列表
def GetServerList(game, db_type):
    washer_conn = GetWasherCfgConn()
    cur = washer_conn.cursor()
    cur.execute("select server_id from config_server_list "
                "where db_type = %s and game = %s" % (repr(db_type), repr(game)))
    server_list = cur.fetchall()
    cur.close()
    washer_conn.close()
    return server_list

# 获取任务列表
def GetServerListByTaskID(task_id):
    washer_conn = GetWasherCfgConn()
    cur = washer_conn.cursor()
    sql = """
    select t2.* from(
    SELECT game, db_type FROM `config_task_list` where task_id = %d limit 1
    )t1
    inner join (
        select game, server_id, db_type from `config_server_list`
    )t2 on t1.game = t2.game and t1.db_type = t2.db_type
    """ % task_id

    cur.execute(sql)
    server_list = cur.fetchall()
    cur.close()
    washer_conn.close()
    return server_list

def GetTaskConfig(task_id):
    conn = GetWasherCfgConn()
    cur = conn.cursor()
    cur.execute("select game, db_type, py_name, save_name, day_one, unique_key, exec_tm from config_task_list "
                "where task_id = '" + str(task_id) + "'")
    task = cur.fetchone()
    cur.close()
    conn.close()
    return task

def GetZoneByIP(ip):
    conn = GetWasherCfgConn()
    cur = conn.cursor()
    cur.execute("select zone from config_zone_cfg where ip = '" + str(ip) + "'")
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
    cur.execute("select zone from config_server_list where server_id = '" + server_id + "'")
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
def InsertTaskData(task_type, taskid, src_server, src_db, tar_server, tar_db, param, len=0):

    create_sql = """
    CREATE TABLE IF NOT EXISTS`task_state_log` (
    `_id` int(20) NOT NULL AUTO_INCREMENT,
    `tasktype` int(10) DEFAULT NULL,
    `task_id` varchar(32) DEFAULT NULL,
    `src_server_id` varchar(64) DEFAULT NULL,
    `src_db_type` varchar(64) DEFAULT NULL,
    `tar_server_id` varchar(64) DEFAULT NULL,
    `tar_db_type` varchar(64) DEFAULT NULL,
    `params` varchar(1024) DEFAULT NULL,
    `script_id` varchar(64) DEFAULT NULL,
    `start_tm` datetime DEFAULT NULL,
    `end_tm` datetime DEFAULT NULL,
    `status` int(10) DEFAULT NULL,
    `len` int(10) DEFAULT NULL,
    `pos` int(10) DEFAULT NULL,
    `code` int(10) DEFAULT NULL,
    `msg` varchar(1024) DEFAULT NULL,
    PRIMARY KEY (`_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """
    conn = GetWasherDataConn()
    cursor = conn.cursor()

    # 创建表格
    cursor.execute(create_sql)
    conn.commit()

    # 插入任务记录
    sql = "INSERT INTO task_state_log(tasktype, task_id, src_server_id, src_db_type, " \
          "tar_server_id, tar_db_type, params, len, start_tm) value (%d,%s,%s,%s,%s,%s,%s,%d, now())"
    sql = sql % (task_type,taskid, repr(src_server), repr(src_db), repr(tar_server), repr(tar_db), repr(param), len)
    cursor.execute(sql)
    conn.commit()

    # 获取插入的最后一个自增id
    cursor.execute("select last_insert_id() as idx")
    line = cursor.fetchone()
    idx = 0
    if line: idx = line["idx"]
    conn.close()
    return idx

# state 执行状态
# 0 准备
# 1 执行中
# 2 执行成功
# 3 执行失败
def UpdateTaskState(idx, state, code=0, msg=""):
    conn = GetWasherDataConn()
    cursor = conn.cursor()
    sql = "update task_state_log set status = %d, code=%d, msg = %s " % (state, code, repr(msg))
    if state == 2 or state == 3:
        sql += ", end_tm = now() "
    sql += " where _id = %d" % idx
    cursor.execute(sql)
    conn.commit()
    conn.close()

# 更新任务进度
# param idx 任务id
# param pos 任务进度
def UpdateTaskPos(idx, pos):
    conn = GetWasherDataConn()
    cursor = conn.cursor()
    sql = "update task_state_log set pos= %d" % pos
    sql += " where _id = %d" % idx
    cursor.execute(sql)
    conn.commit()
    conn.close()


def Test():
    sql = u"""
            select
        'ssal023', _id as '角色id', name as '角色名', acct as '账号名', battle as '战斗
力', school as '职业', format(point_total/10,0) as '充值金额'
        from t_role
        order by battle desc
        limit 22
    """
    print sql
    conn = GetServerConn("ana", "ana_db")
    cur = conn.cursor()
    cur.execute(sql)

if __name__ == "__main__":
    Test()
