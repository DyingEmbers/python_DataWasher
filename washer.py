# coding=utf-8

import sys, redis, time, json, traceback, washer_utils, MySQLdb, datetime, socket, threading

import constant_var
import wash_config

sys.path.append("task")


__G_REDIS_CONN = None
__G_WASHER_ZONE = None   # 清洗脚本所属区域
__G_WASHER_ID = None     # 清洗脚本id 动态获取
__G_EXEC_TASK = ""       # 当前正在执行的任务

# 根据传入参数的类型返回类型字符串
def GetDataType(data):
    if isinstance(data, int):
        return "int "
    if isinstance(data, long):
        return "bigint"
    if isinstance(data, basestring):
        return "varchar(250)"
    if isinstance(data, float):
        return "double"
    if isinstance(data, datetime.datetime):
        return "datetime"
    return "NULL"


# 根据数据创建表格
# param db_id 目标数据库id
# param table_name 表名
# param data 数据
def CreateTableByData(db_id, db_type, table_name, data):
    # 获取目标数据库连接
    conn = washer_utils.GetServerConn(db_id, db_type)
    if not conn: return

    # 构造sql语句
    sql = "CREATE TABLE IF NOT EXISTS `" + table_name + "`(`__t_id` INT UNSIGNED AUTO_INCREMENT,"
    line = data[0]
    for key in line:
        sql += "`" + key + "`"
        # 解析类型
        sql += GetDataType(line[key]) + ", "

    sql += "PRIMARY KEY (`__t_id`), KEY date_idx(wash_date))"

    # 创建表格
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()

def AddTableIndex(db_id, db_type, table_name, key, val):
    # 获取目标数据库连接
    conn = washer_utils.GetServerConn(db_id, db_type)
    if not conn: return

    # 构造sql语句
    # ALTER TABLE table_name ADD field_name field_type;
    sql = "ALTER TABLE `" + table_name + "` ADD `" + key + "`"
    sql += " " + GetDataType(val)

    # 创建表格
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()

# 将清洗过后的数据插入数据
# param table_name 表格名称
# param desc_conn 目标数据库连接
# param data 需要插入的数据
def InsertWashedData(table_name, desc_conn, data, clear_sql):
    desc_cur = desc_conn.cursor()
    try:
        row_num = desc_cur.execute(clear_sql)

        print "delete " + str(row_num) + " row data from table " + table_name
    except MySQLdb.Error, e:
        # 表格不存在 无需删除
        if e.args[0] != 1146:
            print "Error %d:%s" % (e.args[0], e.args[1])
            return
        print e

    if len(data) != 0:
        sql = "INSERT INTO "
        sql += table_name

        data_key = "("
        data_val = "("

        is_first = True
        line = data[0]
        for key in line:
            if not is_first:
                data_key += ", "
                data_val += ", "
            data_key += "`" + key + "`"
            data_val += "%(" + key + ")s"
            is_first = False
        data_key += ")"
        data_val += ")"
        sql += data_key + " VALUES" + data_val

        while True:
            try:
                desc_cur.executemany(sql, data)
                desc_conn.commit()
                break
            except MySQLdb.Error, e:
                print "Error %d:%s" % (e.args[0], e.args[1])
                if e.args[0] == 1146:  # 表格不存在，创建默认表
                    print "Try to create table " + table_name
                    CreateTableByData("ana", "ana_db", table_name, data)
                if e.args[0] == 1054:
                    tmp = e.args[1].split("'")
                    AddTableIndex("ana", "ana_db", table_name, tmp[1], line[tmp[1]])
                break  # 无法处理

        print"insert " + str(len(data)) + " row data to " + table_name

    desc_conn.commit()
    desc_cur.close()
    desc_conn.close()

# 汇报任务完成情况
def ReportTaskResult(task, msg):
    global __G_REDIS_CONN, __G_EXEC_TASK

    # 插入任务
    result = {"time": task["time"], "task_id": task["task_id"], "task_idx": task["task_idx"],
              "server": task["server"], "result": msg}
    __G_REDIS_CONN.rpush(constant_var.__STATIC_DEAL_LIST, json.dumps(result))

    state = constant_var.__STATIC_TASK_SUCCEED  # 默认成功
    code = 0
    if msg != "Finish":
        state = constant_var.__STATIC_TASK_FAILED  # 失败
        code = 1
    washer_utils.UpdateTaskState(__G_EXEC_TASK, state, code, msg)


@washer_utils.CPU_STAT
def ProcessTask(json_task):
    global __G_EXEC_TASK
    task = json.loads(json_task)
    __G_EXEC_TASK = task["task_idx"]
    # 将任务标记为执行中
    washer_utils.UpdateTaskState(__G_EXEC_TASK, constant_var.__STATIC_TASK_IN_PROGRESS)

    # 读取任务配置
    task_config = washer_utils.GetTaskConfig(task["task_id"])
    if not task_config:
        ReportTaskResult(task, "TaskNoFound")
        return

    task_obj = __import__(task_config["py_name"])
    if not hasattr(task_obj, "Task"):
        print task_config["py_name"] + " do not has Function Task"
        ReportTaskResult(task, "NoTaskFunc")
        return

    # 执行任务
    func = getattr(task_obj, "Task")
    try:
        data, data_date = func(task["server"], task["db_type"], task["time"])
    except Exception, e:
        ReportTaskResult(task, "ExecTaskErr")
        print e
        return

    if hasattr(task_obj, "CreateTable"):
        getattr(task_obj, "CreateTable")()

    # 对数据进行加工，填充服务器， 时间， 等信息
    for line in data:
        line["server"] = task["server"]
        line["wash_date"] = data_date
        line["wash_time"] = task["time"]

    # 写入数据 TODO@apm30 目标数据库现在是写死的，以后应该改成task_list的一个参数
    desc_conn = washer_utils.GetServerConn("ana", "ana_db")
    if not desc_conn:
        ReportTaskResult(task, "DescConnErr")
        return

    desc_cur = desc_conn.cursor()
    # 每次执行的数据唯一， 删除这个服务器该时间点的其他数据
    tmp_sql = "delete from " + task_config["save_name"] + " where %s and `server` = '" + task["server"] + "'"

    if task_config["day_one"] == 1:
        tmp_sql = tmp_sql % ("`wash_date` = date('" + task["time"] + "')")
    else:
        tmp_sql = tmp_sql % ("`wash_time` = '" + task["time"] + "'")

    # 准备插入数据
    InsertWashedData(task_config["save_name"], desc_conn, data, tmp_sql)

    if hasattr(task_obj, "AfterProcess"):
        getattr(task_obj, "AfterProcess")(task["server"], task["time"])

    # 发送成功消息，清理环境
    ReportTaskResult(task, "Finish")
    desc_conn.close()

# 获取清洗中心所属的区域
def GetWasherZone():
    ip_address = socket.gethostbyname(socket.gethostname())
    print "washer setup @ " + ip_address
    return washer_utils.GetZoneByIP(ip_address)

def WasherHeartbeat():
    global __G_REDIS_CONN, __G_WASHER_ID, __G_EXEC_TASK
    __G_REDIS_CONN.setex("washer_" + __G_WASHER_ID, "exec task:" + str(__G_EXEC_TASK), 10)  # 10秒过期

    t = threading.Timer(5, WasherHeartbeat)  # 5秒心跳
    t.start()

def WasherInit():
    global __G_REDIS_CONN, __G_WASHER_ZONE, __G_WASHER_ID
    # 初始化redis连接
    __G_REDIS_CONN = redis.Redis(host=wash_config.__CFG_REDIS_IP, port=wash_config.__CFG_REDIS_PORT)
    if not __G_REDIS_CONN: return False

    # 获取id号
    pipe = __G_REDIS_CONN.pipeline(transaction=True)
    __G_REDIS_CONN.incr("washer_id")
    __G_WASHER_ID = __G_REDIS_CONN.get("washer_id")
    pipe.execute()

    # 获取所在区域
    __G_WASHER_ZONE = GetWasherZone()
    if not __G_WASHER_ZONE:
        return False

    print "washer init finish @ zone[%s]" % __G_WASHER_ZONE

    # 启动心跳
    WasherHeartbeat()
    return True

def main():
    global __G_REDIS_CONN, __G_WASHER_ZONE, __G_EXEC_TASK
    if not WasherInit(): return

    listen_task = __G_WASHER_ZONE + "_" + constant_var.__STATIC_TASK_LIST

    while True:
        time.sleep(0.1)
        task = __G_REDIS_CONN.lpop(listen_task)
        if not task: continue
        try:
            ProcessTask(task)
            __G_EXEC_TASK = 0
        except Exception, e:
            ex_str = traceback.format_exc()
            print e.message
            print ex_str

if __name__ == "__main__":
    main()

