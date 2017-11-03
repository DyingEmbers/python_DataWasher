# coding=utf-8

import sys, redis, time, json, traceback, washer_utils, MySQLdb, datetime, socket

import constant_var
sys.path.append("task")

__CFG_REDIS_IP = "127.0.0.1"
__CFG_REDIS_PORT = "6379"

__G_REDIS_CONN = None
__G_WASHER_ZONE = None   # 清洗脚本所属区域

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
def CreateTableByData(db_id, table_name, data):
    # 获取目标数据库连接
    conn = washer_utils.GetServerConn(db_id)
    if not conn: return

    # 构造sql语句
    sql = "CREATE TABLE IF NOT EXISTS `" + (table_name) + "`(`__t_id` INT UNSIGNED AUTO_INCREMENT,"
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

def AddTableIndex(db_id, table_name, key, val):
    # 获取目标数据库连接
    conn = washer_utils.GetServerConn(db_id)
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
def InsertWashedData(table_name, desc_conn, data):
    if len(data) == 0: return
    desc_cur = desc_conn.cursor()
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
                CreateTableByData("ana_db", table_name, data)
            if e.args[0] == 1054:
                tmp = e.args[1].split("'")
                AddTableIndex("ana_db", table_name, tmp[1], line[tmp[1]])

    print"insert " + str(len(data)) + " row data to " + table_name

# 汇报任务完成情况
def ReportTaskResult(task, msg):
    global __G_REDIS_CONN

    # 插入任务
    result = {"time": task["time"], "task_id": task["task_id"], "task_idx": task["task_idx"],
              "server": task["server"], "result": msg}
    __G_REDIS_CONN.rpush(constant_var.__STATIC_DEAL_LIST, json.dumps(result))


@washer_utils.CPU_STAT
def ProcessTask(json_task):
    task = json.loads(json_task)

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

    # 任务执行前python
    if hasattr(task_obj, "BeforeProcess"):
        getattr(task_obj, "BeforeProcess")(task["server"], task["time"])

    # 执行任务
    func = getattr(task_obj, "Task")
    src_conn = washer_utils.GetServerConn(task["server"])
    if not src_conn:
        ReportTaskResult(task, "TargetConnErr")
        return
    try:
        data, data_date = func(src_conn, task["time"])
    except Exception, e:
        ReportTaskResult(task, "ExecTaskErr")
        return
    src_conn.close()

    if hasattr(task_obj, "CreateTable"):
        getattr(task_obj, "CreateTable")()

    # 对数据进行加工，填充服务器， 时间， 等信息
    for line in data:
        line["server"] = task["server"]
        line["wash_date"] = data_date
        line["wash_time"] = task["time"]

    # 写入数据 TODO@apm30 目标数据库现在是写死的，以后应该改成task_list的一个参数
    desc_conn = washer_utils.GetServerConn("ana_db")
    if not desc_conn:
        ReportTaskResult(task, "DescConnErr")
        return

    desc_cur = desc_conn.cursor()
    # 每次执行的数据唯一， 删除这个服务器该时间点的其他数据
    if task_config["day_one"] == 1:
        try:
            tmp_sql = "delete from " + task_config["save_name"] + " where `wash_time` = '" + task["time"] + "' and `server` = '" + task["server"] + "'"
            row_num = desc_cur.execute(tmp_sql)
            desc_conn.commit()
            print "delete " + str(row_num) + " row data from table " + task_config["save_name"]
        except MySQLdb.Error, e:
            # 表格不存在 无需删除
            if e.args[0] != 1146:
                print "Error %d:%s" % (e.args[0], e.args[1])
                return


    # 准备插入数据
    try:
        # 拼接sql语句
        InsertWashedData(task_config["save_name"], desc_conn, data)
    except Exception, e:
        print e
        # TODO 异常处理：字段不匹配

    if hasattr(task_obj, "AfterProcess"):
        getattr(task_obj, "AfterProcess")(task["server"], task["time"])

    # 发送成功消息，清理环境
    ReportTaskResult(task, "Finish")
    desc_conn.close()

# 获取清洗中心所属的区域
def GetWasherZone():
    ip_address = socket.gethostbyname(socket.gethostname())
    return washer_utils.GetZoneByIP(ip_address)

def main():
    global __G_REDIS_CONN, __G_WASHER_ZONE
    __G_REDIS_CONN = redis.Redis(host=__CFG_REDIS_IP, port=__CFG_REDIS_PORT)
    __G_WASHER_ZONE = GetWasherZone()
    if not __G_WASHER_ZONE:
        return
    print "washer init finish @ zone[%s]" % __G_WASHER_ZONE

    listen_task = __G_WASHER_ZONE + "_" + constant_var.__STATIC_TASK_LIST

    while True:
        time.sleep(0.1)
        task = __G_REDIS_CONN.lpop(listen_task)
        if not task: continue
        try:
            ProcessTask(task)
        except Exception, e:
            ex_str = traceback.format_exc()
            print e.message
            print ex_str

if __name__ == "__main__":
    main()

