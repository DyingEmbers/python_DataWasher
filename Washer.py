# coding=utf-8
import sys, redis, time, json, traceback, WasherUtils
sys.path.append("task")

__CFG_REDIS_IP = "127.0.0.1"
__CFG_REDIS_PORT = "6379"

__G_REDIS_CONN = None

# 静态变量
__STATIC_TASK_LIST = "task_list"
__STATIC_TIME_FORMAT = "%Y-%m-%d %H:%M"


def GetTaskConfig(task_id):
    conn = WasherUtils.GetWasherCfgConn()
    cur = conn.cursor()
    cur.execute("select py_name, save_name, day_one, unique_key from task_list where task_id = '" + str(task_id) + "'")
    task = cur.fetchone()
    if task == None: return None
    return {'py_name':task[0], "save_name":task[1], "day_one":task[2], "unique_key":task[3]}

def DeleteOldData():
    pass

# 将清洗过后的数据插入数据
# param table_name 表格名称
# param desc_conn 目标数据库连接
# param data 需要插入的数据
def InsertWashedData(table_name, desc_conn, data):
    if len(data) == 0:return
    desc_cur = desc_conn.cursor()
    is_first = True
    sql = "INSERT INTO "
    sql += table_name
    sql += " VALUES ("
    line = data[0]
    is_first = True
    for key in line:
        if not is_first: sql += ", "
        sql += "%s"
        is_first = False
    sql += ")"
    desc_cur.executemany(sql, data)
    desc_conn.commit()

    print"insert " + str(len(data)) + " row data to " + table_name

@WasherUtils.CPU_STAT
def ProcessTask(json_task):
    task = json.loads(json_task)

    # 读取任务配置
    task_config = GetTaskConfig(task["task_id"])
    if task_config == None: return True

    task_obj = __import__(task_config["py_name"])
    if not hasattr(task_obj, "Task"):
        print task_config["py_name"] + " do not has Function Task"

    func = getattr(task_obj, "Task")

    #执行任务
    src_conn = WasherUtils.GetServerConn(task["game"], task["server"])
    data, data_date = func(src_conn,task["time"])
    # 对数据进行加工，填充服务器， 时间， 等信息
    save_data = []
    for line in data:
        tmp = [None]
        tmp.extend(line)
        save_data.append(tmp)

    for line in save_data:
        line.append(task["server"])
        line.append(data_date)
        line.append(task["time"])


    # 写入数据 TODO 目标数据库现在是写死的，以后应该改成task_list的一个参数
    desc_conn = WasherUtils.GetServerConn("ana", "ana_db")
    desc_cur = desc_conn.cursor()
    # 每天的数据唯一， 删除这个服务器今天的其他数据
    if task_config["day_one"] == 1:
        row_num = desc_cur.execute("delete from " + task_config["save_name"] + " where `wash_date` = '" + data_date + "' and `server` = '" + task["server"] + "'")
        desc_conn.commit()
        print "delete " + str(row_num) + " row data from table " + task_config["save_name"]

    # 准备插入数据
    try:
        # 拼接sql语句
        InsertWashedData(task_config["save_name"], desc_conn, save_data)
    except Exception,e:
        print e
        # TODO 异常处理：表格不存在

        # TODO 异常处理：字段不匹配

def main():
    global __G_REDIS_CONN, __STATIC_TASK_LIST
    __G_REDIS_CONN = redis.Redis(host=__CFG_REDIS_IP, port=__CFG_REDIS_PORT)

    while True:
        time.sleep(1)
        task = __G_REDIS_CONN.lpop(__STATIC_TASK_LIST)
        if not task:continue
        try:
            ProcessTask(task)
        except Exception,e:
            exstr = traceback.format_exc()
            print e.message
            print exstr

if __name__ == "__main__":
    main()

