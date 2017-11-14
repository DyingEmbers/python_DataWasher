# coding=utf-8

# 模块导入
import redis, time, datetime, json, washer_utils, signal, traceback

# 自定义模块
import constant_var

# 配置
__CFG_REDIS_IP = "127.0.0.1"                # redis连接地址
__CFG_REDIS_PORT = "6379"                   # redis端口
__CFG_TASK_TIME_OUT = 300                   # 任务超时时间

# 全局变量
__G_REDIS_CONN = None                       # redis 连接
__G_TASK_PROCESS = None                     # 任务进度
__G_EXIT_FLAG = False                       # 退出标记位

def ExitHandler(signum, frame):
    global __G_EXIT_FLAG
    __G_EXIT_FLAG = True
    print('WasherManager prepare to exit')

signal.signal(signal.SIGINT, ExitHandler)
signal.signal(signal.SIGTERM, ExitHandler)

# 讲字符串时间转换成datetime
def ParseDateTime(input_str, tm_format="%Y-%m-%d %H:%M"):
    return datetime.datetime.fromtimestamp(time.mktime(time.strptime(input_str, tm_format)))


# 获取当前任务执行的进度
def GetTaskProcessTime():
    global __G_REDIS_CONN
    task_process = __G_REDIS_CONN.get(constant_var.__STATIC_TASK_PROCESS)
    if task_process is None:
        task_process = datetime.datetime.now()
        task_process -= datetime.timedelta(minutes=1)
        task_process = task_process.strftime(constant_var.__STATIC_TIME_FORMAT)
    return ParseDateTime(task_process, constant_var.__STATIC_TIME_FORMAT)


# 更新任务进度
def SetTaskProcessTime(tm):
    global __G_REDIS_CONN
    __G_REDIS_CONN.set(constant_var.__STATIC_TASK_PROCESS, tm.strftime(constant_var.__STATIC_TIME_FORMAT))

# 系统初始化
def Init():
    global __G_REDIS_CONN, __G_TASK_PROCESS
    __G_REDIS_CONN = redis.Redis(host=__CFG_REDIS_IP, port=__CFG_REDIS_PORT)
    __G_TASK_PROCESS = GetTaskProcessTime()

    # TODO@apm30 异常数据清理工作

def PushTask(task_list, db_type, server, time_node, task_id):
    global __G_REDIS_CONN

    # 插入任务
    task = {"db_type": db_type, "server": server, "time": str(time_node), "task_id": task_id}
    task_json = json.dumps(task)
    task_idx = washer_utils.InsertTaskData(constant_var.__STATIC_REGULAR_TASK,
                                           task_id, server, db_type, "ana", "ana_db", task_json)

    task["task_idx"] = task_idx
    # 将任务推送到redis队列
    zone = washer_utils.GetZoneByServerID(server)
    if not zone: return
    __G_REDIS_CONN.rpush(zone + "_" + constant_var.__STATIC_TASK_LIST, json.dumps(task))
    task_list.append(task)


def _CheckTimeNode(time_node, target_time):
    if time_node == "*":
        return True

    return int(time_node) == target_time

# 检查时间是否符合条件
def CheckTask(tm_rule, target_time):
    tm_vec = tm_rule.split(" ")
    if len(tm_vec) != 4:
        print "ERRO: tm_rule " + tm_rule + " erro\n"
        return False # 数组长度错误

    if not _CheckTimeNode(tm_vec[0], target_time.hour): return False
    if not _CheckTimeNode(tm_vec[1], target_time.minute): return False
    if not _CheckTimeNode(tm_vec[2], target_time.month): return False
    if not _CheckTimeNode(tm_vec[3], target_time.year): return False

    return True

# 记录执行错误的任务
def SaveFailedTask(task_id, server, wash_time, err_msg):
    data_conn = washer_utils.GetWasherDataConn()
    sql = "INSERT INTO err_task(task_id, server, wash_time, err_msg)VALUE(%s,%s,%s,%s)"
    data_cursor = data_conn.cursor()
    data_cursor.execute(sql, [task_id, server, str(wash_time), err_msg])
    data_conn.commit()
    data_cursor.close()
    data_conn.close()

# 接收清洗脚本的执行结果
def CheckTaskResult(task_list, task_id, time_node):
    global __G_REDIS_CONN
    while __G_REDIS_CONN.llen(constant_var.__STATIC_DEAL_LIST) != 0:
        result_json = __G_REDIS_CONN.lpop(constant_var.__STATIC_DEAL_LIST)
        task_result = json.loads(result_json)

        # 检查任务是否是当前执行的任务
        if task_result["task_id"] != task_id or ParseDateTime(task_result["time"],"%Y-%m-%d %H:%M:%S") != time_node:
            # TODO@apm30 超时任务需要做好异常处理
            print "ERRO: receive time out task[%d] at time_node[%s]" % (task_result["task_id"], str(time_node))
            SaveFailedTask(task_id, task_result["server"], time_node, "TimeOut")
            continue

        # 收到任务执行结果，删除task_list中对应的任务
        found = False
        for item in task_list:
            if item["task_idx"] != task_result["task_idx"]: continue
            task_list.remove(item)
            found = True
            break

        if not found:
            # TODO@apm30 收到任务执行结果，但内存中没有对应任务， 可能任务重复执行
            print "ERRO: process task[%d] at time_node[%s] task json[%s]" % (task_result["task_id"], str(time_node), result_json)
            SaveFailedTask(task_id, task_result["server"], time_node, "DuplicateTask")
            continue

        # 检查任务完成情况
        if task_result["result"] != "Finish":
            print "ERRO: process task[%d] at time_node[%s] get err[%s] from result" % (task_result["task_id"], str(time_node), task_result["result"])
            # 记录执行失败的问题
            SaveFailedTask(task_id, task_result["server"], time_node, task_result["result"])
            continue


# 处理定时任务
def ProcessTask(task_id, time_node):
    global __CFG_TASK_TIME_OUT  # 配置
    task_cfg = washer_utils.GetTaskConfig(task_id)
    db_type = task_cfg["db_type"]
    task_list = []
    # 向redis写任务
    server_list = washer_utils.GetServerList(db_type)
    for server in server_list:
        PushTask(task_list, db_type, server["server_id"], time_node, task_id)

    # 等待任务执行完毕
    task_begin = datetime.datetime.now()
    while len(task_list) != 0:
        time.sleep(0.1)
        try:
            CheckTaskResult(task_list, task_id, time_node)
        except Exception,e:
            ex_str = traceback.format_exc()
            print "Check task result err \n err stack is :\n" + ex_str
        # 超时
        if task_begin + datetime.timedelta(seconds=__CFG_TASK_TIME_OUT) <= datetime.datetime.now(): break

# 任务Tick
@washer_utils.CPU_STAT
def TaskTick():
    global __G_REDIS_CONN, __G_TASK_PROCESS
    # 检查当前时间点的有哪些任务
    now = datetime.datetime.now()
    now_min = now.strftime(constant_var.__STATIC_TIME_FORMAT)

    if __G_TASK_PROCESS > now:
        print "ERRO: task process[" + str(__G_TASK_PROCESS) + "] > now[" + str(now) + "]"
        return

    if __G_TASK_PROCESS.strftime(constant_var.__STATIC_TIME_FORMAT) == now_min:
        # 进度大于等于当前时间， sleep
        sleep_time = 60 - (now - __G_TASK_PROCESS).seconds
        print "Washer sleep %d  second, task_process[%s] now[%s]" % (sleep_time, str(__G_TASK_PROCESS), str(now))
        time.sleep(sleep_time)
        return

    # 时间比现实时间慢， 尝试执行下一分钟的任务
    __G_TASK_PROCESS += datetime.timedelta(minutes=1)
    print "Begin washer data @ " + str(__G_TASK_PROCESS)

    # 遍历所有任务，并执行当前时间点的任务
    task_list = washer_utils.GetActiveTask()
    for row in task_list:
        # 检查任务是否需要执行
        if not CheckTask(row["exec_tm"], __G_TASK_PROCESS): continue
        
        ProcessTask(row["task_id"], __G_TASK_PROCESS)

    # 更新任务进度
    SetTaskProcessTime(__G_TASK_PROCESS)
    print "End washer data @ " + str(__G_TASK_PROCESS)

# 执行额外任务
def ProcessExecTask(task_date):
    pass
    # 二外执行的脚本由另一个脚本执行
    # 解析任务参数
    # task_cfg = washer_utils.GetTaskConfig(task_id)
    # if not task_cfg:
    #     print "can not found task[%d]" % task_id
    #     return
    #
    # # 去除秒数
    # process_time = begin_time - datetime.timedelta(seconds=begin_time.second)
    # while process_time <= end_time:
    #     # 检查当前时间点是否需要执行
    #     if not CheckTask(task_cfg["exec_tm"], process_time): continue
    #     ProcessTask(task_id, process_time)
    #     print "Process exec task[%d] at time_node[%s]" % (task_id, str(process_time))
    #     process_time += datetime.timedelta(minutes=1)

# 额外任务Tick
def ExecTick():
    # 检查是否有额外任务
    conn = washer_utils.GetServerConn("wash_mgr", "wash_db")
    sql = "SELECT * from tt_exec"
    cursor = conn.cursor()
    cursor.execute(sql)
    exec_task = cursor.fetchall()

    # 执行额外任务
    for line in exec_task:
        ProcessExecTask(line)
        # 清理现场
        cursor.execute("delete from tt_exec where _id = " + str(line["_id"]))

    cursor.close()
    conn.close()

def main():
    global __G_EXIT_FLAG
    # 系统初始化
    Init()

    # 主循环
    while True:
        try:
            TaskTick()
            # ExecTick()
        except Exception, e:
            ex_str = traceback.format_exc()
            print e.message
            print ex_str
        if __G_EXIT_FLAG:
            print "WasherManager Exit!"
            break

if __name__ == "__main__":
    main()
