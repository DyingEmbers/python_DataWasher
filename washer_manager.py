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

def PushTask(task_list, game, server, time_node, task_id, task_idx):
    global __G_REDIS_CONN

    # 插入任务
    task = {"game": game,"server": server, "time": str(time_node), "task_id": task_id, "task_idx": task_idx}
    __G_REDIS_CONN.rpush(constant_var.__STATIC_TASK_LIST, json.dumps(task))
    task_list.append(task)
    return task

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

# 接收清洗脚本的执行结果
def CheckTaskResult(task_list, task_id, time_node):
    global __G_REDIS_CONN
    while __G_REDIS_CONN.llen(constant_var.__STATIC_DEAL_LIST) != 0:
        task_result = __G_REDIS_CONN.lpop(constant_var.__STATIC_DEAL_LIST)
        task_result = json.loads(task_result)

        # 检查任务是否是当前执行的任务
        if task_result["task_id"] != task_id or ParseDateTime(task_result["time"],"%Y-%m-%d %H:%M:%S") != time_node:
            # TODO@apm30 超时任务需要做好异常处理
            print "ERRO: receive time out task[%d] at time_node[%s]" % (task_result["task_id"], str(time_node))
            continue

        # 任务执行成功，删除task_list中对应的任务
        task_list.pop(task_result["task_idx"])
        
        # 检查任务完成情况
        if task_result["result"] != 0:
            # TODO@apm30 失败任务的处理
            print "ERRO: process task[%d] at time_node[%s] get err[%d] from result" % (task_result["task_id"], str(time_node), task_result["result"])
            continue


# 处理任务
def ProcessTask(game, task_id, time_node):
    global __CFG_TASK_TIME_OUT # 配置
    task_list = []
    # 向redis写任务
    server_list = washer_utils.GetServerList(game)
    task_idx = 0
    for server in server_list:
        PushTask(task_list, game, server["server_id"], time_node, task_id, task_idx)
        task_idx += 1

    # 等待任务执行完毕
    wait_second = 0
    while len(task_list) != 0:
        time.sleep(1)
        wait_second += 1
        CheckTaskResult(task_list, task_id, time_node)

        # 超时
        if wait_second > __CFG_TASK_TIME_OUT: break

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

    # 遍历所有任务，并执行当前时间点的任务
    task_list = washer_utils.GetActiveTask()
    for row in task_list:
        # 检查任务是否需要执行
        if not CheckTask(row["exec_tm"], __G_TASK_PROCESS): continue
        
        ProcessTask(row["game"], row["task_id"], __G_TASK_PROCESS)

    # 更新任务进度
    SetTaskProcessTime(__G_TASK_PROCESS)

def main():
    global __G_EXIT_FLAG
    # 系统初始化
    Init()

    # 主循环
    while True:
        try:
            TaskTick()
        except Exception, e:
            ex_str = traceback.format_exc()
            print e.message
            print ex_str
        if __G_EXIT_FLAG:
            print "WasherManager Exit!"
            break

if __name__ == "__main__":
    main()
