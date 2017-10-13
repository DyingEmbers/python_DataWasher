# coding=utf-8

# 模块导入
import redis, time, datetime, json, WasherUtils, signal

# 配置
__CFG_REDIS_IP = "127.0.0.1"
__CFG_REDIS_PORT = "6379"

# 全局变量
__G_REDIS_CONN = None           # redis 连接
__G_TASK_PROCESS = None         # 任务进度
__G_EXIT_FLAG = False           # 退出标记位

# 静态变量
__STATIC_TASK_LIST = "task_list"    # 待领取的任务列表
__STATIC_DEAL_LIST = "deal_list"    # 已处理的任务列表 有任务的对应处理结果
__STATIC_TASK_PROCESS = "task_process"
__STATIC_TIME_FORMAT = "%Y-%m-%d %H:%M"
# 需要提取的服务器配置
# 包含字段：game:所属游戏 server_id:服务器id ip:数据库地址 port:端口 user:用户名 password:密码
__STATIC_SERVER_LIST = "server_list"

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
    global __G_REDIS_CONN, __STATIC_TASK_PROCESS, __STATIC_TIME_FORMAT
    task_process = __G_REDIS_CONN.get(__STATIC_TASK_PROCESS)
    if task_process is None:
        task_process = datetime.datetime.now().strftime(__STATIC_TIME_FORMAT)
    return ParseDateTime(task_process, __STATIC_TIME_FORMAT)


# 更新任务进度
def SetTaskProcessTime(tm):
    global __G_REDIS_CONN, __STATIC_TASK_PROCESS
    __G_REDIS_CONN.set(__STATIC_TASK_PROCESS, tm.strftime(__STATIC_TIME_FORMAT))

# 系统初始化
def Init():
    global __G_REDIS_CONN, __G_TASK_PROCESS
    __G_REDIS_CONN = redis.Redis(host=__CFG_REDIS_IP, port=__CFG_REDIS_PORT)
    __G_TASK_PROCESS = GetTaskProcessTime()

    # TODO 异常数据清理工作

def PushTask(game, server, time_node, task_id):
    global __G_REDIS_CONN, __STATIC_TASK_LIST

    # 插入任务
    task = {"game": game,"server": server, "time": str(time_node), "task_id": task_id}
    __G_REDIS_CONN.rpush(__STATIC_TASK_LIST, json.dumps(task))
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

    return  True


# 任务Tick
@WasherUtils.CPU_STAT
def TaskTick():
    global __G_REDIS_CONN, __STATIC_TASK_LIST, __G_TASK_PROCESS, __STATIC_TIME_FORMAT, __STATIC_DEAL_LIST
    # 检查当前时间点的有哪些任务
    now = datetime.datetime.now()
    now_min = now.strftime(__STATIC_TIME_FORMAT)

    if __G_TASK_PROCESS > now:
        print "ERRO: task process[" + str(__G_TASK_PROCESS) + "] > now[" + str(now) + "]"
        return

    if __G_TASK_PROCESS.strftime(__STATIC_TIME_FORMAT) == now_min:
        # 进度大于等于当前时间， sleep
        sleep_time = 60 - (now - __G_TASK_PROCESS).seconds
        print "Washer sleep " + str(sleep_time) + " second, task_process[" + str(__G_TASK_PROCESS) + "] now[" + str(now)+"]"
        time.sleep(sleep_time)
        return

    # 时间比现实时间慢， 尝试执行下一分钟的任务
    __G_TASK_PROCESS += datetime.timedelta(minutes=1)

    # 遍历所有任务，并执行当前时间点的任务
    washer_conn = WasherUtils.GetWasherCfgConn()
    cur = washer_conn.cursor()
    cur.execute("select task_id, game, py_name, exec_tm, last_tm from task_list where active = 1")

    task_list = cur.fetchall()
    for row in task_list:
        # 检查任务是否需要执行
        if not CheckTask(row[3], __G_TASK_PROCESS):continue

        task_list = []
        # 向redis写任务
        cur.execute("select server_id from server_list where game = '" + row[1] + "'")
        server_list = cur.fetchall()
        for server in server_list:
            task_list.append(PushTask(row[1], server[0], __G_TASK_PROCESS, row[0]))

        # 等待任务执行完毕
        wait_second = 0
        while __G_REDIS_CONN.llen(__STATIC_DEAL_LIST) != len(task_list):
            time.sleep(1)
            wait_second += 1
            if wait_second > 300:


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
            print e
        if __G_EXIT_FLAG:
            print "WasherManager Exit!"
            break

if __name__ == "__main__":
    main()
