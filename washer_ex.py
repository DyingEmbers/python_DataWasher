# coding=utf-8

import sys, redis, time, json, traceback, washer_utils, socket

import constant_var
sys.path.append("task")

__CFG_REDIS_IP = "127.0.0.1"
__CFG_REDIS_PORT = "6379"

__G_REDIS_CONN = None
__G_WASHER_ZONE = None   # 清洗脚本所属区域
__G_WASHER_ID = None     # 清洗脚本id 动态获取


# 汇报任务完成情况
def ReportAdditionResult(task_idx, msg):
    global __G_REDIS_CONN

    state = constant_var.__STATIC_TASK_SUCCEED  # 默认成功
    code = 0
    if msg != "Finish":
        state = constant_var.__STATIC_TASK_FAILED  # 失败
        code = 1
    washer_utils.UpdateTaskState(task_idx, state, code, msg)


@washer_utils.CPU_STAT
def ProcessTask(json_task):
    task = json.loads(json_task)
    task_name = task["task_name"]
    # 创建任务记录
    task_idx = washer_utils.InsertTaskData(constant_var.__STATIC_ADDITIONAL_TASK,
                                           0, "NULL", "NULL", "NULL", "NULL", json_task)
    # 标记为执行中
    washer_utils.UpdateTaskState(task_idx, constant_var.__STATIC_TASK_IN_PROGRESS)

    task_obj = __import__(task_name)
    if not hasattr(task_obj, "Task"):
        print task_name + " do not has Function Task"
        ReportAdditionResult(task, "NoTaskFunc")
        return

    # 执行任务
    func = getattr(task_obj, "Task")

    try:
        func(task)
    except Exception, e:
        ReportAdditionResult(task_idx, "ExecTaskErr")
        return

    # 发送成功消息，清理环境
    ReportAdditionResult(task_idx, "Finish")

# 获取清洗中心所属的区域
def GetWasherZone():
    ip_address = socket.gethostbyname(socket.gethostname())
    print "washer setup @ " + ip_address
    return washer_utils.GetZoneByIP(ip_address)


def WasherInit():
    global __G_REDIS_CONN, __G_WASHER_ZONE, __G_WASHER_ID
    # 初始化redis连接
    __G_REDIS_CONN = redis.Redis(host=__CFG_REDIS_IP, port=__CFG_REDIS_PORT)
    if not __G_REDIS_CONN: return False

    # 获取所在区域
    __G_WASHER_ZONE = GetWasherZone()
    if not __G_WASHER_ZONE:
        return False

    print "washer init finish @ zone[%s]" % __G_WASHER_ZONE
    return True

def main():
    global __G_REDIS_CONN, __G_WASHER_ZONE
    if not WasherInit(): return

    listen_task = "additional_list"

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

