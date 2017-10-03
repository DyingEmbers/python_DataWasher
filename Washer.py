# coding=utf-8
import sys, redis, time, json
sys.path.append("task")

__CFG_REDIS_IP = "127.0.0.1"
__CFG_REDIS_PORT = "6379"
__G_REDIS_CONN = None

def GetTaskConfig(task_id):
    return {'py_name':'task1'}
    pass

def ProcessTask(json_task):
    task = json.loads(json_task)

    # 读取任务配置
    task_config = GetTaskConfig(1)

    task_obj = __import__(task_config["py_name"])
    if not hasattr(task_obj, "Task"):
        print task_config["py_name"] + " do not has Function Task"

    func = getattr(task_obj, "Task")

    #执行任务
    data = func(1,"sss")

    # 写入数据
    

def main():
    global __G_REDIS_CONN
    __G_REDIS_CONN = redis.Redis(host=__CFG_REDIS_IP, port=__CFG_REDIS_PORT)

    while True:
        time.sleep(1)
        task = __G_REDIS_CONN.lpop("task")
        if not task:continue
        try:
            ProcessTask(task)
        except Exception,e:
            print e.message

if __name__ == "__main__":
    main()

