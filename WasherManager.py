# coding=utf-8

# 模块导入
import redis, time

# 全局变量定义
__CFG_REDIS_IP = "127.0.0.1"
__CFG_REDIS_PORT = "6379"

__G_REDIS_CONN = None
__G_TEST_IDX = 0

# 系统初始化
def Init():
    global __G_REDIS_CONN
    __G_REDIS_CONN = redis.Redis(host=__CFG_REDIS_IP, port=__CFG_REDIS_PORT)

    pass

# 网络tick
def NetTick():
    time.sleep(0.5)
    pass


# 任务Tick
def TaskTick():
    global __G_REDIS_CONN
    # 检查当前时间点的油哪些任务

    # 向redis写任务



def main():
    # 系统初始化
    Init()

    # 主循环
    try:
        while True:
            NetTick()
            TaskTick()

    except Exception,e:
        print e


if __name__ == "__main__":
    main()
