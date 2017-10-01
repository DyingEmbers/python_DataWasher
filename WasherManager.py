# coding=utf-8

# 模块导入
import socket

# 全局变量定义
__G_LISTENING__PORT = "25000"

# 系统初始化
def Init():
    global __G_LISTENING__PORT
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # 监听指定端口
    s.bind(('127.0.0.1', __G_LISTENING__PORT))
    s.listen(1)
    print('系统初始化完成， 开始监听' + __G_LISTENING__PORT + "端口")


# 网络tick
def NetTick():

    pass


# 任务Tick
def TaskTick():
    pass



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
