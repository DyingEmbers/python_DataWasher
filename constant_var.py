# coding=utf-8

# WasherManager和Washer的共用配置

# 静态变量
__STATIC_TASK_LIST = "task_list"            # 待领取的任务列表
__STATIC_DEAL_LIST = "deal_list"            # 已处理的任务列表 有任务的对应处理结果
__STATIC_TASK_PROCESS = "task_process"      # 任务进度
__STATIC_TIME_FORMAT = "%Y-%m-%d %H:%M"     # 任务进度储存的时间格式
# 需要提取的服务器配置
# 包含字段：game:所属游戏 server_id:服务器id ip:数据库地址 port:端口 user:用户名 password:密码
__STATIC_SERVER_LIST = "server_list"
