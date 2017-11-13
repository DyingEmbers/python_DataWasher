# coding=utf-8

# WasherManager和Washer的共用配置

# 静态变量
__STATIC_TASK_LIST = "task_list"            # 待领取的任务列表
__STATIC_DEAL_LIST = "deal_list"            # 已处理的任务列表 有任务的对应处理结果
__STATIC_TASK_PROCESS = "task_process"      # 任务进度
__STATIC_TIME_FORMAT = "%Y-%m-%d %H:%M"     # 任务进度储存的时间格式


# 任务状态枚举
__STATIC_TASK_PREPARE = 0                   # 准备中
__STATIC_TASK_IN_PROGRESS = 1               # 执行中
__STATIC_TASK_SUCCEED = 2                   # 执行成功
__STATIC_TASK_FAILED = 3                    # 执行失败
