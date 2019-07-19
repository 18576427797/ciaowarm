# -*- coding: utf-8 -*-
"""
 %(levelno)s:         打印日志级别的数值
 %(levelname)s:    打印日志级别名称
 %(pathname)s:    打印当前执行程序的路径，其实就是sys.argv[0]
 %(filename)s:      打印当前执行程序名
 %(funcName)s:    打印日志的当前函数
 %(lineno)d:         打印日志的当前行号
 %(asctime)s:      打印日志的时间
 %(thread)d:        打印线程ID
 %(threadName)s: 打印线程名称
 %(process)d:      打印进程ID
 %(message)s:    打印日志信息

%y 两位数的年份表示（00-99）
%Y 四位数的年份表示（000-9999）
%m 月份（01-12）
%d 月内中的一天（0-31）
%H 24小时制小时数（0-23）
%I 12小时制小时数（01-12）
%M 分钟数（00=59）
%S 秒（00-59）
%a 本地简化星期名称
%A 本地完整星期名称
%b 本地简化的月份名称
%B 本地完整的月份名称
%c 本地相应的日期表示和时间表示
%j 年内的一天（001-366）
%p 本地A.M.或P.M.的等价符
%U 一年中的星期数（00-53）星期天为星期的开始
%w 星期（0-6），星期天为星期的开始
%W 一年中的星期数（00-53）星期一为星期的开始
%x 本地相应的日期表示
%X 本地相应的时间表示
%Z 当前时区的名称
%% %号本身


"""
import logging
import datetime


def getLog(path='.', file_name=''):
    dt = datetime.datetime.now()
    dtstr = dt.strftime('%Y-%m-%d')
    # dtstr=dt.strftime('%Y-%m-%d_%H_%M_%S')
    # 创建一个logger实例
    logger = logging.getLogger(file_name)
    logging.basicConfig(filemode='wa')
    logging.propagate = 0
    hds = logger.handlers
    for h in hds:
        logger.removeHandler(h)
    logger.setLevel(logging.INFO)
    # 创建一个handler,用于写入日志文件，handler可以把日志内容写到不同的地方
    logName = "%s/%s.test.log" % (path, dtstr)
    fh = logging.FileHandler(logName, encoding='utf-8')
    fh.setLevel(logging.INFO)
    log_format = logging.Formatter(fmt="[%(asctime)s.%(msecs)03d-%(filename)s:%(lineno)d]  %(message)s",
                                   datefmt='%Y-%m-%d,%H:%M:%S')
    fh.setFormatter(log_format)  # setFormatter() selects a Formatter object for this handler to use
    # 再创建一个handler，用于输出控制台
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(log_format)
    logger.addHandler(ch)

    logger.addHandler(fh)

    return logger


if __name__ == '__main__':
    log = getLog()
    a = [45, 66, 88, 345, 88, 1234]
    log.info(a)
    log.error('test1')
    log.info('test2')
    log.info('test3')
