# -*- coding: utf-8 -*-
# coding=utf-8
# unit test
import time
import datetime

import math
import numpy as np
import rest.http_util as http
import rest.message_package_util as message_package
from mongodb.pymongo_util import Database
from decimal import Decimal
from log.logger import Logger

# import random

log = Logger(path="../log", filename="check_water_pressure")

# mongodb配置信息
MONGODB_IP = "47.102.220.27"
MONGODB_PORT = 27017
MONGODB_DB = "ciaowarm"
MONGODB_USERNAME = "ciaowarm"
MONGODB_PASSWORD = "Iwarm905()%"


# 根据时间戳获取日期
def timestamp_to_date(timestamp):
    time_array = time.localtime(timestamp / 1000)
    date = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    return date


def run():
    log.logger.info("自检水压值起始时间------>" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

    # 初始化MongoDB对象
    db = Database(MONGODB_IP, MONGODB_PORT, MONGODB_DB, MONGODB_USERNAME, MONGODB_PASSWORD)
    # 设备分析时间，一般七天为一个周期
    today = datetime.date.today()
    query_time = today - datetime.timedelta(days=7)
    # 查询起始时间
    query_start_time = int(time.mktime(time.strptime(str(query_time), '%Y-%m-%d'))) * 1000
    # 查询结束时间
    query_end_time = query_start_time + 7 * 86400000
    # 获取所有的设备信息
    devices = http.get_all_device()
    # 遍历设备信息
    for device in devices:
        # 设备AI分析
        try:
            data = db.find("g" + str(device['id']),
                           {"$and": [{"timestamp": {'$gte': query_start_time, '$lt': query_end_time}},
                                     {"boilers.water_pressure_value": {"$exists": "true"}}]},
                           {"boilers.water_pressure_value": '1', "timestamp": '1'})
            water_pressure_value_arr = []
            for item in data:
                if 'boilers' in item:
                    boiler = item['boilers'][0]
                    if 'water_pressure_value' in boiler:
                        water_pressure_value_arr.append(boiler['water_pressure_value'])

        except Exception as e:
            log.logger.error("Unexpected Error: {}".format(e))

    log.logger.info("自检水压值结束时间------>" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))


run()
