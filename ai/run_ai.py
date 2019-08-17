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

log = Logger(path="../log", filename="run_ai")
# 会重复打印日志
log.propagate = 0

# mongodb配置信息
MONGODB_IP = "47.102.220.27"
MONGODB_PORT = 27017
MONGODB_DB = "ciaowarm"
MONGODB_USERNAME = "ciaowarm"
MONGODB_PASSWORD = "Iwarm905()%"

# 目标温度不变时长，暂定超过5小时
TRG_TEMP_NOT_CHANGE_TIME = 5
# 室内温度恒定时长，暂定超过3小时，即10800000毫秒，4小时，即14400000毫秒
ROOM_TEMP_CONSTANT_TIME = 10800000
# 遍历室内温度，暂定步进时长1小时，即3600000毫秒
ROOM_TEMP_STEP_TIME = 3600000
# 统计室温离散性时，将室温时长区间切分成2分钟进行分析
ROOM_TEMP_SEGMENT_TIME = 120000
# 室内温度标准差，暂定小于9具有分析价值
ROOM_TEMP_STANDARD_DEVIATION = 9
# 室内温度标准差，暂定小于6.5恒温效果最佳
ROOM_TEMP_STANDARD_OPTIMUM = 6.5
# 室内温度平均数波动范围，暂定0.6度以内，不包含0.6度
ROOM_TEMP_AVG_RANGE = 6
# CHR调节幅度，暂定0.1
CHR_ADJUST_RANGE = 0.1
# 某些参数在超过三分钟的时间处于非正常燃烧状态，我们就认为这段恒温时段没有分析价值
ABNORMAL_BURN_STATUS_TIME = 180000


# 根据时间戳获取日期
def timestamp_to_date(timestamp):
    time_array = time.localtime(timestamp / 1000)
    date = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    return date


def run():
    log.logger.info("起始时间------>" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    # 初始化MongoDB对象
    db = Database(MONGODB_IP, MONGODB_PORT, MONGODB_DB, MONGODB_USERNAME, MONGODB_PASSWORD)
    # 设备分析时间，一般一天为一个周期
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_start = int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d'))) * 1000
    yesterday_end = yesterday_start + 86400000
    # 获取所有的设备信息
    devices = http.get_all_device()
    # 遍历设备信息
    for device in devices:
        # 设备AI分析
        try:
            device_analysis(db, device, yesterday_start, yesterday_end)
        except Exception as e:
            log.logger.error("Unexpected Error: {}".format(e))

    log.logger.info("结束时间------>" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))


# 设备AI分析
def device_analysis(db, device, yesterday_start, yesterday_end):
    device_id = str(device['id'])
    table_name = "g" + device_id

    # 获取目标温度5小时恒定数组
    trg_temp_obj_arr = get_trg_temp_time(db, table_name, yesterday_start, yesterday_end)
    # 遍历目标温度5小时恒定数组
    one_day_room_temp_obj_arr = []
    for trg_temp_obj in trg_temp_obj_arr:
        # 获取恒温时段数组
        room_temp_obj_arr = get_constant_temp_time(db, table_name, trg_temp_obj['start_time'],
                                                   trg_temp_obj['end_time'], trg_temp_obj['trg_temp'])
        # 遍历恒温时段数组
        for room_temp_obj in room_temp_obj_arr:
            one_day_room_temp_obj_arr.append(room_temp_obj)
    # 判断设备整天有无满足恒温燃烧条件的时段
    if len(one_day_room_temp_obj_arr) == 0:
        log.logger.warning(table_name + "设备整天都不满足恒温燃烧的条件")
        return
    # 对恒温时段按标准值从小到大排序
    one_day_room_temp_obj_arr.sort(key=lambda x: x["room_temp_std"])
    for room_temp_obj in one_day_room_temp_obj_arr:
        obj = {}
        # 初始化内存对象
        init_obj(db, table_name, room_temp_obj, obj)
        # 检验是否满足燃烧条件
        flag, heating_return_water_temp_arr = check_burn_status(db, table_name, room_temp_obj, obj)
        if flag is False:
            continue
        # 分析燃烧状况
        burn_status = 0
        if (room_temp_obj['trg_temp'] * 10) > (room_temp_obj['room_temp_mean'] + ROOM_TEMP_AVG_RANGE):
            log.logger.info(table_name + "从" + timestamp_to_date(room_temp_obj['start_time']) + "到" + timestamp_to_date(
                room_temp_obj['end_time']) + "的目标温度为：%f" % room_temp_obj['trg_temp'] + "，平均值为：%f" % room_temp_obj[
                                'room_temp_mean'] + "，标准差为:%f" % room_temp_obj['room_temp_std'] + ", 室温没有达到目标温度，需调大CHR")
            burn_status = 1
        elif (room_temp_obj['room_temp_mean'] - ROOM_TEMP_AVG_RANGE) > (room_temp_obj['trg_temp'] * 10):
            log.logger.info(table_name + "从" + timestamp_to_date(room_temp_obj['start_time']) + "到" + timestamp_to_date(
                room_temp_obj['end_time']) + "的目标温度为：%f" % room_temp_obj['trg_temp'] + "，平均值为：%f" % room_temp_obj[
                                'room_temp_mean'] + "，标准差为:%f" % room_temp_obj['room_temp_std'] + ", 烧超温，需调小CHR")
            burn_status = 2
        else:
            if room_temp_obj['room_temp_std'] > ROOM_TEMP_STANDARD_OPTIMUM:
                log.logger.info(
                    table_name + "从" + timestamp_to_date(room_temp_obj['start_time']) + "到" + timestamp_to_date(
                        room_temp_obj['end_time']) + "的目标温度为：%f" % room_temp_obj['trg_temp'] + "，平均值为：%f" %
                    room_temp_obj[
                        'room_temp_mean'] + "，标准差为:%f" % room_temp_obj['room_temp_std'] + "，需调小CHR")
                burn_status = 3

        # 向小沃精灵发送chr值
        if send_chr_to_ciaowarm(device_id, burn_status, obj, heating_return_water_temp_arr) is True:
            # 计算升温时长
            get_heating_up_time(db, table_name, room_temp_obj)
        return


# 查询前一天满足目标温度5个小时内不变动的时间段
def get_trg_temp_time(db, table_name, yesterday_start, yesterday_end):
    data = db.find(table_name,
                   {"$and": [{"timestamp": {'$gte': yesterday_start, '$lt': yesterday_end}},
                             {"thermostats.trg_temp": {"$exists": "true"}}]},
                   {"thermostats.trg_temp": '1', "timestamp": '1'})
    # 存放最新目标温度值和时间戳
    trg_temp = {}
    # 存放满足目标温度5个小时内不变动的时间段
    trg_temp_obj = {}
    trg_temp_obj_arr = []
    for item in data:
        if 'thermostats' in item:
            thermostat = item['thermostats'][0]
            # 目标温度5个小时之内不变动，室温维持稳定3个小时才认为这些数据有分析的价值
            if 'trg_temp' in thermostat:
                if 'trg_temp' in trg_temp:
                    if thermostat['trg_temp'] != trg_temp['trg_temp']:
                        if math.floor(
                                (item['timestamp'] - trg_temp['trg_temp_time']) / 3600000) >= TRG_TEMP_NOT_CHANGE_TIME:
                            trg_temp_obj['start_time'] = trg_temp['trg_temp_time']
                            trg_temp_obj['end_time'] = item['timestamp']
                            trg_temp_obj['trg_temp'] = trg_temp['trg_temp']
                            trg_temp_obj_arr.append(trg_temp_obj)
                            # 必须重新创建字典，不然会覆盖数组里面的字典
                            trg_temp_obj = {}
                        trg_temp['trg_temp'] = thermostat['trg_temp']
                        trg_temp['trg_temp_time'] = item['timestamp']
                else:
                    trg_temp['trg_temp'] = thermostat['trg_temp']
                    trg_temp['trg_temp_time'] = item['timestamp']

    # 判断目标温度最后一个上报点到结束时间时长
    if 'trg_temp_time' in trg_temp and math.floor(
            (yesterday_end - trg_temp['trg_temp_time']) / 3600000) >= TRG_TEMP_NOT_CHANGE_TIME:
        trg_temp_obj['start_time'] = trg_temp['trg_temp_time']
        trg_temp_obj['end_time'] = yesterday_end
        trg_temp_obj['trg_temp'] = trg_temp['trg_temp']
        trg_temp_obj_arr.append(trg_temp_obj)

    return trg_temp_obj_arr


# 查询某时段内房间室温
def get_constant_temp_time(db, table_name, start_time, end_time, trg_temp):
    query_start_time = start_time
    query_end_time = start_time + ROOM_TEMP_CONSTANT_TIME
    room_temp_return_obj = {}
    room_temp_return_obj_arr = []

    while (True):
        room_temp_obj = {}
        room_temp_obj_arr = []
        # 判断网关是否是在线状态
        online_status = device_online_status(db, table_name, query_start_time, query_end_time)
        if online_status is False:
            query_start_time = query_start_time + ROOM_TEMP_STEP_TIME
            query_end_time = query_end_time + ROOM_TEMP_STEP_TIME
            continue
        # 查询室温数据
        front_room_temp = 0
        # 查询query_start_time之前的第一个室温点
        data = get_front_data(db, table_name, query_start_time, "thermostats.room_temp")
        if data is not None:
            if 'thermostats' in data:
                thermostat = data['thermostats'][0]
                # 房间温度
                front_room_temp = thermostat['room_temp']
        # 查询query_start_time到query_end_time之间的室温数据
        data = db.find(table_name,
                       {"$and": [{"timestamp": {'$gte': query_start_time, '$lt': query_end_time}},
                                 {"thermostats.room_temp": {"$exists": "true"}}]},
                       {"thermostats.room_temp": '1', "timestamp": '1'})
        for item in data:
            if 'thermostats' in item:
                thermostat = item['thermostats'][0]
                # 房间温度
                room_temp_obj['room_temp'] = thermostat['room_temp']
                room_temp_obj['timestamp'] = item['timestamp']
                room_temp_obj_arr.append(room_temp_obj)
                room_temp_obj = {}

        if len(room_temp_obj_arr) > 18:
            # 获取该时间段统计数据离散性的数组
            room_temp_arr = get_room_temp_discrete_value(query_start_time, query_end_time, front_room_temp,
                                                         room_temp_obj_arr)
            # 室温数组的平均值
            room_temp_mean = np.mean(room_temp_arr)
            # 室温数组的标准差，标准差是方差的算术平方根，标准差能反应一个数据集的离散程度。
            # 标准差=sqrt(((x1-x)^2 +(x2-x)^2 +......(xn-x)^2)/n)
            room_temp_std = np.std(room_temp_arr, ddof=0)
            # 标准差小于9，我们就认为该时段有分析价值
            # log.logger.info(table_name + "设备从" + timestamp_to_date(query_start_time) + "到" + timestamp_to_date(
            #     query_end_time) + "标准差为：" + str(room_temp_std))
            if room_temp_std < ROOM_TEMP_STANDARD_DEVIATION:
                # 平均值在目标温度波动范围0.6度以外，则认为没有达到目标温度
                room_temp_return_obj['start_time'] = query_start_time
                room_temp_return_obj['end_time'] = query_end_time
                room_temp_return_obj['room_temp_mean'] = room_temp_mean
                room_temp_return_obj['room_temp_std'] = room_temp_std
                room_temp_return_obj['trg_temp'] = trg_temp
                # 计算升温时长的起始时间
                room_temp_return_obj['heating_up_start_time'] = start_time
                # 目标温度结束时间
                room_temp_return_obj['trg_temp_end_time'] = end_time
                # 将离散数据放入数组中
                room_temp_return_obj_arr.append(room_temp_return_obj)
                # 必须重新创建字典，不然会覆盖数组里面之前的字典
                room_temp_return_obj = {}

        query_start_time = query_start_time + ROOM_TEMP_STEP_TIME
        query_end_time = query_end_time + ROOM_TEMP_STEP_TIME

        if query_end_time > end_time:
            break

    return room_temp_return_obj_arr


# 判断设备在线状态是否正常
def device_online_status(db, table_name, query_start_time, query_end_time):
    # 判断网关是否是在线状态
    online_status = {}
    data = get_front_data(db, table_name, query_start_time, "online")
    # 查询网关在线状态query_start_time之前的第一个点
    if data is not None:
        if "online" in data:
            online_status['online'] = data['online']
            online_status['online_time'] = query_start_time
    # 查询网关在线状态query_start_time到query_end_time之间的数据
    data = db.find(table_name,
                   {"$and": [{"timestamp": {'$gt': query_start_time, '$lte': query_end_time}},
                             {"online": {"$exists": "true"}}]},
                   {"timestamp": 1, "online": 1, "_id": 0})
    # 先判断网关在线状态是否符合要求(离线时长不能超过3分钟)
    for item in data:
        if 'online' in item:
            # obj存放前一个点数据，item存放后一个点数据
            if 'online' in online_status:
                if online_status['online'] is not True:
                    # 前一个点为False时，进行比较
                    if (item['timestamp'] - online_status['online_time']) > ABNORMAL_BURN_STATUS_TIME:
                        log.logger.warning(table_name + "设备从" + timestamp_to_date(
                            query_start_time) + "到" + timestamp_to_date(
                            query_end_time) + "网关离线时长超过3分钟")
                        return False
                    # 后一个点为True时，直接赋值
                    if item['online'] is True:
                        online_status['online'] = item['online']
                        online_status['online_time'] = item['timestamp']
                else:
                    # 前一个点为True时，后一个点直接赋值给前一个点
                    online_status['online'] = item['online']
                    online_status['online_time'] = item['timestamp']
            else:
                # 存放第一个点数据
                online_status['online'] = item['online']
                online_status['online_time'] = item['timestamp']

    return True


# 计算室温离散值
def get_room_temp_discrete_value(query_start_time, query_end_time, front_room_temp, room_temp_obj_arr):
    start_time = query_start_time
    end_time = query_start_time + ROOM_TEMP_SEGMENT_TIME
    # 存放两分钟室温值
    room_temp = []
    # 存放两分钟室温的平均值
    room_temp_mean = []
    for room_temp_obj in room_temp_obj_arr:
        while (True):
            if start_time <= room_temp_obj['timestamp'] <= end_time:
                room_temp.append(room_temp_obj['room_temp'])
                break
            else:
                if len(room_temp) > 0:
                    room_temp_mean.append(np.mean(room_temp))
                else:
                    room_temp_mean.append(front_room_temp)

                room_temp = []
                start_time = end_time
                end_time = end_time + ROOM_TEMP_SEGMENT_TIME

                if end_time > query_end_time:
                    break

        # 前一个室温点值
        front_room_temp = room_temp_obj['room_temp']

    while (True):
        if end_time > query_end_time:
            break

        if len(room_temp) > 0:
            room_temp_mean.append(np.mean(room_temp))
        else:
            room_temp_mean.append(front_room_temp)

        room_temp = []
        end_time = end_time + ROOM_TEMP_SEGMENT_TIME

    return room_temp_mean


# 查询start_time时间点的前一个数据点
def get_front_data(db, table_name, start_time, para):
    data = db.find_one(table_name,
                       {"timestamp": {"$lte": start_time}, para: {"$exists": "true"}},
                       {"timestamp": 1, para: 1, "_id": 0}, sort=-1)
    return data


# 查询start_time时间点的后一个数据点
def get_next_data(db, table_name, start_time, para):
    data = db.find_one(table_name,
                       {"timestamp": {"$gte": start_time}, para: {"$exists": "true"}},
                       {"timestamp": 1, para: 1, "_id": 0})
    return data


# 初始化内存对象
def init_obj(db, table_name, room_temp_obj, obj):
    # 初始化内存对象
    data = db.find_one(table_name, {"timestamp": {'$lte': room_temp_obj['start_time']}, "flag": {"$exists": "true"}},
                       sort=-1)

    if data is not None:
        if 'thermostats' in data:
            if len(data['thermostats']) != 0:
                thermostat = data['thermostats'][0]
                obj['thermostat_work_mode'] = thermostat['work_mode']
                obj['thermostat_work_mode_time'] = data['timestamp']

        if 'boilers' in data:
            if len(data['boilers']) != 0:
                boiler = data['boilers'][0]
                if 'online' in boiler:
                    obj['boiler_online'] = boiler['online']
                    obj['boiler_online_time'] = data['timestamp']

                if 'heating_ctrl' in boiler:
                    obj['heating_ctrl'] = boiler['heating_ctrl']
                    obj['heating_ctrl_time'] = data['timestamp']

                if 'season_ctrl' in boiler:
                    obj['season_ctrl'] = boiler['season_ctrl']
                    obj['season_ctrl_time'] = data['timestamp']

                if 'work_mode' in boiler:
                    obj['boiler_work_mode'] = boiler['work_mode']
                    obj['boiler_work_mode_time'] = data['timestamp']

                if 'fault_code' in boiler:
                    obj['fault_code'] = boiler['fault_code']
                    obj['fault_code_time'] = data['timestamp']

                if 'radiator_type' in boiler:
                    obj['radiator_type'] = boiler['radiator_type']
                    obj['radiator_type_time'] = data['timestamp']

                if 'heating_trg_temp' in boiler:
                    obj['heating_trg_temp'] = boiler['heating_trg_temp']
                    obj['heating_trg_temp_time'] = data['timestamp']

                if 'power_output_percent' in boiler:
                    obj['power_output_percent'] = boiler['power_output_percent']
                    obj['power_output_percent_time'] = data['timestamp']

                if 'heating_water_temp' in boiler:
                    obj['heating_water_temp'] = boiler['heating_water_temp']
                    obj['heating_water_temp_time'] = data['timestamp']

                if 'heating_return_water_temp' in boiler:
                    obj['heating_return_water_temp'] = boiler['heating_return_water_temp']
                    obj['heating_return_water_temp_time'] = data['timestamp']

                if 'receiver' in boiler:
                    receiver = boiler['receiver']
                    if 'auto_ctrl' in receiver:
                        obj['auto_ctrl'] = receiver['auto_ctrl']
                        obj['auto_ctrl_time'] = data['timestamp']

                    if 'online' in receiver:
                        obj['receiver_online'] = receiver['online']
                        obj['receiver_online_time'] = data['timestamp']


# 校验某个时段内，壁挂炉是否处于正常燃烧状态
def check_burn_status(db, table_name, room_temp_obj, obj):
    # 标识恒温时段是否满足采暖要求
    flag = True
    # 采暖回水温度数组
    heating_return_water_temp_arr = []
    data = db.find(table_name, {
        "$and": [
            {"timestamp": {'$gte': room_temp_obj['start_time'], '$lt': room_temp_obj['end_time']}}]})
    for item in data:
        if 'thermostats' in item:
            if len(item['thermostats']) != 0:
                thermostat = item['thermostats'][0]
                # 温控器工作模式[0离家, 1居家, 2睡眠]，温控器工作模式不能改变
                if 'work_mode' in thermostat:
                    if 'thermostat_work_mode' in obj:
                        if obj['thermostat_work_mode'] != thermostat['work_mode']:
                            log.logger.warning(table_name + "设备" + timestamp_to_date(
                                room_temp_obj['start_time']) + "到" + timestamp_to_date(
                                room_temp_obj['end_time']) + "温控器工作模式有变动")
                            flag = False
                            break
                    else:
                        obj['thermostat_work_mode'] = thermostat['work_mode']
                        obj['thermostat_work_mode_time'] = item['timestamp']

        if 'boilers' in item:
            if len(item['boilers']) == 0:
                continue
            boiler = item['boilers'][0]
            # 壁挂炉在线状态，必须为True
            if 'online' in boiler:
                # obj存放前一个点数据，boiler存放后一个点数据
                if 'boiler_online' in obj:
                    if obj['boiler_online'] is not True:
                        # 前一个点为False时，进行比较
                        if (item['timestamp'] - obj['boiler_online_time']) > ABNORMAL_BURN_STATUS_TIME:
                            log.logger.warning(table_name + "设备" + timestamp_to_date(
                                room_temp_obj['start_time']) + "到" + timestamp_to_date(
                                room_temp_obj['end_time']) + "壁挂炉离线时长超过3分钟")
                            flag = False
                            break
                        # 后一个点为True时，直接赋值
                        if boiler['online'] is True:
                            obj['boiler_online'] = boiler['online']
                            obj['boiler_online_time'] = item['timestamp']
                    else:
                        # 前一个点为True时，后一个点直接赋值给前一个点
                        obj['boiler_online'] = boiler['online']
                        obj['boiler_online_time'] = item['timestamp']
                else:
                    # 存放第一个点数据
                    obj['boiler_online'] = boiler['online']
                    obj['boiler_online_time'] = item['timestamp']

            # 采暖允许，必须允许状态True，特殊处理
            if 'heating_ctrl' in boiler:
                obj['heating_ctrl'] = boiler['heating_ctrl']
                obj['heating_ctrl_time'] = item['timestamp']

            # 冬夏模式，必须为冬季True
            if 'season_ctrl' in boiler:
                if boiler['season_ctrl'] is False:
                    log.logger.warning(table_name + "设备" + timestamp_to_date(
                        room_temp_obj['start_time']) + "到" + timestamp_to_date(
                        room_temp_obj['end_time']) + "壁挂炉冬夏模式出现False")
                    flag = False
                    break
                obj['season_ctrl'] = boiler['season_ctrl']
                obj['season_ctrl_time'] = item['timestamp']

            # 壁挂炉工作模式，必须为采暖模式2
            if 'work_mode' in boiler:
                # obj存放前一个点数据，boiler存放后一个点数据
                if 'boiler_work_mode' in obj:
                    if obj['boiler_work_mode'] != 2:
                        # 前一个点为非2时，进行判断
                        if (item['timestamp'] - obj['boiler_work_mode_time']) > ABNORMAL_BURN_STATUS_TIME:
                            log.logger.warning(table_name + "设备" + timestamp_to_date(
                                room_temp_obj['start_time']) + "到" + timestamp_to_date(
                                room_temp_obj['end_time']) + "壁挂炉工作模式为非采暖模式时长超过3分钟")
                            flag = False
                            break
                        if boiler['work_mode'] == 2:
                            obj['boiler_work_mode'] = boiler['work_mode']
                            obj['boiler_work_mode_time'] = item['timestamp']
                    else:
                        # 前一个点为2时，后一个点直接赋值给前一个点
                        obj['boiler_work_mode'] = boiler['work_mode']
                        obj['boiler_work_mode_time'] = item['timestamp']
                else:
                    # 存放第一个点数据
                    obj['boiler_work_mode'] = boiler['work_mode']
                    obj['boiler_work_mode_time'] = item['timestamp']

            # 故障码，必须为无故障状态0
            if 'fault_code' in boiler:
                # obj存放前一个点数据，boiler存放后一个点数据
                if 'fault_code' in obj:
                    if obj['fault_code'] != 0:
                        # 前一个点为非0时，进行比较
                        if (item['timestamp'] - obj['fault_code_time']) > ABNORMAL_BURN_STATUS_TIME:
                            log.logger.warning(table_name + "设备" + timestamp_to_date(
                                room_temp_obj['start_time']) + "到" + timestamp_to_date(
                                room_temp_obj['end_time']) + "壁挂炉故障码为非0状态时长超过3分钟")
                            flag = False
                            break
                        if boiler['fault_code'] == 0:
                            obj['fault_code'] = boiler['fault_code']
                            obj['fault_code_time'] = item['timestamp']
                    else:
                        # 前一个点为0时，后一个点直接赋值给前一个点
                        obj['fault_code'] = boiler['fault_code']
                        obj['fault_code_time'] = item['timestamp']
                else:
                    # 存放第一个点数据
                    obj['fault_code'] = boiler['fault_code']
                    obj['fault_code_time'] = item['timestamp']

            # 散热器，1为地暖，0为暖气片
            if 'radiator_type' in boiler:
                if 'radiator_type' in obj:
                    if obj['radiator_type'] != boiler['radiator_type']:
                        log.logger.warning(table_name + "设备" + timestamp_to_date(
                            room_temp_obj['start_time']) + "到" + timestamp_to_date(
                            room_temp_obj['end_time']) + "壁挂炉散热器类型有变动")
                        flag = False
                        break
                else:
                    obj['radiator_type'] = boiler['radiator_type']
                    obj['radiator_type_time'] = item['timestamp']

            # 采暖目标温度，地暖最大的采暖目标温度为60度，暖气片最大的采暖目标温度为80度
            # if 'heating_trg_temp' in boiler:
            #     obj['heating_trg_temp'] = boiler['heating_trg_temp']
            #     obj['heating_trg_temp_time'] = item['timestamp']

            # 功率输出百分比
            # if 'power_output_percent' in boiler:
            #     obj['power_output_percent'] = boiler['power_output_percent']
            #     obj['power_output_percent_time'] = item['timestamp']

            # 采暖出水温度
            # if 'heating_water_temp' in boiler:
            #     obj['heating_water_temp'] = boiler['heating_water_temp']
            #     obj['heating_water_temp_time'] = item['timestamp']

            # 采暖回水温度
            if 'heating_return_water_temp' in boiler:
                heating_return_water_temp_arr.append(boiler['heating_return_water_temp'])

            # 接收器参数
            if 'receiver' in boiler:
                receiver = boiler['receiver']
                # 必须为AI控制True
                if 'auto_ctrl' in receiver:
                    if receiver['auto_ctrl'] is False:
                        log.logger.warning(table_name + "设备" + timestamp_to_date(
                            room_temp_obj['start_time']) + "到" + timestamp_to_date(
                            room_temp_obj['end_time']) + "壁挂炉AI控制出现False")
                        flag = False
                        break
                    obj['auto_ctrl'] = receiver['auto_ctrl']
                    obj['auto_ctrl_time'] = item['timestamp']

                # 接收器离线时长不能超过3分钟
                if 'online' in receiver:
                    # obj存放前一个点数据，receiver存放后一个点数据
                    if 'receiver_online' in obj:
                        if obj['receiver_online'] is not True:
                            # 前一个点为False时，进行比较
                            if (item['timestamp'] - obj['receiver_online_time']) > ABNORMAL_BURN_STATUS_TIME:
                                log.logger.warning(table_name + "设备" + timestamp_to_date(
                                    room_temp_obj['start_time']) + "到" + timestamp_to_date(
                                    room_temp_obj['end_time']) + "接收器离线时长超过3分钟")
                                flag = False
                                break
                            # 后一个点为True时，直接赋值
                            if receiver['online'] is True:
                                obj['receiver_online'] = receiver['online']
                                obj['receiver_online_time'] = item['timestamp']
                        else:
                            # 前一个点为True时，后一个点直接赋值给前一个点
                            obj['receiver_online'] = receiver['online']
                            obj['receiver_online_time'] = item['timestamp']
                    else:
                        # 存放第一个点数据
                        obj['receiver_online'] = receiver['online']
                        obj['receiver_online_time'] = item['timestamp']

    return flag, heating_return_water_temp_arr


# 向小沃精灵发送chr值
def send_chr_to_ciaowarm(device_id, burn_status, obj, heating_return_water_temp_arr):
    # 判断壁挂炉是否是最大功率在燃烧，如果采暖回水温度已达到上限就没有调大CHR的必要
    if burn_status == 1:
        # radiator_type(1:地暖, 2:暖气片)
        # 地暖升温阶段上限为60度，地暖恒温阶段上限为50度
        # 暖气片升温阶段上限为80度，暖气片恒温阶段上限为70度
        if len(heating_return_water_temp_arr) > 0:
            heating_return_water_temp_mean = np.mean(heating_return_water_temp_arr)
            if obj['radiator_type'] == 1 and heating_return_water_temp_mean > 49:
                log.logger.warning(str(device_id) + "设备采暖回水温度已达到上限，采用地暖类型，采暖回水温度平均值为：" +
                                   str(heating_return_water_temp_mean) + "，采暖回水温度数量为：" + str(
                    len(heating_return_water_temp_arr)))
                return False
            elif obj['radiator_type'] == 2 and heating_return_water_temp_mean > 69:
                log.logger.warning(str(device_id) + "设备采暖回水温度已达到上限，采用暖气片类型，采暖回水温度平均值为：" +
                                   str(heating_return_water_temp_mean) + "，采暖回水温度数量为：" + str(
                    len(heating_return_water_temp_arr)))
                return False

    gateway = http.get_device_memory_info(device_id)
    gateway_id = gateway['gateway_id']
    thermostat = gateway['thermostats'][0]
    thermostat_id = thermostat['thermostat_id']
    chr = thermostat['chr']
    # 需调大CHR
    if burn_status == 1:
        # python里面计算浮点类型存在精确性
        chr = Decimal(str(chr)) + Decimal(str(CHR_ADJUST_RANGE))
        chr = float(chr)
        message = message_package.get_thermostat_message_package(gateway_id, thermostat_id, 'chr', chr)
        log.logger.info(message)
        # result = http.send_mqtt(message)
        # if result['message_code'] == 0:
        #     log.logger.info("需要加大CHR, MQTT发送成功")
        return False
    # 需调小CHR
    elif burn_status == 2 or burn_status == 3:
        # python里面计算浮点类型存在精确性
        chr = Decimal(str(chr)) - Decimal(str(CHR_ADJUST_RANGE))
        chr = float(chr)
        message = message_package.get_thermostat_message_package(gateway_id, thermostat_id, 'chr', chr)
        log.logger.info(message)
        # result = http.send_mqtt(message)
        # if result['message_code'] == 0:
        #     log.logger.info("需要减小CHR, MQTT发送成功")
        return False
    # 无需调整CHR，计算实际升温时长
    elif burn_status == 0:
        message = message_package.get_thermostat_message_package(gateway_id, thermostat_id, 'chr', chr)
        log.logger.info(message)
        # result = http.send_mqtt(message)
        # if result['message_code'] == 0:
        #     log.logger.info("燃烧工况良好，无需修改CHR, MQTT发送成功")
        return True


# 计算升温时长
def get_heating_up_time(db, table_name, room_temp_obj):
    # 升温起始时间整天都不满足恒温燃烧的条件
    start_time = room_temp_obj['heating_up_start_time']
    # 目标温度结束时间
    query_end_time = room_temp_obj['trg_temp_end_time']
    trg_temp = room_temp_obj['trg_temp'] * 10
    # 查询起始室温
    start_room_temp = 0
    data = get_next_data(db, table_name, start_time, "thermostats.room_temp")
    if data is not None:
        if 'thermostats' in data:
            thermostat = data['thermostats'][0]
            # 房间温度
            start_room_temp = thermostat['room_temp']
    # 目标温度高于当前室温3度以上才有计算升温时长的必要
    if (trg_temp - start_room_temp) > 30:
        data = db.find_one(table_name, {"$and": [{"timestamp": {"$gte": start_time, "$lte": query_end_time}}, {
            "thermostats.room_temp": {"$gte": trg_temp}}]}, {"timestamp": 1, "thermostats.room_temp": 1, "_id": 0})
        if data is not None:
            # 升温结束时间
            end_time = data['timestamp']
        else:
            log.logger.error(table_name + "设备出现异常，没有烧到目标温度")
    else:
        log.logger.error(table_name + "设备出现异常，没有满足条件的升温时段")


run()
# db = Database(MONGODB_IP, MONGODB_PORT, MONGODB_DB, MONGODB_USERNAME, MONGODB_PASSWORD)
# obj = {}
# room_temp_obj = {}
# room_temp_obj['start_time'] = 1562753453485
# room_temp_obj['end_time'] = 1562774400000
# get_constant_temp_time(db, "g1060", 1562918400000, 1562929200000, 24)
# get_constant_temp_time(db, "g1060", 1562860800000, 1562947199000, 24)
# init_obj(db, 'g1066', room_temp_obj, obj)
# log.logger.info(obj)
