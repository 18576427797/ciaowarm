# unit test
import time
import datetime
import math
import numpy as np
import rest.http_util as http
import rest.message_package_util as message_package
from mongodb.pymongo_util import Database
from decimal import Decimal

# import random

# mongodb配置信息
MONGODB_IP = "47.102.220.27"
MONGODB_PORT = 27017
MONGODB_DB = "ciaowarm"
MONGODB_USERNAME = "ciaowarm"
MONGODB_PASSWORD = "Iwarm905()%"

# 目标温度不变时长，暂定超过5小时
TRG_TEMP_NOT_CHANGE_TIME = 5
# 室内温度恒定时长，暂定超过3小时，即10800000毫秒
ROOM_TEMP_CONSTANT_TIME = 10800000
# 遍历室内温度，暂定步进时长1小时，即3600000毫秒
ROOM_TEMP_STEP_TIME = 3600000
# 室内温度最小样本数，大于等于19个
ROOM_TEMP_SAMPLE_NUM = 19
# 室内温度标准差，暂定小于1.5
ROOM_TEMP_STANDARD_DEVIATION = 1.5
# 室内温度平均数波动范围，暂定0.6度以内，不包含0.6度
ROOM_TEMP_AVG_RANGE = 6
# CHR调节幅度，暂定0.1
CHR_ADJUST_RANGE = 0.1
# 某些参数在超过三分钟的时间处于非正常燃烧状态，我们就认为这段恒温时段没有分析价值
ABNORMAL_BURN_STATUS_TIME = 180000


def run():
    print("起始时间------>" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    db = Database(MONGODB_IP, MONGODB_PORT, MONGODB_DB, MONGODB_USERNAME, MONGODB_PASSWORD)
    devices = http.get_all_device()
    for device in devices:
        device_id = str(device['id'])
        table_name = "g" + device_id
        trg_temp_obj_arr = get_trg_temp_time(db, table_name)
        for trg_temp_obj in trg_temp_obj_arr:
            # 恒温时段
            room_temp_obj_arr = get_constant_temp_time(db, table_name, trg_temp_obj['start_time'],
                                                       trg_temp_obj['end_time'], trg_temp_obj['trg_temp'])
            for room_temp_obj in room_temp_obj_arr:
                obj = {}
                if check_burn_status(db, table_name, room_temp_obj, obj) == False:
                    continue
                if room_temp_obj['room_temp_status'] == 1:
                    gateway = http.get_device_memory_info(device_id)
                    gateway_id = gateway['gateway_id']
                    thermostat = gateway['thermostats'][0]
                    thermostat_id = thermostat['thermostat_id']
                    chr = thermostat['chr']
                    # python里面计算浮点类型存在精确性
                    chr = Decimal(str(chr)) + Decimal(str(CHR_ADJUST_RANGE))
                    chr = float(chr)
                    message = message_package.get_thermostat_message_package(gateway_id, thermostat_id, 'chr', chr)
                    print(message)
                    # result = http.send_mqtt(message)
                    # if result['message_code'] == 0:
                    #     print("需要加大CHR, MQTT发送成功")
                    break
                elif room_temp_obj['room_temp_status'] == 2:
                    gateway = http.get_device_memory_info(device_id)
                    gateway_id = gateway['gateway_id']
                    thermostat = gateway['thermostats'][0]
                    thermostat_id = thermostat['thermostat_id']
                    chr = thermostat['chr']
                    # python里面计算浮点类型存在精确性
                    chr = Decimal(str(chr)) - Decimal(str(CHR_ADJUST_RANGE))
                    chr = float(chr)
                    message = message_package.get_thermostat_message_package(gateway_id, thermostat_id, 'chr', chr)
                    print(message)
                    # result = http.send_mqtt(message)
                    # if result['message_code'] == 0:
                    #     print("需要减小CHR, MQTT发送成功")
                    break

    print("结束时间------>" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))


# 根据时间戳获取日期
def timestamp_to_date(timestamp):
    time_array = time.localtime(timestamp / 1000)
    date = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    return date


# 查询前一天满足目标温度5个小时内不变动的时间段
def get_trg_temp_time(db, table_name):
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_start = int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d'))) * 1000
    # yesterday_start = 1561824000000
    yesterday_end = yesterday_start + 86400000
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
    room_temp_obj = {}
    room_temp_obj_arr = []

    while (True):
        room_temp = []
        data = db.find(table_name,
                       {"$and": [{"timestamp": {'$gte': query_start_time, '$lt': query_end_time}},
                                 {"thermostats.room_temp": {"$exists": "true"}}]},
                       {"thermostats.room_temp": '1', "timestamp": '1'})
        for item in data:
            if 'thermostats' in item:
                thermostat = item['thermostats'][0]
                # 房间温度
                if 'room_temp' in thermostat:
                    room_temp.append(thermostat['room_temp'])

        # 求室温数组的平均值、方差、标准差
        if len(room_temp) >= ROOM_TEMP_SAMPLE_NUM:
            # 平均值
            room_temp_mean = np.mean(room_temp)
            # 方差，方差是每个样本值与全体样本值的平均数之差的平方值的平均数。方差=((x1-x)^2 +(x2-x)^2 +......(xn-x)^2)/n
            room_temp_var = np.var(room_temp)
            # 标准差，标准差是方差的算术平方根，标准差能反应一个数据集的离散程度。标准差=sqrt(((x1-x)^2 +(x2-x)^2 +......(xn-x)^2)/n)
            room_temp_std = np.std(room_temp, ddof=0)
            # 标准差小于1.5，我们就认为达到了恒温状态
            if room_temp_std < ROOM_TEMP_STANDARD_DEVIATION:
                # 平均值在目标温度波动范围0.6度以外，则认为没有达到目标温度
                room_temp_obj['start_time'] = query_start_time
                room_temp_obj['end_time'] = query_end_time
                if (trg_temp * 10) > (room_temp_mean + ROOM_TEMP_AVG_RANGE):
                    print(table_name + "从" + timestamp_to_date(query_start_time) + "到" + timestamp_to_date(
                        query_end_time) + "的目标温度为：%f" % trg_temp + "，平均值为：%f" % room_temp_mean +
                          ", 方差为：%f" % room_temp_var + "，标准差为:%f" % room_temp_std + ", 室温没有达到目标温度")
                    room_temp_obj['room_temp_status'] = 1
                elif (room_temp_mean - ROOM_TEMP_AVG_RANGE) > (trg_temp * 10):
                    print(table_name + "从" + timestamp_to_date(query_start_time) + "到" + timestamp_to_date(
                        query_end_time) + "的目标温度为：%f" % trg_temp + "，平均值为：%f" % room_temp_mean +
                          ", 方差为：%f" % room_temp_var + "，标准差为:%f" % room_temp_std + ", 烧超温")
                    room_temp_obj['room_temp_status'] = 2
                else:
                    print(table_name + "从" + timestamp_to_date(query_start_time) + "到" + timestamp_to_date(
                        query_end_time) + "的目标温度为：%f" % trg_temp + "，平均值为：%f" % room_temp_mean +
                          ", 方差为：%f" % room_temp_var + "，标准差为:%f" % room_temp_std)
                    room_temp_obj['room_temp_status'] = 0
                room_temp_obj_arr.append(room_temp_obj)
                # 必须重新创建字典，不然会覆盖数组里面的字典
                room_temp_obj = {}

        query_start_time = query_start_time + ROOM_TEMP_STEP_TIME
        query_end_time = query_end_time + ROOM_TEMP_STEP_TIME

        if query_end_time > end_time:
            break

    return room_temp_obj_arr


# 校验某个时段内，壁挂炉是否处于正常燃烧状态
def check_burn_status(db, table_name, room_temp_obj, obj):
    # 标识恒温时段是否满足采暖要求
    flag = True
    data = db.find(table_name, {
        "$and": [
            {"timestamp": {'$gte': room_temp_obj['start_time'], '$lt': room_temp_obj['end_time']}}]})
    for item in data:
        # 网关在线状态
        if 'online' in item:
            # obj存放前一个点数据，item存放后一个点数据
            if 'online' in obj:
                if obj['online'] != True:
                    # 前一个点为False时，进行比较
                    if (item['timestamp'] - obj['online_time']) > ABNORMAL_BURN_STATUS_TIME:
                        print(table_name + "设备" + timestamp_to_date(
                            room_temp_obj['start_time']) + "到" + timestamp_to_date(
                            room_temp_obj['end_time']) + "网关离线时长超过3分钟")
                        flag = False
                        break
                    # 后一个点为True时，直接赋值
                    if item['online'] == True:
                        obj['online'] = item['online']
                        obj['online_time'] = item['timestamp']
                else:
                    # 前一个点为True时，后一个点直接赋值给前一个点
                    obj['online'] = item['online']
                    obj['online_time'] = item['timestamp']
            else:
                # 存放第一个点数据
                obj['online'] = item['online']
                obj['online_time'] = item['timestamp']

        if 'thermostats' in item:
            if len(item['thermostats']) != 0:
                thermostat = item['thermostats'][0]
                # 温控器工作模式[0离家, 1居家, 2睡眠]，温控器工作模式不能改变
                if 'work_mode' in thermostat:
                    if 'thermostat_work_mode' in obj:
                        if obj['thermostat_work_mode'] != thermostat['work_mode']:
                            print(table_name + "设备" + timestamp_to_date(
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
                    if obj['boiler_online'] != True:
                        # 前一个点为False时，进行比较
                        if (item['timestamp'] - obj['boiler_online_time']) > ABNORMAL_BURN_STATUS_TIME:
                            print(table_name + "设备" + timestamp_to_date(
                                room_temp_obj['start_time']) + "到" + timestamp_to_date(
                                room_temp_obj['end_time']) + "壁挂炉离线时长超过3分钟")
                            flag = False
                            break
                        # 后一个点为True时，直接赋值
                        if boiler['online'] == True:
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
                if boiler['season_ctrl'] == False:
                    print(table_name + "设备" + timestamp_to_date(
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
                            print(table_name + "设备" + timestamp_to_date(
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
                            print(table_name + "设备" + timestamp_to_date(
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
                        print(table_name + "设备" + timestamp_to_date(
                            room_temp_obj['start_time']) + "到" + timestamp_to_date(
                            room_temp_obj['end_time']) + "壁挂炉散热器类型有变动")
                        flag = False
                        break
                else:
                    obj['radiator_type'] = boiler['radiator_type']
                    obj['radiator_type_time'] = item['timestamp']

            # 采暖目标温度，地暖最大的采暖目标温度为60度，暖气片最大的采暖目标温度为80度
            if 'heating_trg_temp' in boiler:
                obj['heating_trg_temp'] = boiler['heating_trg_temp']
                obj['heating_trg_temp_time'] = item['timestamp']

            # 功率输出百分比
            if 'power_output_percent' in boiler:
                obj['power_output_percent'] = boiler['power_output_percent']
                obj['power_output_percent_time'] = item['timestamp']

            # 采暖出水温度
            if 'heating_water_temp' in boiler:
                obj['heating_water_temp'] = boiler['heating_water_temp']
                obj['heating_water_temp_time'] = item['timestamp']

            # 采暖回水温度
            if 'heating_return_water_temp' in boiler:
                obj['heating_return_water_temp'] = boiler['heating_return_water_temp']
                obj['heating_return_water_temp_time'] = item['timestamp']

            if 'receiver' in boiler:
                receiver = boiler['receiver']
                # 必须为AI控制True
                if 'auto_ctrl' in receiver:
                    if receiver['auto_ctrl'] == False:
                        print(table_name + "设备" + timestamp_to_date(
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
                        if obj['receiver_online'] != True:
                            # 前一个点为False时，进行比较
                            if (item['timestamp'] - obj['receiver_online_time']) > ABNORMAL_BURN_STATUS_TIME:
                                print(table_name + "设备" + timestamp_to_date(
                                    room_temp_obj['start_time']) + "到" + timestamp_to_date(
                                    room_temp_obj['end_time']) + "接收器离线时长超过3分钟")
                                flag = False
                                break
                            # 后一个点为True时，直接赋值
                            if receiver['online'] == True:
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

    return flag


run()
# db = Database(MONGODB_IP, MONGODB_PORT, MONGODB_DB, MONGODB_USERNAME, MONGODB_PASSWORD)
# room_temp_obj = {}
# room_temp_obj['start_time'] = 1562640633000
# room_temp_obj['end_time'] = 1562651433000
# check_burn_status(db, 'g1055', room_temp_obj)
