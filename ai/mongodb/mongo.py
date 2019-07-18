# -*- coding: utf-8 -*-
# coding=utf-8
import time
from pymongo import MongoClient

# 建立MongoDB数据库连接
client = MongoClient('mongodb://ciaowarm:Iwarm905()%@47.102.220.27:27017/ciaowarm')
# client = MongoClient('localhost', 27017)

# 连接所需数据库,test为数据库名
db = client.ciaowarm

# 连接所用集合，也就是我们通常所说的表，test为表名
collection = db.g1055


# 查询壁挂炉工作模式[0空闲，1卫浴，2采暖，3一级防冻，4二级防冻，5非锁定故障，6锁定故障]
def get_work_mode(boiler_id, start_time, end_time, work_mode):
    parameter_name = "boilers.work_mode"
    work_mode_result = {"_id": 0, parameter_name: 1, "timestamp": 1}

    # 查询第一个work_mode
    work_mode_query = {"$and": [{"boilers.boiler_id": boiler_id},
                                {"timestamp": {"$lte": start_time}},
                                {parameter_name: {"$exists": "true"}}]}
    work_mode_first_data = collection.find(work_mode_query, work_mode_result).sort("timestamp", -1).limit(1)
    first_point_time = 0
    for item in work_mode_first_data:
        work_mode_mongo = item['boilers'][0]['work_mode']
        if (work_mode_mongo == work_mode):
            if (item['timestamp'] != start_time):
                first_point_time = start_time

    # 查询start_time到end_time这段时间的work_mode
    work_mode_query = {"$and": [{"boilers.boiler_id": boiler_id},
                                {"timestamp": {"$gte": start_time, "$lte": end_time}},
                                {parameter_name: {"$exists": "true"}}]}
    work_mode_data = collection.find(work_mode_query, work_mode_result).sort("timestamp", 1)
    work_mode_dict = dict()
    for item in work_mode_data:
        work_mode_mongo = item['boilers'][0]['work_mode']
        if (work_mode_mongo == work_mode):
            if (first_point_time == 0):
                first_point_time = item['timestamp']
        else:
            if (first_point_time != 0):
                work_mode_dict[first_point_time] = item['timestamp']
                first_point_time = 0
    if (first_point_time != 0 and first_point_time != end_time):
        work_mode_dict[first_point_time] = end_time

    return work_mode_dict


# 查询火焰状态[0有火，1无火]
def get_flame_status(boiler_id, start_time, end_time):
    parameter_name = "boilers.flame_status"
    flame_status_result = {"_id": 0, parameter_name: 1, "timestamp": 1}

    # 查询第一个flame_status
    flame_status_query = {"$and": [{"boilers.boiler_id": boiler_id},
                                   {"timestamp": {"$lte": start_time}},
                                   {parameter_name: {"$exists": "true"}}]}
    flame_status_first_data = collection.find(flame_status_query, flame_status_result).sort("timestamp", -1).limit(1)
    first_point_time = 0
    for item in flame_status_first_data:
        flame_status_mongo = item['boilers'][0]['flame_status']
        if (not flame_status_mongo):
            if (item['timestamp'] != start_time):
                first_point_time = start_time

    # 查询start_time到end_time这段时间的flame_status
    flame_status_query = {"$and": [{"boilers.boiler_id": boiler_id},
                                   {"timestamp": {"$gte": start_time, "$lte": end_time}},
                                   {parameter_name: {"$exists": "true"}}]}
    flame_status_data = collection.find(flame_status_query, flame_status_result).sort("timestamp", 1)
    flame_status_dict = dict()
    for item in flame_status_data:
        flame_status_mongo = item['boilers'][0]['flame_status']
        if (not flame_status_mongo):
            if (first_point_time == 0):
                first_point_time = item['timestamp']
        else:
            if (first_point_time != 0):
                flame_status_dict[first_point_time] = item['timestamp']
                first_point_time = 0
    if (first_point_time != 0 and first_point_time != end_time):
        flame_status_dict[first_point_time] = end_time

    return flame_status_dict


# 查询功率输出百分比
def get_power_output_percent(boiler_id, start_time, end_time, power):
    parameter_name = "boilers.power_output_percent"
    power_output_percent_result = {"_id": 0, parameter_name: 1, "timestamp": 1}

    # 查询第一个power_output_percent
    power_output_percent_query = {"$and": [{"boilers.boiler_id": boiler_id},
                                           {"timestamp": {"$lte": start_time}},
                                           {parameter_name: {"$exists": "true"}}]}
    power_output_percent_first_data = collection.find(power_output_percent_query, power_output_percent_result).sort(
        "timestamp", -1).limit(1)
    k = 0
    energy_total = 0.0
    for item in power_output_percent_first_data:
        power_output_percent = item['boilers'][0]['power_output_percent']
        k = (power - 5.7) * power_output_percent / 100 + 5.7

    # 查询start_time到end_time这段时间的power_output_percent
    power_output_percent_query = {"$and": [{"boilers.boiler_id": boiler_id},
                                           {"timestamp": {"$gte": start_time, "$lte": end_time}},
                                           {parameter_name: {"$exists": "true"}}]}
    power_output_percent_data = collection.find(power_output_percent_query, power_output_percent_result).sort(
        "timestamp", 1)
    for item in power_output_percent_data:
        # 计算开始时间到第一个输出功率百分比点这段时间消耗的能量
        first_point_time = item['timestamp']
        energy_total = energy_total + k * (first_point_time - start_time)

        start_time = first_point_time
        power_output_percent = item['boilers'][0]['power_output_percent']
        k = (power - 5.7) * power_output_percent / 100 + 5.7
    # 最后一个输出功率百分比到结束时间所用的能量
    energy_total = energy_total + k * (end_time - start_time)

    return energy_total


print("起始时间------>" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
energy_total = 0.0
work_mode_dict = get_work_mode(1, 1556239823000, 1556246635000, 1)
for start_time, end_time in work_mode_dict.items():
    flame_status_dict = get_flame_status(1, start_time, end_time)
    for flame_status_start_time, flame_status_end_time in flame_status_dict.items():
        energy_total = energy_total + get_power_output_percent(1, flame_status_start_time,
                                                               flame_status_end_time, 30)
work_mode_dict = get_work_mode(1, 1556239823000, 1556246635000, 2)
for start_time, end_time in work_mode_dict.items():
    flame_status_dict = get_flame_status(1, start_time, end_time)
    for flame_status_start_time, flame_status_end_time in flame_status_dict.items():
        energy_total = energy_total + get_power_output_percent(1, flame_status_start_time,
                                                               flame_status_end_time, 24)

gas_total = energy_total / 35580000
print(gas_total)
print("结束时间------>" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))

# print(db.delete("ut", {}))
# print(time.time())
# start_time = int(time.time() * 1e6)
# for i in range(100):
#     t = int(time.time() * 1e6)
#     db.insert_one("ut", {"username": str(t),
#                          "timestamp": t,
#                          "password": "aaaa",
#                          "telephone": str(random.random() * 1000000)})
# print("deleted count: ", db.delete("ut", {"timestamp": {"$gt": start_time + 500}}))
# data = db.find('g1055', {})
# for item in data:
#     print(item)
# print(db.update("ut", {"password": ["aaaa", "bbbb"]}))
# print(db.find("ut", {}, {"password": 1, "username": 1}).count())
data = db.find("",
               {"$and": [{"timestamp": {'$gte': start_time, '$lt': end_time}}]})
obj = {}
room_temp = []
for item in data:
    if 'thermostats' in item:
        thermostat = item['thermostats'][0]
        # 目标温度
        if 'trg_temp' in thermostat:
            obj['trg_temp'] = thermostat['trg_temp']
            obj['trg_temp_time'] = item['timestamp']
        # 房间温度
        if 'room_temp' in thermostat:
            obj['room_temp'] = thermostat['room_temp']
            obj['room_temp_time'] = item['timestamp']
            room_temp.append(thermostat['room_temp'])
        # 温控器工作模式[0离家, 1居家, 2睡眠]
        if 'work_mode' in thermostat:
            obj['thermostat_work_mode'] = thermostat['work_mode']
            obj['thermostat_work_mode_time'] = item['timestamp']

    if 'boilers' in item:
        boiler = item['boilers'][0]
        # 壁挂炉在线状态，必须为True
        if 'online' in boiler:
            obj['online'] = boiler['online']
            obj['online_time'] = item['timestamp']
        # 采暖允许，必须允许状态True
        if 'heating_ctrl' in boiler:
            obj['heating_ctrl'] = boiler['heating_ctrl']
            obj['heating_ctrl_time'] = item['timestamp']
        # 冬夏模式，必须为冬季True
        if 'season_ctrl' in boiler:
            obj['season_ctrl'] = boiler['season_ctrl']
            obj['season_ctrl_time'] = item['timestamp']
        # 壁挂炉工作模式，必须为采暖模式2
        if 'work_mode' in boiler:
            obj['boiler_work_mode'] = boiler['work_mode']
            obj['boiler_work_mode_time'] = item['timestamp']
        # 故障码，必须为无故障状态0
        if 'fault_code' in boiler:
            obj['fault_code'] = boiler['fault_code']
            obj['fault_code_time'] = item['timestamp']
        # 散热器，1为地暖，0为暖气片
        if 'radiator_type' in boiler:
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
                obj['auto_ctrl'] = receiver['auto_ctrl']
                obj['auto_ctrl_time'] = item['timestamp']
