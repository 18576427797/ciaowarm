# mongodb database
from pymongo import MongoClient


class Database(object):
    def __init__(self, address, port, database, username, password):
        self.conn = MongoClient(host=address, port=port)
        self.db = self.conn[database]
        self.db.authenticate(name=username, password=password)

    def get_state(self):
        return self.conn is not None and self.db is not None

    def insert_one(self, collection, data):
        if self.get_state():
            ret = self.db[collection].insert_one(data)
            return ret.inserted_id
        else:
            return ""

    def insert_many(self, collection, data):
        if self.get_state():
            ret = self.db[collection].insert_many(data)
            return ret.inserted_id
        else:
            return ""

    def update(self, collection, data):
        # data format:
        # {key:[old_data,new_data]}
        data_filter = {}
        data_revised = {}
        for key in data.keys():
            data_filter[key] = data[key][0]
            data_revised[key] = data[key][1]
        if self.get_state():
            return self.db[collection].update_many(data_filter, {"$set": data_revised}).modified_count
        return 0

    def find(self, col, condition, column=None):
        if self.get_state():
            if column is None:
                return self.db[col].find(condition)
            else:
                return self.db[col].find(condition, column)
        else:
            return None

    def find_one(self, col, condition, column=None):
        if self.get_state():
            if column is None:
                return self.db[col].find_one(condition, sort=[('timestamp', -1)])
            else:
                return self.db[col].find_one(condition, column, sort=[('timestamp', -1)])
        else:
            return None

    def delete(self, col, condition):
        if self.get_state():
            return self.db[col].delete_many(filter=condition).deleted_count
        return 0


def timestamp_to_date(timestamp):
    time_array = time.localtime(timestamp / 1000)
    date = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
    return date


# 查询前一天满足目标温度5个小时内不变动的时间段
def get_trg_temp_time(db, table_name):
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    # yesterday_start = int(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d'))) * 1000
    yesterday_start = 1562342400000
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
                        if math.floor((item['timestamp'] - trg_temp['trg_temp_time']) / 3600000) >= 5:
                            trg_temp_obj['start_time'] = trg_temp['trg_temp_time']
                            trg_temp_obj['end_time'] = item['timestamp']
                            trg_temp_obj['trg_temp'] = trg_temp['trg_temp']
                            trg_temp_obj_arr.append(trg_temp_obj)
                        trg_temp['trg_temp'] = thermostat['trg_temp']
                        trg_temp['trg_temp_time'] = item['timestamp']
                else:
                    trg_temp['trg_temp'] = thermostat['trg_temp']
                    trg_temp['trg_temp_time'] = item['timestamp']

    # 判断目标温度最后一个上报点到结束时间时长
    if 'trg_temp_time' in trg_temp and math.floor((yesterday_end - trg_temp['trg_temp_time']) / 3600000) >= 5:
        trg_temp_obj['start_time'] = trg_temp['trg_temp_time']
        trg_temp_obj['end_time'] = yesterday_end
        trg_temp_obj['trg_temp'] = trg_temp['trg_temp']
        trg_temp_obj_arr.append(trg_temp_obj)

    return trg_temp_obj_arr


# 查询某时段内房间室温
def get_constant_temp_time(db, table_name, start_time, end_time, trg_temp):
    query_start_time = start_time
    query_end_time = start_time + 10800000
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
        if len(room_temp) > 18:
            # 平均值
            room_temp_mean = np.mean(room_temp)
            # 方差，方差是每个样本值与全体样本值的平均数之差的平方值的平均数。方差=((x1-x)^2 +(x2-x)^2 +......(xn-x)^2)/n
            room_temp_var = np.var(room_temp)
            # 标准差，标准差是方差的算术平方根，标准差能反应一个数据集的离散程度。标准差=sqrt(((x1-x)^2 +(x2-x)^2 +......(xn-x)^2)/n)
            room_temp_std = np.std(room_temp, ddof=0)
            # 标准差小于1.5，我们就认为达到了恒温状态
            if room_temp_std < 1.5:
                # 平均值在目标温度波动范围0.6度以外，则认为没有达到目标温度
                room_temp_obj['start_time'] = query_start_time
                room_temp_obj['end_time'] = query_end_time
                if (trg_temp * 10) > (room_temp_mean + 6):
                    print("室温没有达到目标温度")
                    room_temp_obj['room_temp_status'] = 1
                elif (room_temp_mean - 6) > (trg_temp * 10):
                    print("烧超温")
                    room_temp_obj['room_temp_status'] = 2
                else:
                    room_temp_obj['room_temp_status'] = 0
                room_temp_obj_arr.append(room_temp_obj)
            print(timestamp_to_date(query_start_time) + "到" + timestamp_to_date(
                query_end_time) + "的平均值为：%f" % room_temp_mean + ", 方差为：%f" % room_temp_var +
                  "，标准差为:%f" % room_temp_std + ", " + str(room_temp))
        else:
            print(timestamp_to_date(query_start_time) + "到" + timestamp_to_date(query_end_time) + "的room_temp为空")

        query_start_time = query_start_time + 3600000
        query_end_time = query_end_time + 3600000

        if query_end_time > end_time:
            break

    return room_temp_obj_arr


if __name__ == '__main__':
    # unit test
    import time
    import datetime
    import math
    import numpy as np
    import rest.http_util as http
    import rest.message_package_util as message_package

    # import random

    print("起始时间------>" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    db = Database("47.102.220.27", 27017, "ciaowarm", "ciaowarm", "Iwarm905()%")
    device_id = 1060
    table_name = "g" + str(1060)

    trg_temp_obj_arr = get_trg_temp_time(db, table_name)

    obj = {}
    for trg_temp_obj in trg_temp_obj_arr:
        # 室温变化数组
        room_temp_obj_arr = get_constant_temp_time(db, table_name, trg_temp_obj['start_time'],
                                                   trg_temp_obj['end_time'], trg_temp_obj['trg_temp'])
        for room_temp_obj in room_temp_obj_arr:
            if room_temp_obj['room_temp_status'] == 1:
                gateway = http.get_device_memory_info(device_id)
                gateway_id = gateway['gateway_id']
                thermostat = gateway['thermostats'][0]
                thermostat_id = thermostat['thermostat_id']
                chr = thermostat['chr']
                chr = float(chr) + float(0.1)
                message = message_package.get_thermostat_message_package(gateway_id, thermostat_id, 'chr', chr)
                print(message)
                result = http.send_mqtt(message)
                if result['message_code'] == 0:
                    print("需要加大CHR, MQTT发送成功")
            elif room_temp_obj['room_temp_status'] == 2:
                gateway = http.get_device_memory_info(device_id)
                gateway_id = gateway['gateway_id']
                thermostat = gateway['thermostats'][0]
                thermostat_id = thermostat['thermostat_id']
                chr = thermostat['chr']
                chr = chr - 0.1
                message = message_package.get_thermostat_message_package(gateway_id, thermostat_id, 'chr', chr)
                print(message)
                result = http.send_mqtt(message)
                if result['message_code'] == 0:
                    print("需要减小CHR, MQTT发送成功")
            else:
                data = db.find(table_name, {
                    "$and": [{"timestamp": {'$gte': room_temp_obj['start_time'], '$lt': room_temp_obj['end_time']}}]})
                print("data------>" + str(data.count()))
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
                            # if boiler['work_mode'] != 2:
                            # print("壁挂炉为非采暖模式")
                            # break
                            obj['boiler_work_mode'] = boiler['work_mode']
                            obj['boiler_work_mode_time'] = item['timestamp']
                        # 故障码，必须为无故障状态0
                        if 'fault_code' in boiler:
                            # if boiler['fault_code'] != 0:
                            # print("壁挂炉出现故障代码" + str(boiler['fault_code']))
                            # break
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

    print("结束时间------>" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
