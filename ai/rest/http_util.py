import requests, hashlib, time, json

request_url = "http://test.iwarm.com/ciaowarm"


def get_all_device():
    http_url = request_url + "/python/v1/gateway/all"
    token = get_sha1_token()
    header = {'content-type': 'application/x-www-form-urlencoded', "token": token}
    r = requests.get(http_url, headers=header)
    if r.status_code == 200:
        result = json.loads(r.text)
        return result['message_info']
    else:
        print("服务器异常，查询失败")
        return "服务器异常，查询失败"


def get_device_memory_info(device_id):
    http_url = request_url + "/python/v1/gateway/deviceFromMemory"
    data = {
        'device_id': device_id
    }
    token = get_sha1_token()
    header = {'content-type': 'application/x-www-form-urlencoded', "token": token}
    r = requests.get(http_url, params=data, headers=header)
    if r.status_code == 200:
        result = json.loads(r.text)
        return result['message_info']
    else:
        print("服务器异常，查询失败")
        return "服务器异常，查询失败"


def send_mqtt(device_data):
    http_url = request_url + "/python/v1/gateway/mqtt"
    data = {
        'device_data': device_data
    }
    token = get_sha1_token()
    header = {'content-type': 'application/x-www-form-urlencoded', "token": token}
    r = requests.put(http_url, params=data, headers=header)
    if r.status_code == 200:
        result = json.loads(r.text)
        return result


def get_home_info(device_id, query_weather_time):
    http_url = request_url + "/python/v1/gateway/homeInfo"
    data = {
        'device_id': device_id,
        'query_weather_time': query_weather_time
    }
    token = get_sha1_token()
    header = {'content-type': 'application/x-www-form-urlencoded', "token": token}
    r = requests.get(http_url, params=data, headers=header)
    if r.status_code == 200:
        result = json.loads(r.text)
        return result


def get_sha1_token():
    str = "CIAOWARM%" + time.strftime("%Y%m%d") + "_LIUDUO"
    # 第一次加密
    b = hashlib.sha1()
    b.update(str.encode(encoding='utf-8'))
    c = b.hexdigest()
    # 第二次加密
    d = hashlib.sha1()
    d.update(c.encode(encoding='utf-8'))
    token = d.hexdigest()
    return token

# str = "CIAOWARM%" + time.strftime("%Y%m%d") + "_LIUDUO"
# # 第一次加密
# b = hashlib.sha1()
# b.update(str.encode(encoding='utf-8'))
# c = b.hexdigest()
# # 第二次加密
# d = hashlib.sha1()
# d.update(c.encode(encoding='utf-8'))
# token = d.hexdigest()
# print(token)
