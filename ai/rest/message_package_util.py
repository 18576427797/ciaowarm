def get_gateway_message_package(gateway_id, param_key, param_value):
    json = {
        "method": "update",
        "data": {
            "gateway_id": gateway_id,
            param_key: param_value
        }
    }
    return str(json)


def get_thermostat_message_package(gateway_id, thermostat_id, param_key, param_value):
    json = {
        "method": "update",
        "data": {
            "gateway_id": gateway_id,
            "thermostats": [{
                "thermostat_id": thermostat_id,
                param_key: param_value
            }]
        }
    }
    return str(json)


def get_boiler_message_package(gateway_id, boiler_id, param_key, param_value):
    json = {
        "method": "update",
        "data": {
            "gateway_id": gateway_id,
            "boilers": [{
                "boiler_id": boiler_id,
                param_key: param_value
            }]
        }
    }
    return str(json)
