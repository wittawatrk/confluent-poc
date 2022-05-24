import json
import pytz
import base64
from datetime import datetime

UC1122_SERIAL = [
    1550003,
    1670003,
    1680061,
    1680062,
    1680063,
    1680064,
    1680065,
    1680066,
    1680067,
    1710002,
    1710005,
    1710008,
    1710012,
    1710015,
    1710017,
    1710023,
]

UC1152_SERIAL = [
    1550003,
    1670003,
    1680061,
    1680062,
    1680063,
    1680064,
    1680065,
    1680066,
    1680067,
    1710002,
    1710005,
    1710008,
    1710012,
    1710015,
    1710017,
    1710023
]

EM300_SERIAL = [
    1550005,
    1550006,
    1550007,
    1550008,
    1710004,
    1710007,
    1710010,
    1710014,
    1710020,
    1710021,
    1710022,
]

AM102_SERIAL = [
    1550009
]

UC50x_SERIAL = [
    1600002,
    1600003,
    1600004,
    1600006,
    1600007,
    1600008,
    1600009,
    1600010,
    1600011,
    1600012,
    1600014,
    1600015,
    1600016,
    1600017,
    1600018,
    1600019,
    1600093,
    1600095,
    1600097,
    1600099,
    1600101,
]

RA0716Y_SERIAL = [ 
    1680048,
]

def acked(err, msg):
    # global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        pass

def is_data_batch(topic_parts):
    return topic_parts[0] == "app" and topic_parts[-1] == "telemetry"

def get_payloads(record_value):
    payloads = json.loads(record_value.decode('utf-8'))
    
    if type(payloads) is list:
        return map(add_serial, payloads)
         
    tmp = [] 
    tmp.append(payloads)

    return map(add_serial, tmp)

def add_serial(payload):
    for key in payload:
        if key in ['emid', 'esid', 'external_sensor_id', 'deviceName']:
            payload['serial_id'] = payload[key]
            break
    #payload['serial_id'] = lambda x: payload[x] if x in ['emid', 'esid', 'external_sensor_id', 'deviceName'] else payload['serial_id']

    return payload

def is_uc1122(serial_id):
    if int(serial_id) in UC1122_SERIAL:
        return True

    return False

def is_event(payload):
    decoded_bytes = base64.b64decode(payload['data'])

    return decoded_bytes[:2].hex().startswith('ff12')

def is_uc1152(serial_id):
    if int(serial_id) in UC1152_SERIAL:
        return True

    return False

def is_uc11t1(serial_id):
    if int(serial_id) in UC11t1_SERIAL:
        return True

    return False

def is_em300(serial_id):
    if int(serial_id) in EM300_SERIAL:
        return True

    return False

def is_am102(serial_id):
    if int(serial_id) in AM102_SERIAL:
        return True

    return False

def is_uc50x(serial_id):
    if int(serial_id) in UC50x_SERIAL:
        return True

    return False

def is_ra0716y(serial_id):
    if int(serial_id) in RA0716Y_SERIAL:
        return True

    return False

timezone = pytz.timezone("Asia/Bangkok")
def get_dt(payload, timestamp):
    ## ex: "time": "2021-04-29T07:05:47.226422Z"
    if 'time' in payload:
        dt = datetime.strptime(payload['time'], "%Y-%m-%dT%H:%M:%S.%fZ")
        return timezone.localize(dt).strftime("%Y%m%d%H%M%S")

    ## "mdt": "202104282100" still check length 12
    if 'mdt' in payload:
        payload['dt'] = payload['mdt']

    ## "sdt": "20220511154100" length 14
    ## "sdt": "202205111541" still check length 12, 
    if 'sdt' in payload:
        payload['dt'] = payload['sdt']

    # ex: "dt": "202104282100"
    if 'dt' in payload and len(payload['dt']) == 12:
        return datetime.strptime(payload['dt'], "%Y%m%d%H%M").strftime("%Y%m%d%H%M00")

    ## ex: "use_current_time": true || don't have dt
    if 'use_current_time' in payload:
        return datetime.fromtimestamp(timestamp / 1000).strftime("%Y%m%d%H%M%S")

    if 'dt' in payload:
        return payload['dt']

    return datetime.fromtimestamp(timestamp / 1000).strftime("%Y%m%d%H%M%S")

def process_data_batch(producer, topic_parts, value, timestamp):
    account_id = topic_parts[1]
    payloads = get_payloads(value)
    for payload in payloads: 
        ## TODO: check payload time sequence
        serial_id = payload.get('serial_id')

        if serial_id is None:
            continue

        dt = get_dt(payload, timestamp)
        data = {
            'account_id': account_id,
            'serial_id': serial_id,
            'dt': dt,
            'payload': payload
        }

        data['type'] = 'telemetry'
        if is_uc1122(serial_id):
            data['type'] = 'uc1122-telemetry'
            if is_event(payload):
                data['type'] = 'uc1122-event'

        elif is_uc1152(serial_id):
            data['type'] = 'uc1152-telemetry'

        elif is_em300(serial_id):
            data['type'] = 'em300-telemetry'

        elif is_am102(serial_id):
            data['type'] = 'am102-telemetry'

        elif is_uc50x(serial_id):
            data['type'] = 'uc50x-telemetry'

        elif is_ra0716y(serial_id):
            data['type'] = 'ra0716y-telemetry'

        record_key = "{}-{}".format(account_id, serial_id)
        producer.produce('things', key=record_key, value=json.dumps(data), on_delivery=acked)
        producer.poll(0)
