import json
import pytz
from datetime import datetime

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

        dt = get_dt(payload, timestamp)
        data = {
            'account_id': account_id,
            'serial_id': serial_id,
            'dt': dt,
            'payload': payload
        }
        data['type'] = 'telemetry' ## uc11-telemetry, uc11-event

        record_key = "{}-{}".format(account_id, serial_id)
        producer.produce('things', key=record_key, value=json.dumps(data), on_delivery=acked)
        producer.poll(0)
