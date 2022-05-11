#!/usr/bin/env python3

from base64 import decode
from confluent_kafka import Consumer, Producer, KafkaError
import json
import ccloud_lib

def get_consumer(conf):
    conf['group.id'] = 'python_example_group_1'
    conf['auto.offset.reset'] = 'earliest'
    
    return Consumer(conf)
    
def get_producer(conf):
    return Producer(conf)

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        pass

def is_valid_record_key(keys):
    return keys[0] == "app" and keys[2] in ['telemetry', 'gateway'] and keys[-1] == "telemetry"

def get_account_id(keys):
    return keys[1]

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

def group_payloads(payloads):
    tmp = {}
    for payload in payloads:
        serial_id = payload.get('serial_id')
        if serial_id is None:
            continue

        if tmp.get(serial_id) is None:
            tmp[serial_id] = []

        tmp[serial_id].append(payload)

    return tmp

def to_json(payload):
    return json.dumps(payload)

if __name__ == '__main__':
    args = ccloud_lib.parse_args()

    config_file = args.config_file
  
    conf = ccloud_lib.read_ccloud_config(config_file)

    input_topic = args.topic 
    output_topic = 'iot_out_x' 

    ccloud_lib.create_topic(conf, output_topic)

    producer = get_producer(conf) 
    consumer = get_consumer(conf)

    # Subscribe to input topic
    consumer.subscribe([input_topic])
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                record_key = msg.key()

                decoded_record_key = record_key.decode('utf-8')
                keys = decoded_record_key.split('/')

                if not is_valid_record_key(keys):
                    continue

                account_id = get_account_id(keys)
                if account_id is None:
                    continue

                payloads = get_payloads(msg.value())
                grouped_payloads = group_payloads(payloads) 
                try:
                    for serial_id, payloads in grouped_payloads.items():        
                        output_record_key = 'app/{account_id}/device/{serial_id}/telemetry'.format(account_id=account_id, serial_id=serial_id)
                        for payload in payloads:
                            producer.produce(output_topic, key=output_record_key, value=to_json(payload), on_delivery=acked)
                            producer.poll(0)
                except BufferError as bfer:
                    # BufferError: Local: Queue full
                    print(bfer)
                    producer.poll(0.1)
                 
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        # Leave group and commit final offsets
        consumer.close()
