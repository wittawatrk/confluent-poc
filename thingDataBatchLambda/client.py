#!/usr/bin/env python3

from base64 import decode
from confluent_kafka import KafkaError
import json
import helper

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        pass

def get_payloads(record_value):
    payloads = json.loads(record_value.decode('utf-8'))
    
    if type(payloads) is list:
        return map(add_serial, payloads)
         
    tmp = [] 
    tmp.append(payloads.copy())

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

        tmp[serial_id].append(payload.copy())

    return tmp

def to_json(payload):
    return json.dumps(payload)

if __name__ == '__main__':
    args = helper.parse_args()

    config_file = args.config_file
    input_topic = args.input_topic
    output_topic = args.output_topic

    conf = helper.read_config(config_file)
    producer = helper.get_producer(conf) 
    consumer = helper.get_consumer(conf)

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

                if not helper.is_valid_record_key(decoded_record_key, [r'^app/[0-9]+/telemetry$', r'^app/[0-9]+/gateway/[0-9]+/telemetry$']):
                    continue

                account_id = helper.get_account_id(decoded_record_key, [r'^app/(?P<account_id>[0-9]+)/telemetry$', r'^app/(?P<account_id>[0-9]+)/gateway/[0-9]+/telemetry$'])
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
