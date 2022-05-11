#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================
import re
from base64 import decode
from confluent_kafka import Consumer, Producer, KafkaError
import json
import ccloud_lib
import logging

def getConsumer(conf):
    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_1'
    consumer_conf['auto.offset.reset'] = 'earliest'

    return Consumer(consumer_conf)


def getProducer(conf):
    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    return Producer(producer_conf)


delivered_records = 0


def acked(err, msg):
    # global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        pass
        # delivered_records += 1
        # print("Produced record to topic {} partition [{}] @ offset {}"
        #         .format(msg.topic(), msg.partition(), msg.offset()))


def process(record_value):
    record = json.loads(record_value)
    return json.dumps({
        'job': 'App\\Jobs\\EdgeCmdJob',
        'data': {
            'payload': record['payload'],
            'account_id': record['account_id'],
            'serial_id': record['serial_id']
        }
    })


if __name__ == '__main__':
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    producer = getProducer(conf)

    producer_topic = 'edge_job'
    # Create producer topic if needed
    ccloud_lib.create_topic(conf, producer_topic)

    consumer = getConsumer(conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()

                record_value = msg.value()

                key_str = record_key.decode('utf-8')
                topics = key_str.split('/')
                topic_out_key = 'php_job'
                if topics[-1] != 'edge_cmd':
                    continue

                try:
                    result = process(record_value)
                    producer.produce(producer_topic, key=topic_out_key, value=result, on_delivery=acked)
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
