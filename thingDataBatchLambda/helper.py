#!/usr/bin/env python

import re, argparse
from confluent_kafka import Consumer, Producer, KafkaError

def is_valid_record_key(key : str, patterns : list):
    return any(re.match(pattern, key) for pattern in patterns)

def get_account_id(key : str, patterns : list):
    for pattern in patterns:
        x = re.search(pattern, key)
        if x:
            return x.group('account_id')

    return None

def parse_args():
    parser = argparse.ArgumentParser(
             description="Argument Parser")
 
    parser.add_argument('-f',
                          default="./client/librdkafka.config",
                          dest="config_file",
                          help="path to Confluent Cloud configuration file",
                          required=False)
    parser.add_argument('-it', '--in_topic',
                          default="iot",
                          dest="input_topic",
                          help="input topic name",
                          required=False)
    parser.add_argument('-ot', '--out_topic',
                          default="iot_out_x",
                          dest="output_topic",
                          help="output topic name",
                          required=False)
    args = parser.parse_args()

    return args

def read_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()

    return conf

def get_consumer(conf):
    conf['group.id'] = 'python_example_group_1'
    conf['auto.offset.reset'] = 'earliest'
    
    return Consumer(conf)
    
def get_producer(conf):
    return Producer(conf)
