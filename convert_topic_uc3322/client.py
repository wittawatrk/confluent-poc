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

import base64
from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaError
import json
import ccloud_lib

def getConsumer(conf):
    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_convert_topic'
    consumer_conf['auto.offset.reset'] = 'latest'
    
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

serial_mapper = {
    '1234567890AB': {
        'account_id': 1, 'serial_id': 10022, 'env': ['stag']
    },
    '1122334455AB': {
        'account_id': 1, 'serial_id': 10033, 'env': ['stag']
    },
    '6113A2735912': {
        'account_id': 32, 'serial_id': 320010, 'env': ['prod']
    },
    '6123A1585219': {
        'account_id': 133, 'serial_id': 1330102, 'env': ['prod']
    },
    '6113A3982132': {
        'account_id': 160, 'serial_id': 1600036, 'env': ['prod']
    },
    '6113A3915819': {
        'account_id': 160, 'serial_id': 1600037, 'env': ['prod']
    },
    '6113A3927571': {
        'account_id': 160, 'serial_id': 1600038, 'env': ['prod']
    },
    '6113A3909329': {
        'account_id': 160, 'serial_id': 1600039, 'env': ['prod']
    },
    '6113A3980174': {
        'account_id': 160, 'serial_id': 1600040, 'env': ['prod']
    },
    '6113A3967810': {
        'account_id': 160, 'serial_id': 1600041, 'env': ['prod']
    },
    '6113A3906342': {
        'account_id': 160, 'serial_id': 1600042, 'env': ['prod']
    },
    '6113A3916130': {
        'account_id': 160, 'serial_id': 1600043, 'env': ['prod']
    },
    '6113A3912142': {
        'account_id': 160, 'serial_id': 1600044, 'env': ['prod']
    },
    '6113A3924988': {
        'account_id': 160, 'serial_id': 1600045, 'env': ['prod']
    },
    '6113A3919857': {
        'account_id': 160, 'serial_id': 1600046, 'env': ['prod']
    },
    '6123A4955688': {
        'account_id': 164, 'serial_id': 1640001, 'env': ['prod'], 'type': 'UC3452'
    },
    '6123A4995796': {
        'account_id': 164, 'serial_id': 1640003, 'env': ['prod'], 'type': 'UC3452'
    },
    '6123A4953933': {
        'account_id': 164, 'serial_id': 1640007, 'env': ['prod'], 'type': 'UC3452'
    },
    '6113A3993564': {
        'account_id': 164, 'serial_id': 1640014, 'env': ['prod']
    },
    '6123A4978097': {
        'account_id': 3, 'serial_id': 30003, 'env': ['prod']
    },
    '6123A4959094': {
        'account_id': 3, 'serial_id': 30005, 'env': ['prod']
    },
    '6123A4944483': {
        'account_id': 3, 'serial_id': 30007, 'env': ['prod']
    },
    '6113B2103166': {
        'account_id': 160, 'serial_id': 1600126, 'env': ['prod']
    },
    '6113B2179788': {
        'account_id': 160, 'serial_id': 1600128, 'env': ['prod']
    },
    '6113B2110048': {
        'account_id': 160, 'serial_id': 1600130, 'env': ['prod']
    },
    '6113B2104779': {
        'account_id': 160, 'serial_id': 1600132, 'env': ['prod']
    },
    '6123A4998585': {
        'account_id': 172, 'serial_id': 1720001, 'env': ['prod'], 'type': 'UC3452'
    },
    '6123A4960290': {
        'account_id': 173, 'serial_id': 1730001, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123A4998018': {
        'account_id': 174, 'serial_id': 1740006, 'env': ['prod'], 'type': 'UC3452'
    },
    '6123A4995583': {
        'account_id': 174, 'serial_id': 1740002, 'env': ['prod'], 'type': 'UC3452'
    },
    '6113B2122694': {
        'account_id': 160, 'serial_id': 1600144, 'env': ['prod']
    },
    '6113B2136671': {
        'account_id': 160, 'serial_id': 1600146, 'env': ['prod']
    },
    '6113B1572695': {
        'account_id': 177, 'serial_id': 1770001, 'env': ['prod']
    },
    '6113B2180497': {
        'account_id': 177, 'serial_id': 1770002, 'env': ['prod']
    },
    '6123B0552813': {
        'account_id': 177, 'serial_id': 1770003, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0573704': {
        'account_id': 177, 'serial_id': 1770004, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6113A3959131': {
        'account_id': 177, 'serial_id': 1770005, 'env': ['prod']
    },
    '6123B0594071': {
        'account_id': 177, 'serial_id': 1770006, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0584258': {
        'account_id': 177, 'serial_id': 1770007, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0564304': {
        'account_id': 177, 'serial_id': 1770008, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0534563': {
        'account_id': 177, 'serial_id': 1770009, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0573083': {
        'account_id': 177, 'serial_id': 1770010, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6113B2107290': {
        'account_id': 177, 'serial_id': 1770011, 'env': ['prod']
    },
    '6123B0589957': {
        'account_id': 180, 'serial_id': 1800001, 'env': ['prod']
    },
    '6123B0553065': {
        'account_id': 180, 'serial_id': 1800002, 'env': ['prod']
    },
    '6123B0593874': {
        'account_id': 180, 'serial_id': 1800003, 'env': ['prod']
    },
    '6123B0532020': {
        'account_id': 180, 'serial_id': 1800004, 'env': ['prod']
    },
    '6123B0564691': {
        'account_id': 63, 'serial_id': 630023, 'env': ['prod'], 'type': 'UC3452'
    },
    '6123B0559248': {
        'account_id': 63, 'serial_id': 630025, 'env': ['prod'], 'type': 'UC3452'
    },
    '6123B0546886': {
        'account_id': 63, 'serial_id': 630027, 'env': ['prod'], 'type': 'UC3452'
    },
    '6123B0559678': {
        'account_id': 63, 'serial_id': 630029, 'env': ['prod'], 'type': 'UC3452'
    },
    '6123B0551032': {
        'account_id': 181, 'serial_id': 1810001, 'env': ['prod']
    },
    '6123B0595901': {
        'account_id': 181, 'serial_id': 1810003, 'env': ['prod']
    },
    'tester_sn': {
        'account_id': 1, 'serial_id': 10046, 'env': ['prod', 'stag'], 'type': 'UC3452'
    },
    '6123A4955429': {
        'account_id': 133, 'serial_id': 1330102, 'env': ['prod', 'stag'], 'type': 'UC3352_v2'
    },
    '6123B0570975': {
        'account_id': 182, 'serial_id': 1820001, 'env': ['prod']
    },
    '6123B0583741': {
        'account_id': 182, 'serial_id': 1820002, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0588823': {
        'account_id': 182, 'serial_id': 1820004, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0534629': {
        'account_id': 182, 'serial_id': 1820006, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0547606': {
        'account_id': 183, 'serial_id': 1830001, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0572869': {
        'account_id': 183, 'serial_id': 1830003, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0580447': {
        'account_id': 183, 'serial_id': 1830005, 'env': ['prod']
    },
    '6123B0580439': {
        'account_id': 183, 'serial_id': 1830008, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0560055': {
        'account_id': 183, 'serial_id': 1830010, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0575769': {
        'account_id': 183, 'serial_id': 1830012, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0517819': {
        'account_id': 65, 'serial_id': 650001, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0510876': {
        'account_id': 65, 'serial_id': 650003, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0534748': {
        'account_id': 65, 'serial_id': 650005, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0552511': {
        'account_id': 65, 'serial_id': 650007, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0554623': {
        'account_id': 65, 'serial_id': 650009, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0519285': {
        'account_id': 65, 'serial_id': 650011, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0514273': {
        'account_id': 65, 'serial_id': 650013, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0548528': {
        'account_id': 65, 'serial_id': 650015, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123A4958953': {
        'account_id': 133, 'serial_id': 1330112, 'env': ['prod', 'stag'], 'type': 'UC3352_v2'
    },
    '6123B0564407': {
        'account_id': 133, 'serial_id': 1330113, 'env': ['prod', 'stag']
    },
    '6123B0557892': {
        'account_id': 92, 'serial_id': 920001, 'env': ['prod', 'stag']
    },
    '6123A4929445': {
        'account_id': 92, 'serial_id': 920003, 'env': ['prod', 'stag']
    },
    '6123B0515222': {
        'account_id': 92, 'serial_id': 920005, 'env': ['prod', 'stag']
    },
    '6123B0510690': {
        'account_id': 184, 'serial_id': 1840001, 'env': ['prod', 'stag'], 'type': 'UC3352_v2'
    },
    '6123B0517504': {
        'account_id': 185, 'serial_id': 1850001, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0595091': {
        'account_id': 185, 'serial_id': 1850003, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0551393': {
        'account_id': 32, 'serial_id': 320012, 'env': ['prod'], 'type': 'UC3352_v2'
    },
    '6123B0593382': {
        'account_id': 184, 'serial_id': 1840067, 'env': ['prod', 'stag'], 'type': 'UC3352_v2'
    },
    '6123B0541490': {
        'account_id': 184, 'serial_id': 1840070, 'env': ['prod', 'stag'], 'type': 'UC3352_v2'
    },
    '6123B0553930': {
        'account_id': 184, 'serial_id': 1840073, 'env': ['prod', 'stag'], 'type': 'UC3352_v2'
    },
    '6123B0555478': {
        'account_id': 186, 'serial_id': 1860001, 'env': ['prod'], 'type': 'UC3352_v2'
    }
}

def process():
    return None
   
if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    producer = getProducer(conf)
    
    telemetry_topic = 'telemetry'
    ccloud_lib.create_topic(conf, telemetry_topic)
    raw_event_topic = 'raw_event'
    ccloud_lib.create_topic(conf, raw_event_topic)
    
    consumer = getConsumer(conf)
    consumer.subscribe([topic])

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
                record_value = msg.value()

                key_str = record_key.decode('utf-8')
                keys = key_str.split('/')

                if  len(keys) == 5 and keys[0] == 'uc' and keys[-1] in ['msg', 'alarm', 'status']:
                    serial_number = keys[1]
                    topic_type = keys[4]
                    if (serial_number not in serial_mapper):
                        continue
                    selector = serial_mapper[serial_number]
                    if ('prod' not in selector['env']): # env
                        continue

                    payload = json.dumps({
                        'data' : record_value.hex(), # disabled encode base64
                        'account_id': selector['account_id'],
                        'serial_id': selector['serial_id'],
                        'dt': datetime.now().strftime("%Y%m%d%H%M%S")
                    })

                    if (topic_type in ['alarm', 'msg']) and ('type' in  selector) and (selector['type'] in ['UC3452', 'UC3352_v2']):
                        produce_topic = raw_event_topic
                    else:
                        produce_topic = telemetry_topic
                    
                    producer.produce(produce_topic, key=record_key, value=payload, on_delivery=acked)
                    producer.poll(0)
                continue
                
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        consumer.close()
