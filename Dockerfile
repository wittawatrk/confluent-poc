FROM arm64v8/python:3.9-slim

#Install build tools
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
    gcc \
    make \
    python3-dev \
    librdkafka-dev;

#Build confluent-kafka with --no-binary option to disable python wheel
RUN pip3 install "confluent-kafka[avro,json,protobuf]>=1.4.2" --no-binary confluent-kafka

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -U -r /tmp/requirements.txt

COPY librdkafka.config /root/.confluent/librdkafka.config

RUN mkdir -p /var/app/aws-mqtt
RUN mkdir -p /var/app/thing

COPY ./aws-mqtt/*.py ./var/app/aws-mqtt/
COPY ./thing/*.py ./var/app/thing/

CMD /var/app/aws-mqtt/client.py -f /root/.confluent/librdkafka.config -t aws-mqtt
