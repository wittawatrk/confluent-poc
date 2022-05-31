FROM 659821968821.dkr.ecr.ap-southeast-1.amazonaws.com/enres/python:3.9-slim

#Install build tools
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
    gcc \
    make \
    python3-dev \
    librdkafka-dev;

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -U -r /tmp/requirements.txt --no-binary=confluent-kafka

COPY librdkafka.config /root/.confluent/librdkafka.config

RUN mkdir -p /var/app/aws-mqtt
RUN mkdir -p /var/app/thing

COPY ./aws-mqtt/*.py ./var/app/aws-mqtt/
COPY ./thing/*.py ./var/app/thing/

CMD /var/app/aws-mqtt/client.py -f /root/.confluent/librdkafka.config -t aws-mqtt
