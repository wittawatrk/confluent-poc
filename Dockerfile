FROM 659821968821.dkr.ecr.ap-southeast-1.amazonaws.com/amd64/python:3.7-slim
# FROM amd64/python:3.7-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -U -r /tmp/requirements.txt

COPY librdkafka.config /root/.confluent/librdkafka.config

RUN mkdir /var/app
COPY ./client/*.py ./var/app/

CMD /var/app/client.py -f /root/.confluent/librdkafka.config -t aws-mqtt