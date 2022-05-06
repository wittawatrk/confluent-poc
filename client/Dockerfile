FROM python:3.7-slim
# FROM amd64/python:3.7-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -U -r /tmp/requirements.txt

COPY librdkafka.config /root/.confluent/librdkafka.config

COPY *.py ./

CMD ./client.py -f /root/.confluent/librdkafka.config -t iot
