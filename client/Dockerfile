FROM python:3.7-slim
# FROM amd64/python:3.7-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -U -r /tmp/requirements.txt

COPY librdkafka.config /root/.confluent/librdkafka.config

RUN mkdir /var/app
COPY *.py ./var/app/

CMD /var/app/client.py -f /root/.confluent/librdkafka.config -t iot
