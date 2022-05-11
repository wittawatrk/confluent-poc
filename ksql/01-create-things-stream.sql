CREATE STREAM THINGS(account_id INTEGER, serial_id VARCHAR, dt VARCHAR, payload VARCHAR)
WITH (kafka_topic='things', value_format='json');
