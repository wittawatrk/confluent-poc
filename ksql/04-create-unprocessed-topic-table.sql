CREATE TABLE UNPROCESSED_TOPIC_TABLE (VALUE STRING PRIMARY KEY) WITH (KAFKA_TOPIC='unprocessed_topic', VALUE_FORMAT='JSON', PARTITIONS=1);
