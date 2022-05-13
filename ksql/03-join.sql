CREATE TABLE TRANSFORMERS (SERIAL_ID STRING PRIMARY KEY, ACCOUNT_ID INTEGER, CONFIG STRING)
WITH (KAFKA_TOPIC='transformers', KEY_FORMAT='KAFKA', PARTITIONS=3, VALUE_FORMAT='JSON');

--  {
-- 	"account_id":"180","serial_id":"1800004","config":"{}",
-- }


CREATE STREAM THING_TRANSFORMS WITH (KAFKA_TOPIC='thing-transforms', PARTITIONS=3, REPLICAS=3)
AS SELECT
  T.SERIAL_ID SERIAL_ID,
  T.ACCOUNT_ID ACCOUNT_ID,
  T.TYPE TYPE,
  T.DT DT,
  T.PAYLOAD PAYLOAD,
  TF.CONFIG CONFIG
FROM THINGS T
LEFT JOIN TRANSFORMERS TF ON T.SERIAL_ID = TF.ID
EMIT CHANGES;

SELECT
  T.SERIAL_ID SERIAL_ID,
  T.ACCOUNT_ID ACCOUNT_ID,
  T.TYPE TYPE,
  T.DT DT,
  T.PAYLOAD PAYLOAD,
  TF.CONFIG CONFIG
FROM THINGS T
LEFT JOIN TRANSFORMERS TF ON T.SERIAL_ID = TF.ID
where T.serial_id = '1370010'
EMIT CHANGES;

select * from THING_TRANSFORMS where serial_id = '1770011' EMIT CHANGES;

set 'auto.offset.reset' = 'earliest';