select * from THINGS where TYPE = 'uc33-telemetry' EMIT CHANGES;
select EXTRACTJSONFIELD (PAYLOAD, '$.status') AS status from THINGS where TYPE = 'uc33-telemetry' EMIT CHANGES;
