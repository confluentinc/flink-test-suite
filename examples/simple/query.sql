CREATE TABLE person_input (
  name STRING,
  age INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'person-input-file2',
  'properties.bootstrap.servers' = 'kafka.svc.cluster.local:9092',
  'properties.security.protocol' = 'SSL',
  'properties.group.id' = 'flink-test-group-file',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
)
-----
CREATE TABLE person_output (
  name STRING,
  age INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'person-output-file2',
  'properties.bootstrap.servers' = 'kafka.svc.cluster.local:9092',
  'properties.security.protocol' = 'SSL',
  'format' = 'json'
)
-----
INSERT INTO person_output
SELECT name, age
FROM person_input
WHERE age >= 18