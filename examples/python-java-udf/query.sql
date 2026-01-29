ADD JAR '/opt/flink/udf/udf-example-2.1.0-1.0.jar'
-----
CREATE TEMPORARY FUNCTION py_upper AS 'my_udfs.my_python_udf' LANGUAGE PYTHON;
-----
CREATE FUNCTION AddTwo AS 'com.example.my.AddTwo'
-----
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
SELECT py_upper(name), AddTwo(age)
FROM person_input
WHERE age >= 18