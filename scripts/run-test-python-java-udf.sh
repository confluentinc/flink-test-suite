#!/bin/bash

set -xe


TEST_DIR=./examples/python-java-udf && \
mvn test -Dtest=FlinkKafkaFileBasedTest \
  -DsqlFile=$TEST_DIR/query.sql \
  -DinputFile=$TEST_DIR/input.txt \
  -DoutputFile=$TEST_DIR/output.txt \
  -DjavaJars=$TEST_DIR/../udf/java/udf-example-2.1.0-1.0.jar \
  -DpythonPath=$TEST_DIR/../udf/python
