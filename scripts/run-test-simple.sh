#!/bin/bash

set -xe


TEST_DIR=./examples/simple && \
mvn test -Dtest=FlinkKafkaFileBasedTest \
  -DsqlFile=$TEST_DIR/query.sql \
  -DinputFile=$TEST_DIR/input.txt \
  -DoutputFile=$TEST_DIR/output.txt
