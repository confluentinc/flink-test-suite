#!/bin/bash

set -xe

python3.11 -m venv .venv
source .venv/bin/activate
pip install apache-flink==2.1.1