#!/bin/bash
set -e
cd /var/app
. .venv/bin/activate
python health.py &
# Give Docker network/DNS time to resolve "postgres" before producer connects
sleep 3
# Run producer first; only after it completes successfully, build file from DB and submit
python producer.py && python submit.py
