#!/bin/bash
set -e
set -x

/root/tpch benchmark ballista --host localhost --port 50050 --path /data --format tbl --iterations 1 --debug --expected /data --output /data/results/ "$@"


