#!/bin/bash

if [[ $1 == '-d' ]]; then
	rm -rf data
fi

netstat -vanp tcp | grep -E '1234|1235|1236' | awk '{print $9}' | xargs sudo kill -9

cleanup() {
  kill `jobs -p` 2>/dev/null
}
trap "cleanup; exit 0" INT TERM EXIT

for f in {0..2}; do
	python3 server.py "$f" &
done

wait
cleanup