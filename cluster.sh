#!/bin/bash

cleanup() {
  kill `jobs -p` 2>/dev/null
}
trap "cleanup; exit 0" INT TERM EXIT

if [[ $1 == '-d' ]]; then
	rm -rf data
fi

for pid in $(netstat -vanp tcp | grep -E '1234|1235|1236' | awk '{print $9}'); do
  kill -9 $pid >/dev/null 2>&1
done

for f in 1234 1235; do
	python3 -u server.py "$f" | awk '{ print "'"$f"': "$0 }' &
done

python3 -u seed.py | awk '{ print "seed: "$0 }' &

wait
cleanup
