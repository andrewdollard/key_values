#!/bin/bash

if [[ $1 == '-d' ]]; then
	rm -rf data
fi

cleanup() {
  kill `jobs -p` 2>/dev/null
}
trap "cleanup; exit 0" INT TERM EXIT

for f in {1234..1236}; do
  netstat -vanp tcp | grep -E "$f" | awk '{print $9}' | xargs sudo kill -9
	python3 -u server.py "$f" | awk '{ print "'"$f"': "$0 }' &
done

wait
cleanup
