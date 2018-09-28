#!/bin/bash

cleanup() {
  kill `jobs -p` 2>/dev/null
}
trap "cleanup; exit 0" INT TERM EXIT

if [[ $1 == '-d' ]]; then
	rm -rf data
fi

lsof -nP -i4TCP:1234,1235,1236,1237 | awk 'NR!=1 {print $2}' | xargs kill

for f in 1234 1235 1236 1237; do
	python3 -u server.py "$f" | awk '{ print "'"$f"': "$0 }' &
done

python3 -u seed.py | awk '{ print "seed: "$0 }' &

wait
cleanup
