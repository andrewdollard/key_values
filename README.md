# Distributed KV Store project

## Use case

Global key-value store
Multi-datacenter
Edge storage

## Design goals

Backward- and forward-compatibility
High resilience
Redundancy: multiple replicas of all KVs
Arbitrarily sized keys and values

Speed

Massively scalable


## Serialization format

commands:
  GET, SET

1 byte: command
20 byte: key
2 bytes: delimiter
n bytes: value
2 bytes: delimiter


## Replication issues

How does replica know it's now leader?
How does revived dead leader know it's now a replica?

How does a leader-turned-follower request an update?
  starts up
  connects to replica, if set
  asks replica for all updates beyond last known LSN

