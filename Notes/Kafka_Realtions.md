# Kafka Core Relationships — Cheat Sheet

Four key relationships that are easy to mix up. Each is a separate axis.

---

## 1. Consumer ↔ Partition

**One partition connects to exactly one consumer — but only within the same consumer group.**
**One consumer can hold many partitions.**

- One partition → exactly one consumer (within a group) ✅
- One consumer → can own one or many partitions ✅ (not limited to just one)

Example: with 3 partitions and only 1 consumer instance running in a group, that single consumer owns all 3 partitions. Add a 2nd consumer instance to the same group, and Kafka rebalances — splitting the 3 partitions between the two.

> Note: "connects to one consumer" applies **within the same group only**. A consumer in a *different* group can independently read that same partition too — separate consumer groups don't share or block each other; each gets its own full copy of the data.

---

## 2. Service ↔ Topic

**One service can publish to (or consume from) multiple, independent topics.**

Example: Payment Service could publish to:
- `payments`
- `refunds`
- `disputes`

These are completely separate topics, each with its own partition count and its own consumers. A service isn't limited to just one topic.

---

## 3. Topic ↔ Partition

**One topic can have multiple partitions — this is a property of the topic itself.**

Decided at topic creation (or increased later via `--alter`, never decreased). Unrelated to how many topics a service uses — this is purely about how *one* topic's data is split for parallelism.

```
kafka-topics.bat --create --topic payments --partitions 3 --bootstrap-server localhost:9092
```

---

## 4. Partition ↔ Broker (Replication)

**One partition is *replicated* (copied, not split) across multiple servers — for fault tolerance.**

- The full partition — every message, in order — exists identically on each server holding a copy.
- One broker holds the **leader** copy — the active one producers write to and consumers read from.
- Other brokers hold **follower** copies (replicas) — passive backups, ready to take over if the leader's broker crashes.

> Important distinction: "spread across" does **not** mean the messages get divided up between servers (like sharding). It means full identical **copies** exist on multiple servers. This concept only becomes observable with a multi-broker cluster (replication factor > 1) — not visible in a single-broker local setup.

---

## Quick Summary Table

| Concept | Relationship |
|---|---|
| Partition ↔ Consumer (same group) | 1 partition → 1 consumer, but 1 consumer → many partitions |
| Service ↔ Topic | 1 service → can use many topics |
| Topic ↔ Partition | 1 topic → many partitions (you control the count) |
| Partition ↔ Broker | 1 partition → 1 leader broker + optional replica copies on other brokers |