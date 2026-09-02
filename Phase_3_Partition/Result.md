# Phase 3 — Partitions

## What we did
Started from a clean broker, and this time created the `payments` topic directly with **3 partitions** (instead of the default 1), then ran both consumer instances (from Phase 2) against it.

## Setup commands

**1. Start Kafka:**
```
cd C:\kafka_2.13-4.3.1
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

**2. Create the `payments` topic directly with 3 partitions** (skip the old 1-partition version entirely):
```
.\bin\windows\kafka-topics.bat --create --topic payments --partitions 3 --bootstrap-server localhost:9092
```

**3. Verify:**
```
.\bin\windows\kafka-topics.bat --describe --topic payments --bootstrap-server localhost:9092
```
Should show `PartitionCount: 3`.

---

## What we observed — the rebalance lifecycle

### Step 1 — Consumer 1 starts first, alone
It joins the group, and since it's the only member, Kafka gives it **all 3 partitions**:
```
(implied): partitions assigned: [payments-0, payments-1, payments-2]
```

### Step 2 — Consumer 2 starts, joins the same group
This changes group membership, so Kafka must **rebalance** — and rebalancing always starts by **revoking** partitions from everyone first:
```
Consumer 1: partitions revoked: [payments-0, payments-1, payments-2]
```
Important: Consumer 1 briefly loses **all** its partitions, not just some. Kafka doesn't do a smooth handoff — it stops everyone, recalculates the whole assignment from scratch, then reassigns.

### Step 3 — New assignment is calculated and handed out
```
Consumer 2: partitions assigned: [payments-2]
Consumer 1: partitions assigned: [payments-0, payments-1]
```
Final split: Consumer 1 gets 2 partitions, Consumer 2 gets 1 partition. With 3 partitions and 2 consumers, this is the most even split possible (2-and-1, since 3 doesn't divide evenly by 2).

---

## What this proves

- One consumer **can** hold multiple partitions (Consumer 1 held 2 at once).
- Every membership change (a consumer joining or leaving) triggers a **full rebalance** — brief pause, revoke-then-reassign — not an incremental adjustment.
- Kafka tries to distribute partitions as evenly as possible across active consumers in the group.
- Both consumers are now genuinely doing real work in parallel — the actual scaling benefit that the 1-partition setup (Phase 2) couldn't provide.

## Next check
Send several payments rapidly via `/pay` and watch both consumer consoles — messages should be split between them (not all going to one), since each message's partition is determined by its key's hash.