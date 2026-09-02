# Phase 2 — Consumer Group Scaling

## What we did
Ran **two instances** of the same consumer service (same code, same `group-id=notification-service`, different ports: 8082 and 8083). Producer was untouched. The `payments` topic had only **1 partition**.

## How to create a second instance in IntelliJ

1. Go to **Run → Edit Configurations**
2. Select the existing consumer run configuration
3. Click the **copy/duplicate icon** to clone it
4. In the duplicated config, rename it (e.g. `consumer-2`) and add to **Program arguments**:
   ```
   --server.port=8083
   ```
5. Run the original config (port 8082) and the duplicated config (port 8083) **at the same time** — both will appear as separate tabs in the Run window, each with its own console output.

## Step 1 — Start both consumer instances

Both instances start up, connect to the broker, and join the **same** consumer group (`notification-service`). Kafka runs a rebalance and decides how to split the topic's partitions between them.

Since there's only **1 partition** (`payments-0`), Kafka can only hand it to one of the two instances:

**Consumer 1** (port 8082):
```
partitions assigned: [payments-0]
```
Got the only partition — this instance will do all the work (process every message).

**Consumer 2** (port 8083):
```
partitions assigned: []
```
Joined the group successfully, but got an **empty assignment** — no partition to read from. It's connected and idle, doing nothing, purely a standby.

At this point, if you send a payment via `/pay`, only **Consumer 1's** console will print the SMS line. Consumer 2 stays silent.

## Step 2 — Stop Consumer 1

Stop the running instance on port 8082 (red stop button in IntelliJ, or Ctrl+C if run from terminal).

Kafka detects Consumer 1 is gone (missed heartbeats), triggers another rebalance, and reassigns `payments-0` to the only remaining member — Consumer 2.

**Consumer 2** immediately picks it up:
```
Setting offset for partition payments-0 to the committed offset ... offset=1
partitions assigned: [payments-0]
```

Send a new payment via `/pay` now — Consumer 2, which did nothing before, will print the SMS line. It has taken over completely.

## Key takeaways

- A partition is assigned to **exactly one consumer** within a group at a time.
- Extra consumer instances beyond the partition count sit **idle as standby** — not broken, just no work available.
- If the active consumer dies, Kafka **automatically reassigns** its partition(s) to a standby — no code needed.
- The new consumer resumes from the **last committed offset** (not from scratch) — no duplicate processing, no missed messages.

## Why it matters
This is how Kafka gives you **failover for free**. With 1 partition, you get redundancy but no parallelism. To get real parallel processing across consumer instances, you need **more partitions** — which is Phase 3.