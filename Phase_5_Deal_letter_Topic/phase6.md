# Phase 6 — Producer Acks (Reliability)

## What we did
Modified the **producer** service only (consumer untouched). Added `acks=all` to the producer's config to understand and verify Kafka's delivery-guarantee setting.

---

## The core question this phase answers

When the producer calls `kafkaTemplate.send(...)`, how does it know the message was safely saved — and what does "safely saved" actually mean?

A write conceptually goes through these steps:
1. Producer sends message → Leader broker receives it
2. Leader writes it to its own disk
3. Leader replicates it to follower brokers
4. At some point, the producer is told "done"

`acks` controls **at which step** the producer is told "done."

---

## The three `acks` options

| Setting | Producer told "done" when... | Trade-off |
|---|---|---|
| `acks=0` | Immediately, as soon as sent over the network — doesn't even wait for the leader to confirm receipt | Fastest, but a message can silently vanish if the send actually failed |
| `acks=1` (Kafka's default) | The **leader** has written it to its own disk — before replicating to followers | If the leader crashes right after, before replicating, the message is lost even though the producer was told "success" |
| `acks=all` (same as `acks=-1`) | The leader **and all in-sync replicas** have confirmed the write | Slowest, but strongest guarantee — survives even if the leader crashes immediately after |

**Why this matters for a payment system:** with `acks=1`, if the leader broker dies half a second after confirming a write, before replicating, that payment event is gone — silently — even though the producer logged "Payment event sent" and returned success to the user. For banking-type reliability, `acks=all` is the safer choice.

---

## Change made

**Producer's `application.properties`** — added one line:
```properties
spring.kafka.producer.acks=all
```

---

## Verification

Restarted the producer and checked the startup config dump:
```
ProducerConfig values:
    acks = -1
    ...
    enable.idempotence = true
    ...
```

`acks = -1` confirms the setting took effect — Kafka internally represents `all` as `-1` in its config dump; they are the same setting, just two ways of writing it.

### Bonus finding: idempotence

Spring Kafka enables `enable.idempotence = true` by default, without any explicit configuration. This is related to preventing duplicate message delivery on retries (deeper reliability topic, for a later phase).

**Important relationship:** idempotence *requires* `acks=all` to function correctly — Kafka does not allow combining idempotence with a weaker acks setting. So `acks=all` wasn't just an optional safety upgrade here; it's actually required for the idempotent producer Spring Kafka already set up by default.

---

## Known limitation of this test (single broker)

Running only 1 local broker (no replicas) means `acks=1` and `acks=all` behave almost identically in terms of *speed and observable safety* here — there's nothing to replicate to, since the leader is the only copy. The real behavioral and performance difference between `acks=1` and `acks=all` only becomes visible in a multi-broker cluster with actual replicas. This phase covered the **concept and configuration** correctly, even though the local setup can't demonstrate the full effect.

---

## What this proves

- `acks` is a producer-side setting controlling how strong a delivery guarantee you get, at the cost of send latency.
- `acks=all` is the appropriate choice for systems where losing a write silently (like a payment event) is unacceptable.
- Idempotence and `acks=all` work together as part of Kafka's stronger reliability guarantees, and Spring Kafka enables idempotence by default already.