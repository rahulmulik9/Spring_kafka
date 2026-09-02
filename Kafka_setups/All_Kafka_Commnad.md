# Kafka CLI Commands — Quick Reference

All commands run from:
```
cd C:\kafka_2.13-4.3.1
```

---

## GROUP 1 — Broker Setup & Lifecycle (Start / Stop / Initialize)

**One-time setup (do NOT repeat these once done):**

Generate cluster ID:
```
.\bin\windows\kafka-storage.bat random-uuid
```

Format storage (using the generated UUID):
```
.\bin\windows\kafka-storage.bat format --standalone -t <uuid> -c .\config\server.properties
```

**Start Kafka:**
```
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

**Stop Kafka:**
Press `Ctrl + C` in the terminal where the broker is running, then confirm with `Y` if prompted ("Terminate batch job?"). Closing the terminal window also stops it.

> For a complete wipe-and-restart sequence, see **Group 6 — Full Clean Reset** below.

---

## GROUP 2 — Topics & Partitions

**Create a topic with 3 partitions:**
```
.\bin\windows\kafka-topics.bat --create --topic payments --partitions 3 --bootstrap-server localhost:9092
```

**Increase partitions on an existing topic** (can only increase, never decrease):
```
.\bin\windows\kafka-topics.bat --alter --topic payments --partitions 3 --bootstrap-server localhost:9092
```

**Check partition status / count (describe topic):**
```
.\bin\windows\kafka-topics.bat --describe --topic payments --bootstrap-server localhost:9092
```
Shows `PartitionCount`, replication factor, and leader/replica/ISR info per partition.

**List all topics:**
```
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

**Delete a topic:**
```
.\bin\windows\kafka-topics.bat --delete --topic payments --bootstrap-server localhost:9092
```

---

## GROUP 3 — Consumer Groups, Offsets & Lag

**Check consumer group offsets / lag:**
```
.\bin\windows\kafka-consumer-groups.bat --describe --group notification-service --bootstrap-server localhost:9092
```
Shows per partition: `CURRENT-OFFSET`, `LOG-END-OFFSET`, `LAG`, and which consumer instance owns it.

Formula: `LAG = LOG-END-OFFSET - CURRENT-OFFSET`

**List all consumer groups:**
```
.\bin\windows\kafka-consumer-groups.bat --list --bootstrap-server localhost:9092
```

---

## GROUP 4 — Manual Producer / Consumer (quick testing, not JSON-aware)

**Console producer** (type messages manually):
```
.\bin\windows\kafka-console-producer.bat --topic payments --bootstrap-server localhost:9092
```

**Console consumer** (read messages from the beginning):
```
.\bin\windows\kafka-console-consumer.bat --topic payments --from-beginning --bootstrap-server localhost:9092
```

**Console consumer for the Dead Letter Topic:**
```
.\bin\windows\kafka-console-consumer.bat --topic payments.DLT --from-beginning --bootstrap-server localhost:9092
```

---


## GROUP 5 — Full Clean Reset (complete sequence)

Use this when you want to wipe everything and start completely fresh — no topics, no messages, no offsets, no consumer groups.

**1. Stop Kafka:**
Press `Ctrl + C` in the broker terminal.

**2. Delete all Kafka data:**
```
rmdir /s /q C:\tmp\kraft-combined-logs
```

**3. Regenerate cluster ID:**
```
cd C:\kafka_2.13-4.3.1
.\bin\windows\kafka-storage.bat random-uuid
```

**4. Format storage (using the new UUID from step 3):**
```
.\bin\windows\kafka-storage.bat format --standalone -t <uuid> -c .\config\server.properties
```

**5. Start Kafka:**
```
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

**6. Recreate the topic with 3 partitions:**
```
.\bin\windows\kafka-topics.bat --create --topic payments --partitions 3 --bootstrap-server localhost:9092
```

**7. Verify:**
```
.\bin\windows\kafka-topics.bat --describe --topic payments --bootstrap-server localhost:9092
```

**8.** Restart both Spring Boot services (producer + consumer) after this — old offsets/groups no longer exist.