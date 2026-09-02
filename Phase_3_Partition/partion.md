**Full sequence for this clean run:**

1. **Stop Kafka** (Ctrl+C in the broker terminal)
2. **Delete the data folder:**

```
rmdir /s /q C:\tmp\kraft-combined-logs
```

1. **Regenerate cluster ID:**

```
cd C:\kafka_2.13-4.3.1
.\bin\windows\kafka-storage.bat random-uuid
```

1. **Format storage** (using the new UUID):

```
.\bin\windows\kafka-storage.bat format --standalone -t <uuid> -c .\config\server.properties
```

1. **Start Kafka:**

```
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

1. **Create the `payments` topic directly with 3 partitions** (skip the old 1-partition version entirely):

```
.\bin\windows\kafka-topics.bat --create --topic payments --partitions 3 --bootstrap-server localhost:9092
```

1. **Verify:**

```
.\bin\windows\kafka-topics.bat --describe --topic payments --bootstrap-server localhost:9092
```

Go through these in order and paste the `--describe` output at the end — we should see `PartitionCount: 3`.