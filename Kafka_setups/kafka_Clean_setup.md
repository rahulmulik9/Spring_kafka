# Kafka KRaft – Clean Run

Steps to completely reset and restart a standalone Kafka broker running in KRaft mode on Windows.

## 1. Stop Kafka

Press `Ctrl+C` in the broker terminal.

## 2. Delete the Data Folder

```
rmdir /s /q C:\tmp\kraft-combined-logs
```

## 3. Regenerate Cluster ID

```
cd C:\kafka_2.13-4.3.1
.\bin\windows\kafka-storage.bat random-uuid
```

## 4. Format Storage

Replace `<uuid>` with the newly generated UUID from the previous step.

```
.\bin\windows\kafka-storage.bat format --standalone -t <uuid> -c .\config\server.properties
```

## 5. Start Kafka

```
.\bin\windows\kafka-server-start.bat .\config\server.properties
```