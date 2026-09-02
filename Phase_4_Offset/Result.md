# # Phase 4 — Offsets
## What we did
Continue from phase 3  
Dont stop or clean anything  
We have three partition  
Send several payments rapidly via `/pay` and watch both consumer consoles — messages should be split between them (not all going to one), since each message's partition is determined by its key's hash.

## Output
### Case 1 : Consumers are running
Messages are splitting into consumer 1 and consumer 2
Also if you check the kafka status 

````
C:\kafka_2.13-4.3.1>.\bin\windows\kafka-consumer-groups.bat --describe --group notification-service --bootstrap-server localhost:9092
2026-09-02T05:21:06.050084100Z main ERROR Reconfiguration failed: No configuration found for '266474c2' at 'null' in 'null'

GROUP                TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                                          HOST            CLIENT-ID
notification-service payments        0          3               3               0               consumer-notification-service-1-8152e332-8c66-4f28-831f-ae29167c2600 /127.0.0.1      consumer-notification-service-1
notification-service payments        1          50              50              0               consumer-notification-service-1-8152e332-8c66-4f28-831f-ae29167c2600 /127.0.0.1      consumer-notification-service-1
notification-service payments        2          3               3               0               consumer-notification-service-1-8405b472-4581-4182-b634-afcdfedd98d0 /127.0.0.1      consumer-notification-service-1
````
````
PARTITION	Which partition this row is about
CURRENT-OFFSET	How far the notification-service group has read on this partition — 50 messages read
LOG-END-OFFSET	Total messages that exist in this partition — also 50
LAG	LOG-END-OFFSET - CURRENT-OFFSET — messages that exist but haven't been read yet. 0 means fully caught up.
````

---
### Case 2 : Consumers are stopped

Stop all consumer service  
In this case hit the pay api multiple times so that message get stored.  
As no consumer will read this message, so there will be difference in between read message index and lastly inserted message index
This is shown as Lag
````
C:\kafka_2.13-4.3.1>.\bin\windows\kafka-consumer-groups.bat --describe --group notification-service --bootstrap-server localhost:9092
2026-09-02T05:34:24.325339900Z main ERROR Reconfiguration failed: No configuration found for '266474c2' at 'null' in 'null'

Consumer group 'notification-service' has no active members.

GROUP                TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
notification-service payments        0          3               11              8               -               -               -
notification-service payments        1          50              60              10              -               -               -
notification-service payments        2          3               13              10              -               -               -
````

