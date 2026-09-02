# Phase 5 — Error Handling & Dead Letter Topics (DLT)

## What we did
Modified the **consumer** service only (producer untouched, aside from letting `/pay` accept a custom `amount` for testing). Simulated a real business-logic failure (not a deserialization bug like earlier phases) and configured Kafka to route messages that fail repeatedly into a separate **Dead Letter Topic** instead of silently dropping them.

---

## Code Changes

### 1. `NotificationListener.java` — simulate a failure
```java
@Component
public class NotificationListener {

    @KafkaListener(topics = "payments", groupId = "notification-service")
    public void handlePayment(ConsumerRecord<String, PaymentEvent> record) {
        PaymentEvent event = record.value();

        if (event.getAmount() == 999) {
            throw new RuntimeException("Simulated failure: SMS provider down for order " + event.getOrderId());
        }

        System.out.println("📩 SMS to " + record.key() + ": ₹" + event.getAmount()
                + " paid successfully for order " + event.getOrderId());
    }
}
```
Any payment with `amount = 999` deliberately throws an exception, letting us trigger the failure path on demand.

### 2. `PaymentController.java` (producer) — accept custom amount
```java
@PostMapping("/pay")
public String pay(@RequestParam(defaultValue = "500") double amount) {
    String orderId = UUID.randomUUID().toString();
    PaymentEvent event = new PaymentEvent(orderId, amount, "SUCCESS");
    kafkaTemplate.send("payments", orderId, event);
    return "Payment event sent for order " + orderId + " with amount " + amount;
}
```
- `POST /pay` → normal payment (defaults to 500)
- `POST /pay?amount=999` → triggers the simulated failure

### 3. `KafkaConsumerConfig.java` (new) — retry policy + DLT routing
```java
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ProducerFactory<String, Object> dltProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> dltKafkaTemplate(ProducerFactory<String, Object> dltProducerFactory) {
        return new KafkaTemplate<>(dltProducerFactory);
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> dltKafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dltKafkaTemplate,
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition()));

        FixedBackOff backOff = new FixedBackOff(1000L, 3L);

        return new DefaultErrorHandler(recoverer, backOff);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, PaymentEvent> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, PaymentEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
```

**What each bean does, in plain terms:**
1. `dltProducerFactory` — settings for a producer that can correctly write `PaymentEvent` as JSON (key point: without this, the default fallback serializer is `StringSerializer`, which crashes on a Java object).
2. `dltKafkaTemplate` — the actual usable producer tool, built from those settings.
3. `errorHandler` — the policy: retry 3 times, 1 second apart (`FixedBackOff`), then hand off failed messages to `DeadLetterPublishingRecoverer`, which republishes them to `<topic>.DLT` (`payments` → `payments.DLT`).
4. `kafkaListenerContainerFactory` — wires this custom error handler into `@KafkaListener`, overriding Spring's default (9 retries, 0ms delay, silent drop).

---

## Bug hit along the way: SerializationException on DLT publish

**Symptom:**
```
SerializationException: Can't convert value of class PaymentEvent
to class StringSerializer specified in value.serializer
```

**Cause:** the `KafkaTemplate` used by the recoverer was initially auto-built by Spring using default settings (since the consumer's `application.properties` only has consumer-side config, no producer-side serializers). It fell back to `StringSerializer` for the value, which can't handle a `PaymentEvent` object.

**Fix:** explicitly build a `ProducerFactory`/`KafkaTemplate` with `JsonSerializer` set as the value serializer, specifically for DLT publishing (Beans 1 & 2 above).

> Note: `org.springframework.kafka.support.serializer.JsonSerializer` shows a deprecation warning in Spring Kafka 4.x ("for removal, in favor of `JacksonJsonSerializer`"). It still works correctly — left as-is for now since migrating would require touching every service's serializer config, better done as a dedicated exercise later.

---

## Default Spring Kafka Error Handling (before this phase)

| Setting | Default value |
|---|---|
| Retries | 9 attempts |
| Backoff (delay between retries) | 0ms (instant) |
| After retries exhausted | Log error, skip message (offset committed anyway) — **message effectively lost** |
| Dead Letter Topic | Not configured — no DLT exists unless explicitly added |

---

## Result — verified end to end

Triggered `POST /pay?amount=999` several times. Observed:

1. Listener throws exception for each `amount: 999` message
2. Retried exactly **3 times**, ~1 second apart (matching `FixedBackOff(1000L, 3L)`)
3. No `SerializationException` this time — retries stopped cleanly after the 3rd attempt

**Verified the DLT directly:**
```
.\bin\windows\kafka-console-consumer.bat --topic payments.DLT --from-beginning --bootstrap-server localhost:9092
```
Output:
```json
{"orderId":"f25416c1-c97c-4918-8449-fe8272b31d3d","amount":999.0,"status":"SUCCESS"}
{"orderId":"0ac501ba-ce14-4c13-95bb-6b2f059f7f64","amount":999.0,"status":"SUCCESS"}
{"orderId":"d8f3db8b-8dbb-40d5-84f6-2bb3c571b6f7","amount":999.0,"status":"SUCCESS"}
```

All three failed messages preserved intact in `payments.DLT` — none processed by `NotificationListener` (no SMS printed), none lost.

> Note: `status: "SUCCESS"` in the JSON refers to the *original payment* succeeding at the producer side — the failure happened only in *this consumer's* notification processing, not the payment itself. A realistic scenario: payment went through, but the SMS/notification pipeline had a problem.

---

## What this proves

- A single bad/failing message no longer blocks the entire partition forever or gets silently dropped.
- Failed messages are preserved in a separate, inspectable topic (`payments.DLT`) for later investigation or reprocessing.
- Retry behavior (count + delay) is fully customizable via `FixedBackOff`, instead of relying on Kafka's aggressive default (9 instant retries).
- This is the mechanism that makes Kafka-based systems resilient for something like a payment pipeline — failures are visible and recoverable, not silent data loss.