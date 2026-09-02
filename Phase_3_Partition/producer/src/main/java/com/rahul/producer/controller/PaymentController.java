package com.rahul.producer.controller;

import java.util.UUID;
import com.rahul.producer.model.PaymentEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PaymentController {

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    public PaymentController(KafkaTemplate<String, PaymentEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/pay")
    public String pay() {
        String orderId = UUID.randomUUID().toString();
        PaymentEvent event = new PaymentEvent(orderId, 500, "SUCCESS");
        kafkaTemplate.send("payments", orderId, event);
        return "Payment event sent for order " + orderId;
    }
}