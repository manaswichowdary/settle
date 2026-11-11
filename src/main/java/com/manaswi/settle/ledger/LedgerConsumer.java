package com.manaswi.settle.ledger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.manaswi.settle.payments.PaymentEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Reads payment events. The offset is committed only after apply() returns, so a crash before
 * that means Kafka redelivers the event, and LedgerService skips it if it was already applied.
 */
@Component
@ConditionalOnProperty(name = "settle.consumer.enabled", havingValue = "true", matchIfMissing = true)
public class LedgerConsumer {

    private final LedgerService ledger;
    private final ObjectMapper json;

    public LedgerConsumer(LedgerService ledger, ObjectMapper json) {
        this.ledger = ledger;
        this.json = json;
    }

    @KafkaListener(topics = "${settle.topic}", groupId = "ledger", concurrency = "${settle.consumer.concurrency:2}")
    public void onEvent(ConsumerRecord<String, String> record) throws JsonProcessingException {
        ledger.apply(json.readValue(record.value(), PaymentEvent.class));
    }
}
