package com.manaswi.settle.payments;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {

    public record Result(Payment payment, boolean created) {}

    private final JdbcClient jdbc;
    private final ObjectMapper json;
    private final String topic;

    public PaymentService(JdbcClient jdbc, ObjectMapper json, @Value("${settle.topic}") String topic) {
        this.jdbc = jdbc;
        this.json = json;
        this.topic = topic;
    }

    /**
     * Saves the payment and its outbox event in one transaction, so either both exist or neither does.
     * A repeated key returns the first payment. Two requests racing with the same key are settled by
     * the unique index: "on conflict do nothing" waits for the other transaction, then we read its row.
     */
    @Transactional
    public Result create(String idempotencyKey, PaymentRequest req) {
        String hash = RequestHash.of(req);
        UUID id = UUID.randomUUID();
        Optional<UUID> inserted = jdbc.sql("""
                insert into payments (id, idempotency_key, request_hash, from_account, to_account, amount_cents, currency)
                values (:id, :key, :hash, :from, :to, :amount, :currency)
                on conflict (idempotency_key) do nothing
                returning id""")
                .param("id", id).param("key", idempotencyKey).param("hash", hash)
                .param("from", req.fromAccount()).param("to", req.toAccount())
                .param("amount", req.amountCents()).param("currency", req.currency())
                .query(UUID.class).optional();

        if (inserted.isEmpty()) {
            String existingHash = jdbc.sql("select request_hash from payments where idempotency_key = :key")
                    .param("key", idempotencyKey).query(String.class).single();
            if (!existingHash.equals(hash)) {
                throw new IdempotencyConflictException(idempotencyKey);
            }
            return new Result(findByKey(idempotencyKey), false);
        }

        PaymentEvent event = new PaymentEvent(UUID.randomUUID(), id, req.fromAccount(), req.toAccount(),
                req.amountCents(), req.currency());
        jdbc.sql("insert into outbox (event_id, topic, event_key, payload) values (:eventId, :topic, :key, :payload)")
                .param("eventId", event.eventId()).param("topic", topic)
                .param("key", id.toString()).param("payload", toJson(event))
                .update();
        return new Result(findByKey(idempotencyKey), true);
    }

    public Payment findByKey(String key) {
        return jdbc.sql("""
                select id, from_account, to_account, amount_cents, currency, created_at
                from payments where idempotency_key = :key""")
                .param("key", key)
                .query((rs, n) -> new Payment(rs.getObject("id", UUID.class), rs.getString("from_account"),
                        rs.getString("to_account"), rs.getLong("amount_cents"), rs.getString("currency"),
                        rs.getObject("created_at", java.time.OffsetDateTime.class)))
                .single();
    }

    private String toJson(PaymentEvent event) {
        try {
            return json.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialise payment event", e);
        }
    }
}
