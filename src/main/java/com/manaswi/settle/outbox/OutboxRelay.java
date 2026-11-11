package com.manaswi.settle.outbox;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Publishes outbox rows to Kafka, oldest first. Rows are marked published only after Kafka
 * confirms them. If this process dies between "sent" and "marked", the rows are sent again
 * on restart: delivery is at least once, and the workers skip the repeats.
 * "for update skip locked" lets several relays run at once without sending the same row twice.
 */
@Component
@ConditionalOnProperty(name = "settle.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH = 500;

    private record Row(long id, String topic, String key, String payload) {}

    private final JdbcClient jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;

    public OutboxRelay(JdbcClient jdbc, KafkaTemplate<String, String> kafka, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.tx = tx;
    }

    @Scheduled(fixedDelayString = "${settle.relay.interval-ms:100}")
    public void publishBatch() {
        try {
            Integer sent = tx.execute(status -> {
                List<Row> rows = jdbc.sql("""
                        select id, topic, event_key, payload from outbox
                        where published_at is null
                        order by id limit :batch
                        for update skip locked""")
                        .param("batch", BATCH)
                        .query((rs, n) -> new Row(rs.getLong("id"), rs.getString("topic"),
                                rs.getString("event_key"), rs.getString("payload")))
                        .list();
                if (rows.isEmpty()) {
                    return 0;
                }
                List<CompletableFuture<SendResult<String, String>>> acks = new ArrayList<>();
                for (Row row : rows) {
                    acks.add(kafka.send(row.topic(), row.key(), row.payload()));
                }
                for (CompletableFuture<SendResult<String, String>> ack : acks) {
                    try {
                        ack.get(10, TimeUnit.SECONDS);   // any failure rolls back, rows stay unpublished
                    } catch (Exception e) {
                        throw new IllegalStateException("Kafka did not confirm an outbox row", e);
                    }
                }
                jdbc.sql("update outbox set published_at = now() where id in (:ids)")
                        .param("ids", rows.stream().map(Row::id).toList())
                        .update();
                return rows.size();
            });
            if (sent != null && sent > 0) {
                log.debug("published {} outbox rows", sent);
            }
        } catch (Exception e) {
            log.warn("outbox publish failed, will retry: {}", e.toString());
        }
    }
}
