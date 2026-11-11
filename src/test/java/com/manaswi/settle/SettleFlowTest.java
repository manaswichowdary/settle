package com.manaswi.settle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** Runs the real flow against real Postgres and Kafka in Docker: API, outbox relay and worker together. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"settle.relay.interval-ms=50"})
@Testcontainers
class SettleFlowTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    @Autowired TestRestTemplate http;
    @Autowired JdbcClient jdbc;
    @Autowired KafkaTemplate<String, String> kafkaTemplate;

    private static String body(String from, String to, long cents) {
        return """
                {"fromAccount":"%s","toAccount":"%s","amountCents":%d,"currency":"USD"}""".formatted(from, to, cents);
    }

    @SuppressWarnings("rawtypes")
    private ResponseEntity<Map> post(String key, String json) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (key != null) {
            headers.set("Idempotency-Key", key);
        }
        return http.postForEntity("/payments", new HttpEntity<>(json, headers), Map.class);
    }

    private long balance(String account) {
        return jdbc.sql("select coalesce((select balance_cents from account_balances where account = :a), 0)")
                .param("a", account).query(Long.class).single();
    }

    @Test
    void aRetriedKeyReturnsTheSamePayment() {
        var first = post("retry-key-0001", body("acct-a", "acct-b", 500));
        var second = post("retry-key-0001", body("acct-a", "acct-b", 500));
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().get("id")).isEqualTo(first.getBody().get("id"));
        assertThat(jdbc.sql("select count(*) from payments where idempotency_key = 'retry-key-0001'")
                .query(Long.class).single()).isEqualTo(1L);
        assertThat(jdbc.sql("select count(*) from outbox o join payments p on o.event_key = p.id::text "
                + "where p.idempotency_key = 'retry-key-0001'").query(Long.class).single()).isEqualTo(1L);
    }

    @Test
    void aReusedKeyWithADifferentBodyIsRejected() {
        assertThat(post("conflict-key-01", body("acct-a", "acct-b", 500)).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(post("conflict-key-01", body("acct-a", "acct-b", 999)).getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    void badRequestsAreRejected() {
        assertThat(post("invalid-key-01", body("acct-a", "acct-a", 500)).getStatusCode())
                .isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(post(null, body("acct-a", "acct-b", 500)).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(post("short", body("acct-a", "acct-b", 500)).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void aPaymentReachesTheLedger() {
        assertThat(post("flow-key-0001", body("acct-c", "acct-d", 700)).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(balance("acct-d")).isEqualTo(700L);
            assertThat(balance("acct-c")).isEqualTo(-700L);
        });
    }

    @Test
    void aRepeatedEventIsAppliedOnce() {
        UUID eventId = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();
        String event = """
                {"eventId":"%s","paymentId":"%s","fromAccount":"acct-e","toAccount":"acct-f","amountCents":300,"currency":"USD"}"""
                .formatted(eventId, paymentId);
        kafkaTemplate.send("payments", paymentId.toString(), event);
        kafkaTemplate.send("payments", paymentId.toString(), event);   // the same event delivered twice

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(jdbc.sql("select count(*) from skipped_duplicates where event_id = :id")
                        .param("id", eventId).query(Long.class).single()).isEqualTo(1L));
        assertThat(jdbc.sql("select count(*) from ledger_entries where payment_id = :id")
                .param("id", paymentId).query(Long.class).single()).isEqualTo(2L);
        assertThat(balance("acct-f")).isEqualTo(300L);
    }

    @Test
    void theLedgerAlwaysBalances() {
        post("balance-key-001", body("acct-g", "acct-h", 1234));
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> check = http.getForObject("/admin/check", Map.class);
            assertThat(((Number) check.get("outbox_unpublished")).longValue()).isZero();
            assertThat(((Number) check.get("payments_not_applied")).longValue()).isZero();
            assertThat(((Number) check.get("ledger_sum_cents")).longValue()).isZero();
            assertThat(((Number) check.get("balance_mismatches")).longValue()).isZero();
            assertThat(((Number) check.get("payments_with_wrong_entry_count")).longValue()).isZero();
        });
    }
}
