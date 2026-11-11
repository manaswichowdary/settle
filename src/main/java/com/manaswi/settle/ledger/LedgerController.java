package com.manaswi.settle.ledger;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LedgerController {

    private final JdbcClient jdbc;

    public LedgerController(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/accounts/{account}")
    public ResponseEntity<Map<String, Object>> balance(@PathVariable String account) {
        return jdbc.sql("select account, balance_cents from account_balances where account = :a")
                .param("a", account)
                .query((rs, n) -> Map.<String, Object>of("account", rs.getString(1), "balanceCents", rs.getLong(2)))
                .optional()
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Checks the whole ledger. "ok" is true only when every accepted payment was applied exactly once,
     * the entries sum to zero, every balance equals its entries, and the outbox is empty.
     */
    @GetMapping("/admin/check")
    public Map<String, Object> check() {
        Map<String, Object> out = new LinkedHashMap<>(jdbc.sql("""
                select
                  (select count(*) from payments)                                   as payments,
                  (select count(*) from processed_events)                           as events_applied,
                  (select count(*) from outbox where published_at is null)          as outbox_unpublished,
                  (select count(*) from payments p
                     where not exists (select 1 from ledger_entries e where e.payment_id = p.id)) as payments_not_applied,
                  (select count(*) from (select payment_id from ledger_entries
                     group by payment_id having count(*) <> 2) x)                   as payments_with_wrong_entry_count,
                  (select coalesce(sum(amount_cents), 0) from ledger_entries)        as ledger_sum_cents,
                  (select count(*) from account_balances b
                     where b.balance_cents <> (select coalesce(sum(e.amount_cents), 0)
                                               from ledger_entries e where e.account = b.account)) as balance_mismatches,
                  (select count(*) from skipped_duplicates)                          as duplicate_deliveries_skipped
                """).query().singleRow());

        Map<String, Object> latency = jdbc.sql("""
                select percentile_cont(0.5)  within group (order by ms) as p50_ms,
                       percentile_cont(0.95) within group (order by ms) as p95_ms,
                       max(ms)                                           as max_ms
                from (select extract(epoch from (min(e.applied_at) - p.created_at)) * 1000 as ms
                      from payments p join ledger_entries e on e.payment_id = p.id
                      group by p.id, p.created_at) t""").query().singleRow();
        out.put("apply_latency_ms", latency);

        long payments = ((Number) out.get("payments")).longValue();
        boolean ok = payments == ((Number) out.get("events_applied")).longValue()
                && ((Number) out.get("payments_not_applied")).longValue() == 0
                && ((Number) out.get("payments_with_wrong_entry_count")).longValue() == 0
                && ((Number) out.get("ledger_sum_cents")).longValue() == 0
                && ((Number) out.get("balance_mismatches")).longValue() == 0
                && ((Number) out.get("outbox_unpublished")).longValue() == 0;
        out.put("ok", ok);
        return out;
    }
}
