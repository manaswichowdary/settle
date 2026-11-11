package com.manaswi.settle.ledger;

import com.manaswi.settle.payments.PaymentEvent;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class LedgerService {

    private final JdbcClient jdbc;

    public LedgerService(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Applies one payment event exactly once. Recording the event id and writing the entries happen
     * in one transaction: if the worker dies part way, nothing is kept and Kafka delivers the event
     * again. If the event was already applied, the insert into processed_events does nothing and we
     * stop. Balances are locked in account order so two workers never wait on each other in a circle.
     *
     * @return true if applied now, false if it was a repeat
     */
    @Transactional
    public boolean apply(PaymentEvent e) {
        int first = jdbc.sql("insert into processed_events (event_id) values (:id) on conflict do nothing")
                .param("id", e.eventId()).update();
        if (first == 0) {
            jdbc.sql("insert into skipped_duplicates (event_id) values (:id)").param("id", e.eventId()).update();
            return false;
        }
        jdbc.sql("""
                insert into ledger_entries (payment_id, account, amount_cents)
                values (:pid, :from, :debit), (:pid, :to, :credit)""")
                .param("pid", e.paymentId()).param("from", e.fromAccount()).param("to", e.toAccount())
                .param("debit", -e.amountCents()).param("credit", e.amountCents())
                .update();

        record Change(String account, long delta) {}
        List<Change> changes = List.of(new Change(e.fromAccount(), -e.amountCents()),
                new Change(e.toAccount(), e.amountCents()));
        changes.stream().sorted((a, b) -> a.account().compareTo(b.account())).forEach(c ->
                jdbc.sql("""
                        insert into account_balances (account, balance_cents) values (:account, :delta)
                        on conflict (account) do update
                        set balance_cents = account_balances.balance_cents + excluded.balance_cents, updated_at = now()""")
                        .param("account", c.account()).param("delta", c.delta())
                        .update());
        return true;
    }
}
