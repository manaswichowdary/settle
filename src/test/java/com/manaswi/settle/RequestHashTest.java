package com.manaswi.settle;

import static org.assertj.core.api.Assertions.assertThat;

import com.manaswi.settle.payments.PaymentRequest;
import com.manaswi.settle.payments.RequestHash;
import org.junit.jupiter.api.Test;

class RequestHashTest {

    @Test
    void sameRequestGivesSameHash() {
        assertThat(RequestHash.of(new PaymentRequest("a", "b", 500, "USD")))
                .isEqualTo(RequestHash.of(new PaymentRequest("a", "b", 500, "USD")))
                .hasSize(64);
    }

    @Test
    void anyChangedFieldChangesTheHash() {
        String base = RequestHash.of(new PaymentRequest("a", "b", 500, "USD"));
        assertThat(RequestHash.of(new PaymentRequest("a", "c", 500, "USD"))).isNotEqualTo(base);
        assertThat(RequestHash.of(new PaymentRequest("a", "b", 501, "USD"))).isNotEqualTo(base);
        assertThat(RequestHash.of(new PaymentRequest("a", "b", 500, "EUR"))).isNotEqualTo(base);
    }
}
