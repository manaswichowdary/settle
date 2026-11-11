package com.manaswi.settle;

import static org.assertj.core.api.Assertions.assertThat;

import com.manaswi.settle.payments.PaymentRequest;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

class PaymentRequestValidationTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void aNormalPaymentIsValid() {
        assertThat(validator.validate(new PaymentRequest("acct-1", "acct-2", 500, "USD"))).isEmpty();
    }

    @Test
    void rejectsSameAccountZeroAmountAndBadCurrency() {
        assertThat(validator.validate(new PaymentRequest("acct-1", "acct-1", 500, "USD"))).hasSize(1);
        assertThat(validator.validate(new PaymentRequest("acct-1", "acct-2", 0, "USD"))).hasSize(1);
        assertThat(validator.validate(new PaymentRequest("acct-1", "acct-2", 500, "usd"))).hasSize(1);
        assertThat(validator.validate(new PaymentRequest("", "acct-2", 500, "USD"))).isNotEmpty();
    }
}
