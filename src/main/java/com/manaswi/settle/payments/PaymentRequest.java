package com.manaswi.settle.payments;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record PaymentRequest(
        @NotBlank @Size(max = 64) String fromAccount,
        @NotBlank @Size(max = 64) String toAccount,
        @Positive long amountCents,
        @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency) {

    @AssertTrue(message = "fromAccount and toAccount must be different")
    public boolean isDifferentAccounts() {
        return fromAccount == null || !fromAccount.equals(toAccount);
    }
}
