package com.manaswi.settle.payments;

import java.util.UUID;

/** What the API publishes and the workers apply. eventId is how workers recognise a repeat. */
public record PaymentEvent(UUID eventId, UUID paymentId, String fromAccount, String toAccount,
                           long amountCents, String currency) {}
