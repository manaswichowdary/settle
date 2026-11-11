package com.manaswi.settle.payments;

import java.time.OffsetDateTime;
import java.util.UUID;

public record Payment(UUID id, String fromAccount, String toAccount, long amountCents,
                      String currency, OffsetDateTime createdAt) {}
