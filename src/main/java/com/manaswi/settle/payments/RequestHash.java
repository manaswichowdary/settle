package com.manaswi.settle.payments;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Fingerprint of a request, so a reused idempotency key with a different body is caught. */
public final class RequestHash {
    private RequestHash() {}

    public static String of(PaymentRequest r) {
        String canonical = String.join("|", r.fromAccount(), r.toAccount(),
                Long.toString(r.amountCents()), r.currency());
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is always available", e);
        }
    }
}
