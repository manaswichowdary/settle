package com.manaswi.settle.payments;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class PaymentController {

    private final PaymentService payments;

    public PaymentController(PaymentService payments) {
        this.payments = payments;
    }

    /** 201 for a new payment, 200 when the same key is retried, 409 when a key is reused for a different body. */
    @PostMapping("/payments")
    public ResponseEntity<Payment> create(@RequestHeader("Idempotency-Key") String key,
                                          @Valid @RequestBody PaymentRequest request) {
        if (key.length() < 8 || key.length() > 100) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Idempotency-Key must be 8 to 100 characters");
        }
        PaymentService.Result result = payments.create(key, request);
        return ResponseEntity.status(result.created() ? HttpStatus.CREATED : HttpStatus.OK).body(result.payment());
    }
}
