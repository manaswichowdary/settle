package com.manaswi.settle;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * One codebase, two roles. The API accepts payments and publishes them through the outbox.
 * Workers read payment events from Kafka and apply them to the ledger. Properties decide
 * which role a running copy plays (see application.yml and docker-compose.yml).
 */
@SpringBootApplication
@EnableScheduling
public class SettleApplication {
    public static void main(String[] args) {
        SpringApplication.run(SettleApplication.class, args);
    }
}
