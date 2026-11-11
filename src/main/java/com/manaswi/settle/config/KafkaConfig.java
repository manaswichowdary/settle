package com.manaswi.settle.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    /** 6 partitions, so up to 6 worker threads can share the load. */
    @Bean
    public NewTopic paymentsTopic(@Value("${settle.topic}") String topic) {
        return TopicBuilder.name(topic).partitions(6).replicas(1).build();
    }

    /**
     * A failing event is retried 3 times, then parked on a dead letter topic so one bad message
     * cannot block its partition. A message that is not valid JSON goes there straight away.
     */
    @Bean
    public CommonErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        DefaultErrorHandler handler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template),
                new FixedBackOff(500L, 3L));
        handler.addNotRetryableExceptions(JsonProcessingException.class);
        return handler;
    }
}
