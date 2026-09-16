package com.utn.tpi.socialnotif.common.outbox;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Profile("kafka")
public class OutboxPublisher {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int maxAttempts;

    public OutboxPublisher(OutboxRepository outboxRepository,
                           KafkaTemplate<String, String> kafkaTemplate,
                           @Value("${app.outbox.max-attempts:5}") int maxAttempts) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.maxAttempts = maxAttempts;
    }

    @Transactional
    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:3000}")
    public void publishPending() {
        for (OutboxEvent event : outboxRepository.findTop50ByStatusOrderByCreatedAtAsc(OutboxStatus.PENDIENTE)) {
            try {
                kafkaTemplate.send(event.getTopicDestino(), event.getMessageKey(), event.getPayload()).get();
                event.markPublished();
            } catch (Exception e) {
                event.incrementAttempts();
                if (event.getIntentos() >= maxAttempts) {
                    event.markFailed();
                }
            }
            outboxRepository.save(event);
        }
    }
}
