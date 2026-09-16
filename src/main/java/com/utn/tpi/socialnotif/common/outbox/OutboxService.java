package com.utn.tpi.socialnotif.common.outbox;

import com.utn.tpi.socialnotif.common.event.EventEnvelope;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Service
@Profile("kafka")
public class OutboxService {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OutboxService(OutboxRepository outboxRepository, ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    public OutboxEvent save(String topicDestino, String messageKey, String aggregateType, String aggregateId,
                            EventEnvelope<?> event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            OutboxEvent outboxEvent = new OutboxEvent(
                    event.eventId(),
                    event.eventType(),
                    aggregateType,
                    aggregateId,
                    topicDestino,
                    messageKey,
                    payload
            );
            return outboxRepository.save(outboxEvent);
        } catch (JacksonException e) {
            throw new IllegalStateException("Could not serialize outbox event", e);
        }
    }
}
