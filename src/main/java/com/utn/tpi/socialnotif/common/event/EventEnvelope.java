package com.utn.tpi.socialnotif.common.event;

import java.time.Instant;
import java.util.UUID;

public record EventEnvelope<T>(
        UUID eventId,
        String eventType,
        int eventVersion,
        Instant timestamp,
        String producer,
        T payload
) {

    public static <T> EventEnvelope<T> create(String eventType, int eventVersion, String producer, T payload) {
        return new EventEnvelope<>(UUID.randomUUID(), eventType, eventVersion, Instant.now(), producer, payload);
    }
}
