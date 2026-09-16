package com.utn.tpi.socialnotif.common.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_event")
public class OutboxEvent {

    @Id
    @Column(name = "outbox_id", nullable = false, updatable = false)
    private UUID outboxId;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "event_type", nullable = false, length = 120)
    private String eventType;

    @Column(name = "aggregate_type", nullable = false, length = 120)
    private String aggregateType;

    @Column(name = "aggregate_id", nullable = false, length = 120)
    private String aggregateId;

    @Column(name = "topic_destino", nullable = false, length = 160)
    private String topicDestino;

    @Column(name = "message_key", nullable = false, length = 200)
    private String messageKey;

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 30)
    private OutboxStatus status = OutboxStatus.PENDIENTE;

    @Column(name = "intentos", nullable = false)
    private int intentos;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxEvent() {
    }

    public OutboxEvent(UUID eventId, String eventType, String aggregateType, String aggregateId,
                       String topicDestino, String messageKey, String payload) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.topicDestino = topicDestino;
        this.messageKey = messageKey;
        this.payload = payload;
    }

    @PrePersist
    void prePersist() {
        if (outboxId == null) {
            outboxId = UUID.randomUUID();
        }
        if (createdAt == null) {
            createdAt = Instant.now();
        }
        if (status == null) {
            status = OutboxStatus.PENDIENTE;
        }
    }

    public UUID getOutboxId() {
        return outboxId;
    }

    public UUID getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getAggregateType() {
        return aggregateType;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getTopicDestino() {
        return topicDestino;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public String getPayload() {
        return payload;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public int getIntentos() {
        return intentos;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public void markPublished() {
        status = OutboxStatus.PUBLICADO;
        publishedAt = Instant.now();
    }

    public void markFailed() {
        status = OutboxStatus.FALLIDO;
    }

    public void incrementAttempts() {
        intentos++;
    }
}
