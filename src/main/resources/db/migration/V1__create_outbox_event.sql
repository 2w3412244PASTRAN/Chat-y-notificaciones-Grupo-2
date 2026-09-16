CREATE TABLE outbox_event (
    outbox_id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    event_type VARCHAR(120) NOT NULL,
    aggregate_type VARCHAR(120) NOT NULL,
    aggregate_id VARCHAR(120) NOT NULL,
    topic_destino VARCHAR(160) NOT NULL,
    message_key VARCHAR(200) NOT NULL,
    payload TEXT NOT NULL,
    status VARCHAR(30) NOT NULL,
    intentos INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    published_at TIMESTAMP WITH TIME ZONE NULL
);

CREATE INDEX idx_outbox_event_status_created_at ON outbox_event (status, created_at);
CREATE INDEX idx_outbox_event_event_id ON outbox_event (event_id);
