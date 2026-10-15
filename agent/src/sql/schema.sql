CREATE TABLE IF NOT EXISTS captured_messages (
    source          text    NOT NULL,
    topic           text    NOT NULL,
    partition       int     NOT NULL CHECK (partition >= 0),
    "offset"        bigint  NOT NULL CHECK ("offset" >= 0),
    broker_ts_ms    bigint,
    captured_at_ms  bigint  NOT NULL,
    headers         jsonb,
    payload         bytea,
    schema_id       int,
    schema_guid     uuid,
    message_index   int[],
    payload_sha256  bytea,
    PRIMARY KEY (source, topic, partition, "offset")
);

CREATE TABLE IF NOT EXISTS capture_dead_letters (
    source        text    NOT NULL,
    topic         text    NOT NULL,
    partition     int     NOT NULL,
    "offset"      bigint  NOT NULL,
    broker_ts_ms  bigint,
    failed_at_ms  bigint  NOT NULL,
    error         text    NOT NULL,
    headers_text  text,
    payload       bytea,
    PRIMARY KEY (source, topic, partition, "offset")
);

CREATE INDEX IF NOT EXISTS captured_topic_ts
    ON captured_messages (topic, broker_ts_ms);

CREATE INDEX IF NOT EXISTS captured_schema_id
    ON captured_messages (schema_id) WHERE schema_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS captured_schema_guid
    ON captured_messages (schema_guid) WHERE schema_guid IS NOT NULL;

CREATE TABLE IF NOT EXISTS capture_progress (
    source       text   NOT NULL,
    topic        text   NOT NULL,
    partition    int    NOT NULL,
    next_offset  bigint NOT NULL,
    PRIMARY KEY (source, topic, partition)
);

