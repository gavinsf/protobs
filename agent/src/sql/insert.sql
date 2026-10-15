INSERT INTO captured_messages (
    source, topic, partition, "offset",
    broker_ts_ms, captured_at_ms,
    headers, payload,
    schema_id, schema_guid, message_index,
    payload_sha256
) VALUES (
    %s, %s, %s, %s,
    %s, %s,
    %s, %s,
    %s, %s, %s,
    %s
)
ON CONFLICT (source, topic, partition, "offset") DO NOTHING;