INSERT INTO capture_dead_letters (
    source, topic, partition, "offset",
    broker_ts_ms, failed_at_ms, error, headers_text, payload
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (source, topic, partition, "offset") DO NOTHING;