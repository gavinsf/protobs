SELECT topic, partition, next_offset
FROM capture_progress
WHERE source = %s;