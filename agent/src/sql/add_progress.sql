INSERT INTO capture_progress (source, topic, partition, next_offset)
VALUES (%s, %s, %s, %s)
ON CONFLICT (source, topic, partition)
DO UPDATE SET next_offset = GREATEST(capture_progress.next_offset, EXCLUDED.next_offset);