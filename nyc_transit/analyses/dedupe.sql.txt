SELECT
    insert_timestamp,
    event_id,
    event_type,
    user_id,
    event_timestamp
FROM events
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id, event_type, user_id, insert_timestamp
    ORDER BY event_timestamp DESC
) = 1;
