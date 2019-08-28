
CREATE TYPE IF NOT EXISTS squeuel_event_state AS ENUM (
    'NEW',
    'DONE',
    'ERROR'
)

CREATE TABLE IF NOT EXISTS squeuel_events (
    id BIGSERIAL PRIMARY KEY,
    utc_created TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    event_queue VARCHAR(16) NOT NULL,
    event_partition VARCHAR(64) NOT NULL,
    event_state squeuel_event_state NOT NULL,
    event_data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS squeuel_locks (
    event_id BIGINT NOT NULL,
    utc_locked_until TIMESTAMP WITHOUT TIME ZONE NOT NULL
);