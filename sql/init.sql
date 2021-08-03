
CREATE TABLE IF NOT EXISTS messages (
    id serial PRIMARY KEY,
    idempotency_key text,
    message text
);
