CREATE TABLE IF NOT EXISTS links
(
    id         SERIAL PRIMARY KEY,
    link       TEXT UNIQUE NOT NULL,
    short_link VARCHAR(8) UNIQUE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );