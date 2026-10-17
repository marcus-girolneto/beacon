-- Events are append-only and queried by time range, so the table has no
-- primary key and relies on a BRIN index over ts: it is orders of magnitude
-- smaller than a B-tree and nearly free to maintain. ts is the client
-- timestamp, which validation bounds to a few days behind the server clock,
-- so rows arrive close enough to ts order for BRIN ranges to stay tight.
CREATE TABLE IF NOT EXISTS events (
    message_id    text        NOT NULL,
    type          text        NOT NULL,
    name          text        NOT NULL DEFAULT '',
    anonymous_id  text        NOT NULL,
    visitor_hash  text        NOT NULL,
    url           text        NOT NULL,
    host          text        NOT NULL,
    path          text        NOT NULL,
    referrer      text        NOT NULL DEFAULT '',
    referrer_host text        NOT NULL DEFAULT '',
    channel       text        NOT NULL,
    utm_source    text        NOT NULL DEFAULT '',
    utm_medium    text        NOT NULL DEFAULT '',
    utm_campaign  text        NOT NULL DEFAULT '',
    utm_term      text        NOT NULL DEFAULT '',
    utm_content   text        NOT NULL DEFAULT '',
    device        text        NOT NULL,
    properties    jsonb,
    ts            timestamptz NOT NULL,
    received_at   timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS events_ts_brin ON events USING brin (ts);
