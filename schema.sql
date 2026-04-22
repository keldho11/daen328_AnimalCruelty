-- Miami-Dade Animal Services — normalized schema
-- Runs automatically on first `docker compose up` via initdb mount.

CREATE TABLE IF NOT EXISTS issue_types (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS ticket_statuses (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS priorities (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS submission_methods (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS districts (
    id   SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS tickets (
    id                    SERIAL PRIMARY KEY,
    ticket_id             VARCHAR(20)      NOT NULL UNIQUE,
    issue_type_id         INT              REFERENCES issue_types(id),
    street_address        VARCHAR(200),
    city                  VARCHAR(100),
    state                 VARCHAR(50),
    zip_code              INT,
    district_id           INT              REFERENCES districts(id),
    ticket_created_at     TIMESTAMP,
    ticket_updated_at     TIMESTAMP,
    ticket_closed_at      TIMESTAMP,
    status_id             INT              REFERENCES ticket_statuses(id),
    latitude              DOUBLE PRECISION,
    longitude             DOUBLE PRECISION,
    method_id             INT              REFERENCES submission_methods(id),
    priority_id           INT              REFERENCES priorities(id),
    actual_completed_days NUMERIC,
    arcgis_object_id      INT
);

CREATE INDEX IF NOT EXISTS idx_tickets_created  ON tickets (ticket_created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tickets_district ON tickets (district_id);
CREATE INDEX IF NOT EXISTS idx_tickets_issue    ON tickets (issue_type_id);
CREATE INDEX IF NOT EXISTS idx_tickets_status   ON tickets (status_id);
