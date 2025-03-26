package qpostgres

const createTablesSQL = `
CREATE TABLE IF NOT EXISTS Entities (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    cursor_id BIGSERIAL
);

CREATE TABLE IF NOT EXISTS EntitySchema (
    entity_type TEXT NOT NULL,
    field_type TEXT NOT NULL,
    value_type TEXT NOT NULL,
    rank INTEGER NOT NULL,
    read_permissions TEXT[],
    write_permissions TEXT[],
    cursor_id BIGSERIAL,
    PRIMARY KEY (entity_type, field_type)
);

CREATE TABLE IF NOT EXISTS Strings (
    entity_id TEXT NOT NULL,
    field_type TEXT NOT NULL,
    field_value TEXT,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    cursor_id BIGSERIAL,
    PRIMARY KEY (entity_id, field_type)
);

CREATE TABLE IF NOT EXISTS BinaryFiles (
    entity_id TEXT NOT NULL,
    field_type TEXT NOT NULL,
    field_value TEXT,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    cursor_id BIGSERIAL,
    PRIMARY KEY (entity_id, field_type)
);

CREATE TABLE IF NOT EXISTS Ints (
    entity_id TEXT NOT NULL,
    field_type TEXT NOT NULL,
    field_value BIGINT,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    cursor_id BIGSERIAL,
    PRIMARY KEY (entity_id, field_type)
);

CREATE TABLE IF NOT EXISTS Floats (
    entity_id TEXT NOT NULL,
    field_type TEXT NOT NULL,
    field_value DOUBLE PRECISION,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    cursor_id BIGSERIAL,
    PRIMARY KEY (entity_id, field_type)
);

CREATE TABLE IF NOT EXISTS Bools (
    entity_id TEXT NOT NULL,
    field_type TEXT NOT NULL,
    field_value BOOLEAN,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    cursor_id BIGSERIAL,
    PRIMARY KEY (entity_id, field_type)
);

CREATE TABLE IF NOT EXISTS EntityReferences (
    entity_id TEXT NOT NULL,
    field_type TEXT NOT NULL,
    field_value TEXT,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    cursor_id BIGSERIAL,
    PRIMARY KEY (entity_id, field_type)
);

CREATE TABLE IF NOT EXISTS Timestamps (
    entity_id TEXT NOT NULL,
    field_type TEXT NOT NULL,
    field_value TIMESTAMP,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    cursor_id BIGSERIAL,
    PRIMARY KEY (entity_id, field_type)
);

CREATE TABLE IF NOT EXISTS Choices (
    entity_id TEXT NOT NULL,
    field_type TEXT NOT NULL,
    field_value BIGINT,
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    cursor_id BIGSERIAL,
    PRIMARY KEY (entity_id, field_type)
);

CREATE TABLE IF NOT EXISTS ChoiceOptions (
    entity_type TEXT NOT NULL,
    field_type TEXT NOT NULL,
    options TEXT[],
    cursor_id BIGSERIAL,
    PRIMARY KEY (entity_type, field_type)
);

CREATE TABLE IF NOT EXISTS EntityLists (
    entity_id TEXT NOT NULL,
    field_type TEXT NOT NULL,
    field_value TEXT[],
    write_time TIMESTAMP NOT NULL,
    writer TEXT NOT NULL,
    cursor_id BIGSERIAL,
    PRIMARY KEY (entity_id, field_type)
);

CREATE TABLE IF NOT EXISTS ReverseEntityReferences (
    referenced_entity_id TEXT NOT NULL,
    referenced_by_entity_id TEXT NOT NULL,
    referenced_by_field_type TEXT NOT NULL,
    cursor_id BIGSERIAL,
    PRIMARY KEY (referenced_entity_id, referenced_by_entity_id, referenced_by_field_type)
);
`
