BEGIN TRANSACTION;

PRAGMA main.application_id = 0x2237186b;
PRAGMA main.user_version = 1;

CREATE TABLE IF NOT EXISTS root (
    id INTEGER NOT NULL
        PRIMARY KEY
        AUTOINCREMENT,

    channel_url TEXT NULL,
    cache_url TEXT NULL,
    git_revision TEXT NULL,
    fetch_time TEXT NULL,
    closure_file_size INTEGER NULL,

    -- Pending, Downloading, Available
    status TEXT NOT NULL
        CHECK (status IN ('P', 'D', 'A'))
);

CREATE TABLE IF NOT EXISTS root_nar (
    root_id INTEGER NOT NULL
        REFERENCES root(id),
    nar_id INTEGER NOT NULL
        REFERENCES nar(id),
    PRIMARY KEY (root_id, nar_id)
);

CREATE INDEX IF NOT EXISTS root_referencee_idx ON root_nar (nar_id);

CREATE TABLE IF NOT EXISTS nar (
    -- Row id
    id INTEGER NOT NULL
        PRIMARY KEY
        AUTOINCREMENT,

    store_root TEXT NOT NULL,
    hash TEXT NOT NULL
        CHECK (LENGTH(hash) = 32)
        UNIQUE, -- Index
    name TEXT NOT NULL,

    url TEXT NULL,

    compression TEXT NULL,
    file_hash TEXT NULL,
    file_size INTEGER NULL,

    nar_hash TEXT NOT NULL,
    nar_size INTEGER NOT NULL,

    deriver TEXT NULL,
    sig TEXT NULL, -- Space separated
    ca TEXT NULL,

    -- Pending, Available, Trashed
    status TEXT NOT NULL
        CHECK (status IN ('P', 'A', 'T'))
);

CREATE TABLE IF NOT EXISTS nar_ref (
    nar_id INTEGER NOT NULL
        REFERENCES nar (id)
        ON DELETE CASCADE,
    ref_id INTEGER NOT NULL
        REFERENCES nar (id)
        ON DELETE RESTRICT,
    PRIMARY KEY (nar_id, ref_id) -- Index
);

CREATE INDEX IF NOT EXISTS nar_referencee_idx ON nar_ref (ref_id);

CREATE TRIGGER IF NOT EXISTS delete_self_ref
    BEFORE DELETE
    ON nar
    BEGIN
        DELETE FROM nar_ref
            WHERE (nar_id, ref_id) = (OLD.id, OLD.id);
    END;
COMMIT;
