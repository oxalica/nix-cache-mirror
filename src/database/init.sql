BEGIN TRANSACTION;

PRAGMA main.application_id = 0x2237186b;
PRAGMA main.user_version = 1;

CREATE TABLE IF NOT EXISTS generation (
    id INTEGER NOT NULL
        PRIMARY KEY
        AUTOINCREMENT,

    start_time TEXT NOT NULL
        DEFAULT (CURRENT_TIMESTAMP),
    end_time TEXT NULL
        DEFAULT NULL,

    cache_url TEXT NOT NULL,
    extra_info TEXT NOT NULL, -- JSON

    total_paths INTEGER NULL
        DEFAULT NULL,
    total_file_size INTEGER NULL
        DEFAULT NULL,

    status TEXT NOT NULL
        DEFAULT ('pending')
        CHECK (
            status IN ('canceled', 'pending', 'indexing', 'downloading', 'finished') AND
            (status = 'finished') = (end_time IS NOT NULL)
        )
);

CREATE TABLE IF NOT EXISTS generation_root (
    generation_id INTEGER NOT NULL
        REFERENCES generation(id),

    -- The hash is not present in `nar_info` in progress of indexing (fetching meta).
    nar_id INTEGER NULL
        DEFAULT NULL
        REFERENCES nar_info(id),

    hash TEXT NOT NULL,
    name TEXT NOT NULL
    
    -- UNIQUE (generation_id, nar_info)
    -- UNIQUE (generation_id, hash)
);

CREATE TABLE IF NOT EXISTS nar_info (
    -- Row id
    id INTEGER NOT NULL
        PRIMARY KEY
        AUTOINCREMENT,

    -- A store path: /nix/store/d86czzx6rlm7la0xi6h6chy78wy755wk-openssl-1.1.1d
    --                          \------------ hash ------------/ \--- name ---/
    hash TEXT NOT NULL
        UNIQUE,
    name TEXT NOT NULL,
    
    available INTEGER NOT NULL
        CHECK (available IN (FALSE, TRUE)),

    compression TEXT NOT NULL,
    -- Compressed
    file_hash TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    -- Uncompressed
    nar_hash TEXT NOT NULL,
    nar_size INTEGER NOT NULL,

    deriver TEXT NULL,

    sig TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nar_reference (
    nar_id INTEGER NOT NULL
        REFERENCES nar_info(id),
    reference_id INTEGER NOT NULL
        REFERENCES nar_info(id)
    -- UNIQUE (nar_id, reference_id)
);

COMMIT;

-- PRAGMA foreign_keys = ON;
