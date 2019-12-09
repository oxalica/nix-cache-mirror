BEGIN TRANSACTION;

PRAGMA main.application_id = 0x2237186b;
PRAGMA main.user_version = 1;

CREATE TABLE IF NOT EXISTS root (
    id INTEGER NOT NULL
        PRIMARY KEY
        AUTOINCREMENT,

    meta TEXT NOT NULL -- JSON
        DEFAULT ('{}'),

    status TEXT NOT NULL
        DEFAULT ('pending')
        CHECK (status IN ('pending', 'downloading', 'available'))
);

CREATE TABLE IF NOT EXISTS root_nar (
    root_id INTEGER NOT NULL
        REFERENCES root(id),

    nar_id INTEGER NOT NULL
        REFERENCES nar(id)

    -- UNIQUE (root_id, nar_id)
);

CREATE TABLE IF NOT EXISTS nar (
    -- Row id
    id INTEGER NOT NULL
        PRIMARY KEY
        AUTOINCREMENT,

    store_path TEXT NOT NULL
        UNIQUE, -- Indexed

    meta TEXT NOT NULL, -- JSON

    status TEXT NOT NULL
        CHECK (status IN ('pending', 'available'))
);

COMMIT;

-- PRAGMA foreign_keys = ON;
