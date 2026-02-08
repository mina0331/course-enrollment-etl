CREATE TABLE raw_sis_data_new(
    term_id TEXT NOT NULL,
    class_nbr TEXT NOT NULL,
    payload JSON NOT NULL,
    page_fetched INTEGER NOT NULL,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (term_id, class_nbr)
);

DROP TABLE raw_sis_data;
ALTER TABLE raw_sis_data_new RENAME TO raw_sis_data;