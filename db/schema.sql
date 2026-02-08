CREATE TABLE raw_sis_data (
  term_id TEXT NOT NULL,
  payload  JSON NOT NULL,
  page INTEGER NOT NULL,
  fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (term_id)
);

CREATE TABLE raw_professor_data (
  catalog_nbr TEXT NOT NULL,
  subject_id TEXT NOT NULL,
  payload      JSON NOT NULL,
  fetched_at   TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (catalog_nbr, subject_id)
);

-- CREATE TABLE raw_major_requirements_data (
--   major_code TEXT NOT NULL,
--   payload    JSON NOT NULL,
--   fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
--   PRIMARY KEY (major_code)
-- );
-- CREATE TABLE raw_reddit_data();

