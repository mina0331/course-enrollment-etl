CREATE TABLE professor_rating_raw_html_new(
    term TEXT NOT NULL,
    subject TEXT NOT NULL,
    catalog_nbr TEXT NOT NULL,
    course_id INT NOT NULL,
    url TEXT NOT NULL,
    raw_html TEXT NOT NULL,
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (subject, catalog_nbr, course_id)
);
DROP TABLE professor_rating_raw_html_new;
ALTER TABLE professor_rating_raw_html_new RENAME TO professor_rating_raw_html;