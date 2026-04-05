CREATE TABLE IF NOT EXISTS "course"(
    course_id INT PRIMARY KEY,
    --shared across the same class
    title TEXT NOT NULL,
    credits INT, 
    subject_id TEXT NOT NULL,
    catalog_nbr INT NOT NULL, 
    has_discussion BOOLEAN NOT NULL,
    has_lab BOOLEAN NOT NULL,
    component TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "section"(
    course_id INT NOT NULL,
    --shared across the same class 
    class_nbr TEXT NOT NULL,
    --unique for each section for each semester
    term_id TEXT NOT NULL,
    enrollment_status TEXT NOT NULL,
    capacity INT NOT NULL,
    seats_taken INT NOT NULL,
    waitlist_size INT NOT NULL,
    current_waitlist INT NOT NULL,
    meetings_days TEXT NOT NULL,
    meetings_start_time TEXT NOT NULL,
    meetings_end_time TEXT NOT NULL,
    PRIMARY KEY (class_nbr, term_id),
    FOREIGN KEY (course_id) REFERENCES course(course_id)

);

CREATE TABLE IF NOT EXISTS "raw_sis_data"(
    term_id TEXT NOT NULL,
    class_nbr TEXT NOT NULL,
    payload JSON NOT NULL,
    page_fetched INTEGER NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (term_id, class_nbr)
);

CREATE TABLE IF NOT EXISTS "professor"(
    --specific to each professor and each class: a profesor has different ratings for all courses
    professor_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL, 
    unique(name, email)
);

CREATE TABLE IF NOT EXISTS "section_professor"(
    term_id TEXT NOT NULL,
    course_id INT NOT NULL,
    professor_id INT NOT NULL, rating DECIMAL(10,2), difficulty DECIMAL(10,2),
    PRIMARY KEY(term_id, course_id, professor_id),
    FOREIGN KEY (term_id, course_id) REFERENCES section(term_id, course_id),
    FOREIGN KEY (professor_id) REFERENCES professor(professor_id)
);

CREATE TABLE IF NOT EXISTS "professor_rating_raw_html"(
    term TEXT NOT NULL,
    subject TEXT NOT NULL,
    catalog_nbr TEXT NOT NULL,
    course_id INT NOT NULL,
    url TEXT NOT NULL,
    raw_html TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (subject, catalog_nbr, course_id)
);

CREATE TABLE IF NOT EXISTS "course_forum_professor_page_raw_html"(
    course_id INT NOT NULL,
    subject TEXT NOT NULL,
    catalog_nbr TEXT NOT NULL,
    professor_name TEXT NOT NULL,
    professor_page_url TEXT NOT NULL PRIMARY KEY,
    raw_html TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS "course_forum_review"(
    review_key TEXT NOT NULL PRIMARY KEY,
    course_id INT NOT NULL,
    subject TEXT NOT NULL,
    catalog_nbr TEXT NOT NULL,
    professor_id INT,
    professor_name TEXT NOT NULL,
    professor_page_url TEXT NOT NULL,
    review_term_label TEXT,
    updated_at TEXT,
    professor_review_text TEXT,
    course_review_text TEXT,
    full_review_text TEXT NOT NULL,
    sentiment_score DOUBLE PRECISION NOT NULL,
    professor_sentiment_score DOUBLE PRECISION,
    course_sentiment_score DOUBLE PRECISION,
    sentiment_label TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS "course_forum_review_summary"(
    course_id INT NOT NULL,
    professor_id INT,
    subject TEXT NOT NULL,
    catalog_nbr TEXT NOT NULL,
    professor_name TEXT NOT NULL,
    professor_page_url TEXT NOT NULL,
    review_count INT NOT NULL,
    positive_review_count INT NOT NULL,
    neutral_review_count INT NOT NULL,
    negative_review_count INT NOT NULL,
    avg_sentiment_score DOUBLE PRECISION NOT NULL,
    avg_professor_sentiment_score DOUBLE PRECISION,
    avg_course_sentiment_score DOUBLE PRECISION,
    sentiment_demand_score DOUBLE PRECISION NOT NULL,
    most_recent_review_at TEXT,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (course_id, professor_name)
);
