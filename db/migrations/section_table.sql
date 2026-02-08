CREATE TABLE section_new(
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

DROP TABLE section;
ALTER TABLE section_new RENAME TO section;