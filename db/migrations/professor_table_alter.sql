CREATE TABLE professor_new(
    --specific to each professor and each class: a profesor has different ratings for all courses
    professor_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL, 
    unique(name, email)
);

DROP TABLE professor;
ALTER TABLE professor_new RENAME TO professor;

CREATE TABLE section_professor_new(
    term_id TEXT NOT NULL,
    class_nbr TEXT NOT NULL,
    professor_id INT NOT NULL,
    PRIMARY KEY(term_id, class_nbr, professor_id),
    FOREIGN KEY (term_id, class_nbr) REFERENCES section(term_id, class_nbr),
    FOREIGN KEY (professor_id) REFERENCES professor(professor_id)
);

DROP TABLE section_professor;
ALTER TABLE section_professor_new RENAME TO section_professor;