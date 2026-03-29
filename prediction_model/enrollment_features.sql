WITH professor_features AS (
    SELECT
        sp.term_id,
        sp.course_id,
        COUNT(DISTINCT sp.professor_id) AS instructor_count,
        AVG(sp.rating) AS avg_prof_rating,
        AVG(sp.difficulty) AS avg_prof_difficulty
    FROM section_professor sp
    GROUP BY sp.term_id, sp.course_id
),
section_base AS (
    SELECT
        s.term_id,
        CAST(s.term_id AS INTEGER) AS term_numeric,
        s.course_id,
        s.class_nbr,
        c.subject_id,
        c.catalog_nbr,
        c.title,
        c.credits,
        c.component,
        c.has_discussion,
        c.has_lab,
        s.enrollment_status,
        s.capacity,
        s.seats_taken,
        s.waitlist_size,
        s.current_waitlist,
        s.meetings_days,
        s.meetings_start_time,
        s.meetings_end_time,
        CASE
            WHEN s.capacity > 0 THEN s.seats_taken::NUMERIC / s.capacity
            ELSE NULL
        END AS fill_ratio,
        CASE
            WHEN s.capacity > 0 THEN s.current_waitlist::NUMERIC / s.capacity
            ELSE NULL
        END AS waitlist_ratio,
        CASE
            WHEN s.seats_taken >= s.capacity OR s.current_waitlist > 0 THEN 1
            ELSE 0
        END AS filled_binary,
        CASE
            WHEN c.catalog_nbr BETWEEN 1000 AND 1999 THEN 1000
            WHEN c.catalog_nbr BETWEEN 2000 AND 2999 THEN 2000
            WHEN c.catalog_nbr BETWEEN 3000 AND 3999 THEN 3000
            WHEN c.catalog_nbr BETWEEN 4000 AND 4999 THEN 4000
            ELSE NULL
        END AS course_level,
        CASE RIGHT(s.term_id, 1)
            WHEN '2' THEN 'spring'
            WHEN '6' THEN 'summer'
            WHEN '8' THEN 'fall'
            ELSE 'other'
        END AS season,
        COALESCE(
            LENGTH(REGEXP_REPLACE(s.meetings_days, '[^MTWRFSU]', '', 'g')),
            0
        ) AS meeting_day_count,
        CASE WHEN s.meetings_start_time = 'TBA' THEN 1 ELSE 0 END AS is_tba_time,
        pf.instructor_count,
        pf.avg_prof_rating,
        pf.avg_prof_difficulty
    FROM section s
    JOIN course c
        ON c.course_id = s.course_id
    LEFT JOIN professor_features pf
        ON pf.term_id = s.term_id
       AND pf.course_id = s.course_id
),
feature_history AS (
    SELECT
        sb.*,
        LAG(sb.fill_ratio) OVER (
            PARTITION BY sb.course_id
            ORDER BY sb.term_numeric
        ) AS prev_fill_ratio,
        LAG(sb.current_waitlist) OVER (
            PARTITION BY sb.course_id
            ORDER BY sb.term_numeric
        ) AS prev_waitlist,
        LAG(sb.capacity) OVER (
            PARTITION BY sb.course_id
            ORDER BY sb.term_numeric
        ) AS prev_capacity,
        COUNT(*) OVER (
            PARTITION BY sb.course_id
            ORDER BY sb.term_numeric
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prior_course_terms,
        AVG(sb.fill_ratio) OVER (
            PARTITION BY sb.course_id
            ORDER BY sb.term_numeric
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS avg_fill_ratio_prior_course,
        AVG(sb.current_waitlist::NUMERIC) OVER (
            PARTITION BY sb.course_id
            ORDER BY sb.term_numeric
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS avg_waitlist_prior_course,
        AVG(sb.capacity::NUMERIC) OVER (
            PARTITION BY sb.course_id
            ORDER BY sb.term_numeric
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS avg_capacity_prior_course,
        AVG(sb.fill_ratio) OVER (
            PARTITION BY sb.subject_id, sb.course_level
            ORDER BY sb.term_numeric
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS avg_fill_ratio_prior_subject_level,
        AVG(sb.current_waitlist::NUMERIC) OVER (
            PARTITION BY sb.subject_id, sb.course_level
            ORDER BY sb.term_numeric
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS avg_waitlist_prior_subject_level,
        COUNT(*) OVER (
            PARTITION BY sb.subject_id
            ORDER BY sb.term_numeric
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prior_subject_offerings
    FROM section_base sb
)
SELECT
    term_id,
    term_numeric,
    season,
    course_id,
    class_nbr,
    subject_id,
    catalog_nbr,
    title,
    credits,
    component,
    has_discussion,
    has_lab,
    course_level,
    enrollment_status,
    capacity,
    seats_taken,
    waitlist_size,
    current_waitlist,
    fill_ratio,
    waitlist_ratio,
    meetings_days,
    meetings_start_time,
    meetings_end_time,
    meeting_day_count,
    is_tba_time,
    COALESCE(instructor_count, 0) AS instructor_count,
    avg_prof_rating,
    avg_prof_difficulty,
    COALESCE(prev_fill_ratio, 0) AS prev_fill_ratio,
    COALESCE(prev_waitlist, 0) AS prev_waitlist,
    COALESCE(prev_capacity, 0) AS prev_capacity,
    COALESCE(prior_course_terms, 0) AS prior_course_terms,
    avg_fill_ratio_prior_course,
    avg_waitlist_prior_course,
    avg_capacity_prior_course,
    avg_fill_ratio_prior_subject_level,
    avg_waitlist_prior_subject_level,
    COALESCE(prior_subject_offerings, 0) AS prior_subject_offerings,
    filled_binary
FROM feature_history
ORDER BY term_numeric, subject_id, catalog_nbr, class_nbr;
