# Synthetic Training Data

This project includes a synthetic student-section dataset generator in
[prediction_model/student_training_data.py](/Users/sominahn/Library/Mobile%20Documents/com~apple~CloudDocs/Course%20SIS%20Project/course-enrollment-etl/prediction_model/student_training_data.py).

The goal of this dataset is to simulate course registration attempts at the
student-section level using real section demand signals plus simple student-side
behavior assumptions.

## What It Produces

The generator creates one row per simulated registration attempt.

By default:
- `n_students = 2000`
- `attempts_per_student = 5`
- output size is about `10,000` synthetic rows

The CSV export path is:
- [prediction_model/student_section_training_data.csv](/Users/sominahn/Library/Mobile%20Documents/com~apple~CloudDocs/Course%20SIS%20Project/course-enrollment-etl/prediction_model/student_section_training_data.csv)

## How It Is Built

The synthetic dataset starts from the real section-level feature frame loaded by
[prediction_model/baseline.py](/Users/sominahn/Library/Mobile%20Documents/com~apple~CloudDocs/Course%20SIS%20Project/course-enrollment-etl/prediction_model/baseline.py).

That real feature frame already includes:
- section capacity and waitlist fields
- historical fill behavior
- professor rating and difficulty
- Course Forum review features
- Course Forum demand and sentiment features

The synthetic generator then adds simulated student-side attributes and
combines them with real section-side scarcity signals.

## Student Fields

Each synthetic student gets:
- `student_id`: synthetic identifier
- `class_standing`: one of `first_year`, `sophomore`, `junior`, `senior`
- `priority_score`: higher for upperclass students
- `enrollment_slot_hours`: simulated registration timing
- `max_course_level`: upper bound on course level the student is assumed to target

The standing mix is sampled probabilistically:
- senior: `24%`
- junior: `26%`
- sophomore: `25%`
- first_year: `25%`

## Section Fields Carried Into The Synthetic Data

The synthetic rows keep the original section/course features, including:
- `term_id`
- `course_id`
- `class_nbr`
- `subject_id`
- `catalog_nbr`
- `title`
- `component`
- `capacity`
- `seats_taken`
- `current_waitlist`
- `fill_ratio`
- `waitlist_ratio`
- `avg_prof_rating`
- `avg_prof_difficulty`
- `avg_course_forum_sentiment`
- `avg_course_forum_demand`
- `avg_course_forum_review_count`
- `total_course_forum_reviews`
- `avg_fill_ratio_prior_course`
- `avg_fill_ratio_prior_subject_level`

## Derived Synthetic Fields

The generator computes several intermediate fields before sampling outcomes:
- `course_level`: inferred from the catalog number when needed
- `historical_demand`: backfilled from prior course or subject-level fill history
- `course_forum_signal`: combines review demand, review sentiment, and review volume
- `seat_pressure`: combines fill pressure and current waitlist pressure
- `is_morning_course`: whether the meeting time appears to be in the morning
- `level_ok`: whether the course level is at or below the student's assumed level
- `registration_competitiveness`: combined scarcity and demand measure

## Outcome Columns

Each synthetic row gets three outcome-style columns:
- `got_in_probability`: model-based probability of successful enrollment
- `got_in`: Bernoulli draw from that probability
- `waitlisted`: `1` when the student did not get in and the section had a waitlist

The probability is driven by:
- class standing priority
- enrollment timing
- historical demand
- fill pressure
- waitlist pressure
- Course Forum demand signal
- section capacity
- whether the section level matches the student profile

## Why It Is Synthetic

This dataset is not a historical log of real student registration attempts.
It is a simulation layer intended to support:
- prototyping downstream student-facing tools
- testing personalized recommendation logic
- experimenting with waitlist and access-risk features

The section side of the data is real. The student side and the final
registration outcomes are simulated.

## How It Handles Future Uncertainty

The generator is designed to be future-like, not future-perfect.

To avoid simply replaying past enrollment outcomes, the simulation now includes:
- student interest areas tied loosely to subject families
- schedule flexibility differences across students
- difficulty tolerance differences across students
- planning noise at the student level
- random term/course shocks at the attempt level

Those additions make the synthetic outcomes less deterministic and help the
dataset reflect the uncertainty of future enrollment behavior.

That said, the generator still does **not** guarantee a true representation of
future demand. It is best understood as a scenario simulator anchored to real
historical section features.

The main limitations are:
- it does not observe real student preference histories
- it does not model major-specific degree progress directly
- it does not know future curriculum changes, professor switches, or macro shocks
- it uses stylized behavioral assumptions rather than learned causal behavior

The right interpretation is:
- good for experimentation and stress testing
- not a substitute for real future enrollment outcomes
