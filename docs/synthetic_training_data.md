# Synthetic Demandibility Training Data

This project includes a synthetic student-section demandibility dataset generator in
[prediction_model/student_training_data.py](/Users/sominahn/Library/Mobile%20Documents/com~apple~CloudDocs/Course%20SIS%20Project/course-enrollment-etl/prediction_model/student_training_data.py).

The goal is to compute major-aware course demandibility using major
requirements and real section demand signals. This should not represent whether
a specific student will try to enroll, and it should not depend on personal
preference profiles.

## What It Produces

The generator should create one row per synthetic student and section.

By default:
- `n_students = 2000`
- output size is `n_students * section_count`

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

Each synthetic student/program context should get:
- `student_id`: synthetic identifier
- `major_code`: academic program identifier
- `class_standing`: one of `first_year`, `sophomore`, `junior`, `senior`
- `priority_score`: optional registration-order context, if modeling access pressure
- `enrollment_slot_hours`: optional registration timing context, if modeling access pressure

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
- `major_required`: whether the course appears in the student's major requirements
- `required_by_major_count`: number of programs that mention the course
- `major_requirement_demand`: major-driven course demand signal
- `registration_competitiveness`: combined scarcity and demand measure
- `difficulty_demand_relief`: how much high professor difficulty is treated as lowering demand

## Target Columns

The demandibility-focused target should be course demand, not whether a specific
student attempted registration or successfully enrolled.

Preferred target columns:
- `demandibility_score`: continuous course/program demand score
- `demandibility_probability`: sigmoid-scaled demand score
- `high_demand`: optional binary label sampled from the demandibility probability

The score should be driven by:
- major requirement demand
- historical demand
- fill pressure
- waitlist pressure
- professor difficulty as a demand-reducing signal
- Course Forum demand signal
- section capacity

## Why It Is Synthetic

This dataset is not a historical log of real student registration behavior.
It is a simulation layer intended to support:
- prototyping downstream demand analytics
- testing major-aware course demand estimates
- experimenting with waitlist and access-pressure features

The section side of the data is real. The student side and the final
registration outcomes are simulated.

## How It Handles Future Uncertainty

The generator is designed to be future-like, not future-perfect.

To avoid simply replaying past enrollment outcomes, the simulation now includes:
- major-specific requirement matching
- random term/course/subject shocks

Those additions make the synthetic outcomes less deterministic and help the
dataset reflect the uncertainty of future enrollment behavior.

That said, the generator still does **not** guarantee a true representation of
future demand. It is best understood as a scenario simulator anchored to real
historical section features.

The main limitations are:
- it does not observe real student enrollment decisions
- it does not model individual degree progress within a major yet
- it does not know future curriculum changes, professor switches, or macro shocks
- it uses stylized demand assumptions rather than learned causal behavior

The right interpretation is:
- good for experimentation and stress testing
- not a substitute for real future enrollment outcomes
