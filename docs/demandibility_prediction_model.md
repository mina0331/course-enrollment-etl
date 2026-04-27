# Demandibility Prediction Model

This project is now focused on predicting course demandibility: how strongly a course is likely to be demanded by students based on academic-path requirements, historical enrollment pressure, course signals, and available seats.

## Core Framing

The model should prioritize course-level and major-driven demand rather than recommendation behavior, personal preference scoring, or registration-attempt prediction.

Demandibility should answer:

- Is this course required for a student's major?
- Is this course required by many majors?
- Has this course historically filled or waitlisted?
- Is current seat availability constrained?
- Do Course Forum signals suggest high interest?
- Does professor difficulty reduce demand pressure?

## Major Requirement Signal

The major requirement pipeline populates:

- `academic_program`
- `requirement_group`
- `requirement_rule`
- `requirement_course`

The key modeling join is:

```text
academic_program
-> requirement_group
-> requirement_rule
-> requirement_course
-> course / section
```

Useful features:

- `major_required`: whether a course is explicitly listed for a student's major
- `required_by_major_count`: number of programs that explicitly mention the course
- `requirement_group_count`: number of requirement groups mentioning the course
- `requirement_rule_count`: number of rules mentioning the course

## Synthetic Student Shape

Synthetic contexts should be major-specific, not preference-specific.

Keep:

- `student_id`
- `major_code`
- `class_standing`
- optional `priority_score` only if modeling access pressure
- optional `enrollment_slot_hours` only if modeling access pressure

Avoid:

- recommendation labels
- attempt rows
- interest-area personalization
- arbitrary schedule or difficulty taste profiles

## Demandibility Formula Direction

A practical first-pass synthetic score:

```text
major_requirement_demand =
  1.00 * major_required
  + 0.25 * log1p(required_by_major_count)
```

```text
registration_competitiveness =
  historical_demand
  + fill_ratio
  + waitlist_ratio
  + 0.20 * course_forum_signal
  - 0.20 * difficulty_demand_relief
```

```text
linear_score =
  baseline
  + major_requirement_demand
  + historical_demand
  + fill_pressure
  + waitlist_pressure
  + course_forum_signal
  - difficulty_demand_relief
  + shocks
```

The output probability is a demand score scaled to `[0, 1]`:

```text
probability = sigmoid(linear_score)
```

## Cleanup Notes

Old report-style docs and ranking/recommendation framing should stay out of the active documentation. Current docs should point readers toward the demandibility model, major requirement pipeline, and section demand features.
