# Project Next Steps

## Modeling Goals

- Build a section/course popularity model.
- Build a student enrollment-success model.

## Planned Feature Additions

- Add major-requirement features to courses.
- Add meeting-time demand features.
- Add a no-Friday-meetings feature.
- Add related schedule-demand features such as meeting-day patterns and time-of-day effects.

## Recommended Order

1. Strengthen the section/course popularity model first.
2. Add major-requirement and schedule-related features to the section-level dataset.
3. Re-evaluate the section-level baseline after those features are added.
4. Build the student enrollment-success model using enrollment time plus section-demand features.

## Notes

- The section popularity model and the student enrollment-success model should stay separate.
- The student model should use enrollment timing as the primary student-specific feature.
- Major requirement data will likely need a hybrid pipeline with automated parsing plus manual review.
