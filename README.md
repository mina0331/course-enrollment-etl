# course-enrollment-etl

This project builds an end-to-end ETL pipeline that ingests university course enrollment data,
normalizes it into a relational schema, and prepares it for downstream analytics and modeling.

The pipeline extracts raw course, section, and instructor data from term-based sources, handles
inconsistent schemas across semesters, and transforms nested JSON into clean, queryable tables.
Special care is taken to support incremental loads, idempotent upserts, and schema drift between terms.

Data is stored in a relational database with structured tables for courses, sections, instructors,
and term metadata, while preserving raw payloads for traceability. The pipeline is designed to be
reproducible, debuggable, and extensible for future features such as waitlist outcome prediction
and enrollment forecasting.

Key challenges addressed include handling mixed JSON formats, resolving entity relationships across
terms, and ensuring consistent identifiers despite changes in upstream data.

This project serves as the data foundation for course enrollment analytics, historical trend analysis,
and predictive modeling.

---

## Tech Stack

### Languages
- Python

### Data Engineering
- Apache Airflow for orchestration
- ETL pipelines with batch and incremental loads
- JSON normalization and schema drift handling

### Databases
- PostgreSQL for relational storage
- SQLite for local development and testing
- JSON / JSONB for raw payload preservation

### Data Modeling
- Normalized relational schema
- Idempotent upserts and conflict resolution
- Foreign key relationships across courses, sections, instructors, and terms

### Infrastructure & Tooling
- Docker and docker-compose for reproducible environments
- Git and GitHub for version control
- Command-line tooling for local debugging and validation

### Future Extensions
- Enrollment and waitlist outcome prediction
- Feature engineering for course demand modeling
- Student-facing analytics and decision support tools
