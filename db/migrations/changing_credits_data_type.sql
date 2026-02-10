ALTER TABLE course
  ALTER COLUMN credits TYPE NUMERIC(4,1)
  USING credits::numeric;

