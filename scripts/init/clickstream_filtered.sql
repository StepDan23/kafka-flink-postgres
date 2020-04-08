DROP TABLE IF EXISTS clickstream_filtered CASCADE;
DROP TYPE IF EXISTS REACTION_TYPE;

CREATE TYPE REACTION_TYPE AS ENUM ('Dislike', 'Show', 'Click', 'Complete', 'Like');

CREATE TABLE clickstream_filtered
(
  "epk_id" integer NOT NULL,
  "content_id" integer NOT NULL,
  "event_type" REACTION_TYPE,
  "event_ts" integer  NOT NULL,
  "insert_ts" integer  NOT NULL
);
