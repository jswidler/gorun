-- +migrate Up
CREATE TABLE IF NOT EXISTS "gorun_trigger" (
  "id" varchar(32) NOT NULL PRIMARY KEY,
  "tenant_id" varchar(128),
  "created_at" timestamp NOT NULL,
  "updated_at" timestamp NOT NULL,

  "trigger_type" varchar(32) NOT NULL, -- "cron", "simple"
  "trigger_data" varchar(128) NOT NULL, -- "0 0 0 * * *"
  "scheduled_until" timestamp NOT NULL,

  "job_type" varchar(32) NOT NULL,
  "job_args" jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS "gorun_trigger_scheduled_until" ON "gorun_trigger" ("scheduled_until");

CREATE TABLE IF NOT EXISTS "gorun_job_data" (
  "id" varchar(32) NOT NULL PRIMARY KEY,
  "tenant_id" varchar(128),
  "created_at" timestamp NOT NULL,
  "updated_at" timestamp NOT NULL,
  "status" varchar(32) NOT NULL,
  "nonce" bigint NOT NULL,

  "trigger_id" varchar(32),

  "run_at" timestamp NOT NULL,

  "type" varchar(32) NOT NULL,
  "args" jsonb NOT NULL,
  "result" varchar(1000)
);

CREATE INDEX IF NOT EXISTS "gorun_job_data_status_run_at" ON "gorun_job_data" ("status", "run_at");
CREATE INDEX IF NOT EXISTS "gorun_job_data_status_updated_at" ON "gorun_job_data" ("status", "updated_at");

-- +migrate Down

DROP TABLE IF EXISTS "gorun_job_data";
DROP TABLE IF EXISTS "gorun_trigger";
