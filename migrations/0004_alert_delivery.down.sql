-- The old schema can retain only the latest escalation per pair.
DELETE FROM alert_hits a USING alert_hits b
 WHERE a.alert_id=b.alert_id AND a.story_id=b.story_id AND a.level=b.level AND a.generation<b.generation;
ALTER TABLE alert_hits DROP CONSTRAINT alert_hits_pkey;
ALTER TABLE alert_hits ADD PRIMARY KEY (alert_id, story_id, level);
ALTER TABLE alert_hits DROP COLUMN base_sources;
ALTER TABLE alert_hits DROP COLUMN approx;
ALTER TABLE alert_hits DROP COLUMN generation;
ALTER TABLE alerts DROP COLUMN digest_hour_utc;
ALTER TABLE alerts DROP COLUMN daily_digest;
