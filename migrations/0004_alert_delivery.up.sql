-- Preserve each source-doubling generation, including approximate firings.
ALTER TABLE alerts ADD COLUMN daily_digest boolean NOT NULL DEFAULT false;
ALTER TABLE alerts ADD COLUMN digest_hour_utc integer NOT NULL DEFAULT 0 CHECK (digest_hour_utc BETWEEN 0 AND 23);
ALTER TABLE alert_hits ADD COLUMN generation bigint NOT NULL DEFAULT 0 CHECK (generation >= 0);
ALTER TABLE alert_hits ADD COLUMN approx boolean NOT NULL DEFAULT false;
ALTER TABLE alert_hits ADD COLUMN base_sources integer NOT NULL DEFAULT 0;
UPDATE alert_hits h SET base_sources=COALESCE(
 (SELECT NULLIF(n.source_count,0) FROM alert_hits n WHERE n.alert_id=h.alert_id AND n.story_id=h.story_id AND n.level='new'),
 GREATEST(1,CASE WHEN h.level='escalated' THEN h.source_count/2 ELSE h.source_count END));
UPDATE alert_hits SET generation=GREATEST(1,floor(log(2,GREATEST(1,source_count::numeric / GREATEST(1,base_sources))))) WHERE level='escalated';
ALTER TABLE alert_hits DROP CONSTRAINT alert_hits_pkey;
ALTER TABLE alert_hits ADD PRIMARY KEY (alert_id, story_id, generation);
