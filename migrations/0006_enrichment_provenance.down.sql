ALTER TABLE stories DROP COLUMN IF EXISTS place_fallback;
ALTER TABLE stories DROP COLUMN IF EXISTS place_degraded;
ALTER TABLE stories DROP COLUMN IF EXISTS place_backend;
ALTER TABLE stories DROP COLUMN IF EXISTS cat_conf;
ALTER TABLE stories DROP COLUMN IF EXISTS category_backend;
ALTER TABLE stories DROP COLUMN IF EXISTS fallback;
ALTER TABLE stories DROP COLUMN IF EXISTS degraded;
ALTER TABLE stories DROP COLUMN IF EXISTS title_source;
ALTER TABLE stories DROP COLUMN IF EXISTS brief_source;

ALTER TABLE story_locations DROP COLUMN IF EXISTS country;
ALTER TABLE article_locations DROP COLUMN IF EXISTS country;
