-- Provenance for enrichment and briefs.
--
-- articles already carries category/cat_conf/lang; article_locations already
-- carries the place votes. This migration adds the canonical country code so
-- the region filter and the panel can use the resolved place, not the raw
-- feed hint.

ALTER TABLE article_locations ADD COLUMN IF NOT EXISTS country text NOT NULL DEFAULT '';
ALTER TABLE story_locations ADD COLUMN IF NOT EXISTS country text NOT NULL DEFAULT '';

ALTER TABLE stories ADD COLUMN IF NOT EXISTS brief_source text NOT NULL DEFAULT 'none';
ALTER TABLE stories ADD COLUMN IF NOT EXISTS title_source text NOT NULL DEFAULT 'source';
ALTER TABLE stories ADD COLUMN IF NOT EXISTS degraded boolean NOT NULL DEFAULT false;
ALTER TABLE stories ADD COLUMN IF NOT EXISTS fallback boolean NOT NULL DEFAULT false;
ALTER TABLE stories ADD COLUMN IF NOT EXISTS category_backend text NOT NULL DEFAULT '';
ALTER TABLE stories ADD COLUMN IF NOT EXISTS cat_conf real NOT NULL DEFAULT 0;
ALTER TABLE stories ADD COLUMN IF NOT EXISTS place_backend text NOT NULL DEFAULT '';
ALTER TABLE stories ADD COLUMN IF NOT EXISTS place_degraded boolean NOT NULL DEFAULT false;
ALTER TABLE stories ADD COLUMN IF NOT EXISTS place_fallback boolean NOT NULL DEFAULT false;

-- Repair the old behaviour that copied the first article lede into the story
-- brief. Only clear a brief that is exactly one of the story's article ledes;
-- an editor-written brief is left alone.
UPDATE stories AS s
SET brief = '', brief_source = 'none'
WHERE s.brief <> ''
  AND EXISTS (
      SELECT 1 FROM articles AS a
      WHERE a.story_id = s.id AND a.lede <> '' AND a.lede = s.brief
  );

UPDATE stories
SET brief_source = 'generated'
WHERE brief <> '' AND brief_source = 'none';
