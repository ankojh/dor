-- Repair legacy brief provenance after 0006.
--
-- Before F1·2 every story brief was seeded from an article lede. 0006 cleared
-- the exact same-story copies but marked every remaining non-empty brief
-- "generated", including ledes from reassigned/missing articles. 0007 makes
-- provenance evidence-based:
--   * an exact or 60-character prefix match against any article lede/description
--     is source text, never an agent brief;
--   * a "generated" brief with no durable model evidence is legacy text;
--   * only a brief written with brief_model (set by the Editor from now on)
--     may keep "generated".

ALTER TABLE stories ADD COLUMN IF NOT EXISTS brief_model text NOT NULL DEFAULT '';

-- Covers reassigned articles and truncation differences by matching across all
-- stories, in either direction.
UPDATE stories AS s
SET brief_source = 'source_snippet'
WHERE s.brief_source = 'generated'
  AND s.brief_model = ''
  AND btrim(s.brief) <> ''
  AND EXISTS (
      SELECT 1
      FROM articles AS a
      WHERE a.lede <> ''
        AND (
            a.lede = s.brief
            OR left(a.lede, 60) = left(s.brief, 60)
            OR a.lede LIKE '%' || left(s.brief, 60) || '%'
            OR s.brief LIKE '%' || left(a.lede, 60) || '%'
        )
  );

-- Any other non-empty "generated" brief with no model evidence came from the
-- 0006 blanket label. Keep the text but never call it an agent brief.
UPDATE stories
SET brief_source = 'source_snippet'
WHERE brief_source = 'generated'
  AND brief_model = ''
  AND btrim(brief) <> '';

-- A generated label with no text is no brief at all.
UPDATE stories
SET brief_source = 'none'
WHERE brief_source = 'generated'
  AND brief_model = ''
  AND btrim(brief) = '';
