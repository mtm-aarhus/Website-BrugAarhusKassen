/* ============================================================================
   Migration: surface Sommersæson + Vintersæson selections on the tilladelse
   so sagsbehandler can see them in Kassen.

   - Sommersaeson nvarchar(10) — "Ja", "Nej" or NULL (unanswered)
   - Vintermaaneder nvarchar(100) — comma-separated Danish month names in
     winter-season order, e.g. "Oktober, November, December". NULL if none.

   The billable-month logic itself remains computed live in the refresh —
   these are display-only mirrors.

   Run as a single batch in SSMS. Idempotent.
   ============================================================================ */

SET XACT_ABORT ON;
BEGIN TRANSACTION;

IF COL_LENGTH('dbo.BrugAarhus_Udeservering', 'Sommersaeson') IS NULL
    ALTER TABLE dbo.BrugAarhus_Udeservering ADD Sommersaeson nvarchar(10) NULL;

IF COL_LENGTH('dbo.BrugAarhus_Udeservering', 'Vintermaaneder') IS NULL
    ALTER TABLE dbo.BrugAarhus_Udeservering ADD Vintermaaneder nvarchar(100) NULL;

COMMIT;

/* ---------- Sanity check ---------- */
SELECT
    c.name        AS [column],
    TYPE_NAME(c.user_type_id)
        + CASE
            WHEN TYPE_NAME(c.user_type_id) IN ('nvarchar','nchar') THEN '(' + CAST(c.max_length / 2 AS varchar) + ')'
            WHEN TYPE_NAME(c.user_type_id) IN ('varchar','char') THEN '(' + CAST(c.max_length AS varchar) + ')'
            ELSE ''
          END     AS [type],
    c.is_nullable AS [nullable]
FROM sys.columns c
JOIN sys.tables t ON t.object_id = c.object_id
WHERE t.name = 'BrugAarhus_Udeservering'
  AND c.name IN ('Sommersaeson', 'Vintermaaneder')
ORDER BY c.column_id;
