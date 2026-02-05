/*
Project: Automated Healthcare Reporting Pipeline
Purpose: Transform wide REDCap survey data into long-format analytics view
Platform: Snowflake

Techniques:
- LATERAL FLATTEN
- regex parsing
- response normalization
- Tableau-ready modeling
*/
CREATE OR REPLACE VIEW <DB>.<SCHEMA>.SURVEY_Q1_TO_Q29_LONG AS
WITH src AS (
  SELECT
    /* Keep all columns, but exclude demographics we will re-derive cleanly */
    t.* EXCLUDE ("q38_sessionnumber", "q39_age", "q40_gender"),

    /* Clean demographics (empty strings -> NULL) */
    NULLIF(TRIM(t."q38_sessionnumber"), '') AS "q38_sessionnumber",
    NULLIF(TRIM(t."q39_age"), '')          AS "q39_age",
    NULLIF(TRIM(t."q40_gender"), '')       AS "q40_gender",

    /* Refresh timestamp (Mountain Time shown here as example) */
    CONVERT_TIMEZONE('UTC', 'America/Denver', CURRENT_TIMESTAMP()) AS data_refresh_timestamp,

    /* Snapshot the full row so we can GET() question fields safely */
    OBJECT_CONSTRUCT_KEEP_NULL(*) AS rowobj
  FROM <DB>.<SCHEMA>.<SOURCE_TABLE> t
)
SELECT
  /* keep all original fields (except helper object), plus refresh timestamp */
  s.* EXCLUDE (rowobj),
  s.data_refresh_timestamp,

  /* numeric question number parsed from the key (q10_... -> 10) */
  TRY_TO_NUMBER(REGEXP_SUBSTR(q.key::STRING, '[0-9]+')) AS qnum,

  /* user-friendly label, e.g., "Q10 Knowledgeable" */
  'Q' || TRY_TO_NUMBER(REGEXP_SUBSTR(q.key::STRING, '[0-9]+')) || ' ' ||
    INITCAP(
      REPLACE(
        REGEXP_REPLACE(q.key::STRING, '^q[0-9]+[_\\s-]*', ''),
        '_',
        ' '
      )
    ) AS question,

  /* raw response as stored */
  TRIM(q.value::STRING) AS response_raw,

  /* grouped response for Tableau */
  CASE
    WHEN LOWER(TRIM(q.value::STRING)) IN ('no', 'n', '0', '1', '1 - no') THEN 'No'

    WHEN LOWER(TRIM(q.value::STRING)) LIKE '%some extent%'
      OR TRIM(q.value::STRING) IN ('2', '2 - yes, to some extent') THEN 'Yes to some extent'

    WHEN LOWER(TRIM(q.value::STRING)) LIKE '%definitely%'
      OR TRIM(q.value::STRING) IN ('3', '3 - yes, definitely') THEN 'Yes definitely'

    WHEN LOWER(TRIM(q.value::STRING)) IN ('not applicable', 'n/a', 'na') THEN 'Not applicable'

    ELSE 'Other'
  END AS response_group,

  /* sort order for grouped responses */
  CASE
    WHEN LOWER(TRIM(q.value::STRING)) IN ('no', 'n', '0', '1', '1 - no') THEN 1
    WHEN LOWER(TRIM(q.value::STRING)) LIKE '%some extent%'
      OR TRIM(q.value::STRING) IN ('2', '2 - yes, to some extent') THEN 2
    WHEN LOWER(TRIM(q.value::STRING)) LIKE '%definitely%'
      OR TRIM(q.value::STRING) IN ('3', '3 - yes, definitely') THEN 3
    WHEN LOWER(TRIM(q.value::STRING)) IN ('not applicable', 'n/a', 'na') THEN 4
    ELSE 5
  END AS response_sort

FROM src s,
LATERAL FLATTEN(
  INPUT => OBJECT_CONSTRUCT_KEEP_NULL(
    /* ---- Survey questions (edit to match your exact column names) ---- */
    'q1_waitappropriate',    GET(s.rowobj, 'q1_waitappropriate'),
    'q2_simple',             GET(s.rowobj, 'q2_simple'),
    'q3_troubleaccess',      GET(s.rowobj, 'q3_troubleaccess'),
    'q4_welcome',            GET(s.rowobj, 'q4_welcome'),
    'q5_introduce',          GET(s.rowobj, 'q5_introduce'),
    'q6_listened',           GET(s.rowobj, 'q6_listened'),
    'q7_understoodneeds',    GET(s.rowobj, 'q7_understoodneeds'),
    'q8_explain',            GET(s.rowobj, 'q8_explain'),
    'q9_caring',             GET(s.rowobj, 'q9_caring'),
    'q10_knowledgeable',     GET(s.rowobj, 'q10_knowledgeable'),
    'q11_professional',      GET(s.rowobj, 'q11_professional'),
    'q12_preferencesvalues', GET(s.rowobj, 'q12_preferencesvalues'),
    'q13_stigmatized',       GET(s.rowobj, 'q13_stigmatized'),
    'q14_sayno',             GET(s.rowobj, 'q14_sayno'),
    'q15_education',         GET(s.rowobj, 'q15_education'),
    'q16_comfortable',       GET(s.rowobj, 'q16_comfortable'),
    'q17_personalized',      GET(s.rowobj, 'q17_personalized'),
    'q18_helpful',           GET(s.rowobj, 'q18_helpful'),
    'q19_reducesymptoms',    GET(s.rowobj, 'q19_reducesymptoms'),
    'q20_managechallenges',  GET(s.rowobj, 'q20_managechallenges'),
    'q21_hopeful',           GET(s.rowobj, 'q21_hopeful'),
    'q22_decisions',         GET(s.rowobj, 'q22_decisions'),
    'q23_includefamily',     GET(s.rowobj, 'q23_includefamily'),
    'q24_rightamount',       GET(s.rowobj, 'q24_rightamount'),
    'q25_othersupports',     GET(s.rowobj, 'q25_othersupports'),
    'q26_coordinated',       GET(s.rowobj, 'q26_coordinated'),
    'q27_wellnessplan',      GET(s.rowobj, 'q27_wellnessplan'),
    'q28_planlifegoals',     GET(s.rowobj, 'q28_planlifegoals'),
    'q29_connectresources',  GET(s.rowobj, 'q29_connectresources')

    /* Optional: include demographics/program fields too, if you want them in the long set
       (If you include them here, they will become rows in the long format—often NOT desired.)
       Better: keep them as normal columns (as we did in src) and join/group in Tableau.
    */
  )
) q
WHERE q.value IS NOT NULL
  AND TRIM(q.value::STRING) <> ''
  AND TRY_TO_NUMBER(REGEXP_SUBSTR(q.key::STRING, '[0-9]+')) IS NOT NULL;
