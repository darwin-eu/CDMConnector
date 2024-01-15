CREATE TEMPORARY TABLE Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;
INSERT INTO Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from "OMOP_SYNTHETIC_DATASET"."CDM53".CONCEPT where concept_id in (4111714,4102202,4046443,762938,762811,4048787,4048786,4043735,4120316,4194609,4179912,4111713,314667,764503,4116206,4047634,4043901,4121335,4119136,4041680,4100225,764716,4217471,4104695,4319332,4167985,764712,764714,764708,763149,4100224,4098706,4277833,764710,764726,4228209,4234264,762828,4319329,4048890,4057329,764723,4102203,4290940,4079905,4105338)
) I
) C;
CREATE TABLE qualified_events
 AS
SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM (-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC, E.event_id) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.start_date, C.end_date,
  C.visit_occurrence_id, C.start_date as sort_date
FROM 
(
  SELECT co.person_id,co.condition_occurrence_id,co.condition_concept_id,co.visit_occurrence_id,co.condition_start_date as start_date, COALESCE(co.condition_end_date, DATEADD(day,1,co.condition_start_date)) as end_date 
  FROM "OMOP_SYNTHETIC_DATASET"."CDM53".CONDITION_OCCURRENCE co
  JOIN Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)
) C
-- End Condition Occurrence Criteria
  ) E
	JOIN "OMOP_SYNTHETIC_DATASET"."CDM53".observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P
-- End Primary Events
) pe
) QE
;
--- Inclusion Rule Inserts
create temporary table inclusion_events (inclusion_rule_id bigint,
	person_id bigint,
	event_id bigint
);
CREATE TABLE included_events
 AS
SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date
FROM
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from qualified_events Q
    LEFT JOIN inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups
) Results
;
-- date offset strategy
CREATE TABLE strategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATEADD(day,0,start_date) > op_end_date then op_end_date else DATEADD(day,0,start_date) end as end_date
FROM
included_events;
-- generate cohort periods into final_cohort
CREATE TABLE cohort_rows
 AS
SELECT
person_id, start_date, end_date
FROM
( -- first_ends
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, CE.end_date, row_number() over (partition by I.person_id, I.event_id order by CE.end_date) as ordinal
	  from included_events I
	  join ( -- cohort_ends
-- cohort exit dates
-- End Date Strategy
SELECT event_id, person_id, end_date from strategy_ends
    ) CE on I.event_id = CE.event_id and I.person_id = CE.person_id and CE.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
) FE;
CREATE TABLE final_cohort
 AS
SELECT
person_id, min(start_date) as start_date, end_date
FROM
( --cteEnds
	SELECT
		 c.person_id
		, c.start_date
		, MIN(ed.end_date) AS end_date
	FROM cohort_rows c
	JOIN ( -- cteEndDates
    SELECT
      person_id
      , DATEADD(day,-1 * 30, event_date)  as end_date
    FROM
    (
      SELECT
        person_id
        , event_date
        , event_type
        , SUM(event_type) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS interval_status
      FROM
      (
        SELECT
          person_id
          , start_date AS event_date
          , -1 AS event_type
        FROM cohort_rows
        UNION ALL
        SELECT
          person_id
          , DATEADD(day,30,end_date) as end_date
          , 1 AS event_type
        FROM cohort_rows
      ) RAWDATA
    ) e
    WHERE interval_status = 0
  ) ed ON c.person_id = ed.person_id AND ed.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
) e
group by person_id, end_date
;
DELETE FROM "ATLAS"."RESULTS"."temp23019_chrt0" where "cohort_definition_id" = 1;
INSERT INTO "ATLAS"."RESULTS"."temp23019_chrt0" ("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
select 1 as "cohort_definition_id", person_id, start_date, end_date 
FROM final_cohort CO
;
-- BEGIN: Censored Stats
delete from "ATLAS"."RESULTS"."temp23019_chrt0_censor_stats" where "cohort_definition_id" = 1;
-- END: Censored Stats
TRUNCATE TABLE strategy_ends;
DROP TABLE strategy_ends;
TRUNCATE TABLE cohort_rows;
DROP TABLE cohort_rows;
TRUNCATE TABLE final_cohort;
DROP TABLE final_cohort;
TRUNCATE TABLE inclusion_events;
DROP TABLE inclusion_events;
TRUNCATE TABLE qualified_events;
DROP TABLE qualified_events;
TRUNCATE TABLE included_events;
DROP TABLE included_events;
TRUNCATE TABLE Codesets;
DROP TABLE Codesets;
