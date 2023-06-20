i=12
# for (i in seq_along(sql)) {
DBI::dbExecute(con, "drop table cohort_rows")
cat(sql[i])
DBI::dbExecute(con, sql[i])
# }


{
  s <- "CREATE TEMP TABLE final_cohort
AS
SELECT
person_id, min(start_date) as start_date, end_date
FROM
(
  SELECT
  c.person_id
  , c.start_date
  , MIN(ed.end_date) AS end_date
  FROM cohort_rows c
  JOIN (
         SELECT
         person_id
         , (event_date + INTERVAL'-1 * 7 day')  as end_date
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
             , (end_date + INTERVAL'7 day') as end_date
             , 1 AS event_type
             FROM cohort_rows
           ) RAWDATA
         ) e
         WHERE interval_status = 0
  ) ed ON c.person_id = ed.person_id AND ed.end_date >= c.start_date
  GROUP BY c.person_id, c.start_date
) e
group by person_id, end_date"}

DBI::dbExecute(con, s)


