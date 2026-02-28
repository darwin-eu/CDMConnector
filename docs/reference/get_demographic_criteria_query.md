# Get demographic criteria query

Get demographic criteria query

## Usage

``` r
get_demographic_criteria_query(
  dc,
  event_table,
  index_id,
  cdm_schema = "@cdm_database_schema"
)
```

## Arguments

- dc:

  List - DemographicCriteria (OccurrenceStartDate, OccurrenceEndDate,
  Age, Gender, etc.)

- event_table:

  Event table expression

- index_id:

  Index for this criteria

- cdm_schema:

  CDM schema
