# Create a new generated cohort set from a list of concept sets

Generate a new cohort set from one or more concept sets. Each concept
set will result in one cohort and represent the time during which the
concept was observed for each subject/person. Concept sets can be passed
to this function as:

- A named list of numeric vectors, one vector per concept set

- A named list of Capr concept sets

Clinical observation records will be looked up in the respective domain
tables using the vocabulary in the CDM. If a required domain table does
not exist in the cdm object a warning will be given. Concepts that are
not in the vocabulary or in the data will be silently ignored. If end
dates are missing or do not exist, as in the case of the procedure and
observation domains, the the start date will be used as the end date.

## Usage

``` r
generateConceptCohortSet(
  cdm,
  conceptSet = NULL,
  name,
  limit = "first",
  requiredObservation = c(0, 0),
  end = "observation_period_end_date",
  subsetCohort = NULL,
  subsetCohortId = NULL,
  overwrite = TRUE
)
```

## Arguments

- cdm:

  A cdm reference object created by
  [`CDMConnector::cdmFromCon`](cdmFromCon.md) or
  `CDMConnector::cdm_from_con`

- conceptSet:

  A named list of numeric vectors or a Concept Set Expression created
  [`omopgenerics::newConceptSetExpression`](https://darwin-eu.github.io/omopgenerics/reference/newConceptSetExpression.html)

- name:

  The name of the new generated cohort table as a character string

- limit:

  Include "first" (default) or "all" occurrences of events in the cohort

  - "first" will include only the first occurrence of any event in the
    concept set in the cohort.

  - "all" will include all occurrences of the events defined by the
    concept set in the cohort.

- requiredObservation:

  A numeric vector of length 2 that specifies the number of days of
  required observation time prior to index and post index for an event
  to be included in the cohort.

- end:

  How should the `cohort_end_date` be defined?

  - "observation_period_end_date" (default): The earliest
    observation_period_end_date after the event start date

  - numeric scalar: A fixed number of days from the event start date

  - "event_end_date": The event end date. If the event end date is not
    populated then the event start date will be used

- subsetCohort:

  A cohort table containing the individuals for which to generate
  cohorts for. Only individuals in the cohort table will appear in the
  created generated cohort set.

- subsetCohortId:

  A set of cohort IDs from the cohort table for which to include. If
  none are provided, all cohorts in the cohort table will be included.

- overwrite:

  Should the cohort table be overwritten if it already exists? TRUE
  (default) or FALSE.

## Value

A cdm reference object with the new generated cohort set table added
