# Changelog

## CDMConnector 2.4.0

- Implement generateCohortSet for local cdms (e.g. with omock)
- Allow catalogs in duckdb connections
  [@lyoganathan](https://github.com/lyoganathan)
- Add `cdmFromCohortSet` to create synthetic cdms from Atlas cohort
  definitions
- Add `cdmCommentContents` for creating comments with the contents of a
  CDM in tests and documentation

## CDMConnector 2.3.0

CRAN release: 2026-01-10

- Add empty CDM v5.4 example dataset
  [@ginberg](https://github.com/ginberg)
- Support integer64 type for personId argument in `cdmSubset`
  [@elinrow](https://github.com/elinrow)
- Support local CDM objects in `generateConceptCohortSet` and date
  functions [@catalamarti](https://github.com/catalamarti)
- Fix bug with cohortCodelist creation for some cohorts
  [@sulevR](https://github.com/sulevR)
- Simplify `cohortCollapse` logic and support local cdm tables
  [@ablack3](https://github.com/ablack3)

## CDMConnector 2.2.0

CRAN release: 2025-09-16

- Test using DatabaseConnector version 7 JDBC connections on all
  supported databases [@IoannaNika](https://github.com/IoannaNika)
- Change summariseQuantile2 output columns to match
  DrugExposureDiagnostics (e.g. q05, q10)
- Add `computeDataHash` argument to
  [`snapshot()`](../reference/snapshot.md)
- Add `computeDataHashByTable` function to compute a hash for each CDM
  table
- Update connection examples documentation
- Change Eunomia dataset download link
- Add Synthea27NjParquet example dataset

## CDMConnector 2.1.1

CRAN release: 2025-07-10

- fix bug with cohort generation
  [@catalamarti](https://github.com/catalamarti)
- fix bug in summariseQuantile2 [@ablack3](https://github.com/ablack3)

## CDMConnector 2.1.0

CRAN release: 2025-06-25

- Add support for Redshift and Spark with DatabaseConnector driver
  (pending version 7) [@IoannaNika](https://github.com/IoannaNika)
- Update readme instructions to use camel case functions
  [@ginberg](https://github.com/ginberg)
- New `summariseQuantile2` function that supports multiple variables
  [@ablack3](https://github.com/ablack3)
- Fixed the returned table references
  [@mvankessel-EMC](https://github.com/mvankessel-EMC)
- Validate the writePrefix
  [@xihang-chen](https://github.com/xihang-chen)

## CDMConnector 2.0.0

CRAN release: 2025-02-21

- Remove snake case functions in favor of only camel case style
- Remove cohort table manipulation functions
- Add row to cohort attrition table that accounts for record collapsing
  cohort eras step in Atlas/Circe cohort generation
- Increase test coverage
- Fix bugs with `cdmFlatten`
- Add support for snowflake with DatabaseConnector driver
- Add attrition record for cohort era collapse when generating Atlas
  cohorts

## CDMConnector 1.7.0

CRAN release: 2024-12-19

- Add support for Big Query using bigrquery DBI package (pending PRs on
  omopgenerics and bigrquery)
  [@IoannaNika](https://github.com/IoannaNika)
- Add support for the DatabaseConnector postgresql JDBC driver
  [@ablack3](https://github.com/ablack3)
- By default automatically detect CDM version
  [@catalamarti](https://github.com/catalamarti)
- Deprecate snake case functions
  [@IoannaNika](https://github.com/IoannaNika)
- Deprecate cohort manipulation functions
  [@ablack3](https://github.com/ablack3)
- Deprecate validation functions [@ablack3](https://github.com/ablack3)

## CDMConnector 1.6.1

CRAN release: 2024-11-29

- use BIGINT for subject_id column of cohort tables

## CDMConnector 1.6.0

CRAN release: 2024-11-13

- fix bug in copyCdmTo where attribute tables were not being copied
  [\#231](https://github.com/darwin-eu/CDMConnector/issues/231)
  [@catalamarti](https://github.com/catalamarti)
- check that overwrite argument works in compute when using temp tables
  [\#222](https://github.com/darwin-eu/CDMConnector/issues/222)
  [@ablack3](https://github.com/ablack3)
- added synpuf1k with Achilles tables to example datasets
  [\#230](https://github.com/darwin-eu/CDMConnector/issues/230)
  [@ablack3](https://github.com/ablack3)
- add requireEunomia function
  [\#481](https://github.com/darwin-eu/CDMConnector/issues/481)
  [@catalamarti](https://github.com/catalamarti)

## CDMConnector 1.5.0

CRAN release: 2024-07-16

- Get all tests passing on Databricks/Spark using odbc driver
- Emulate temporary tables on Databricks/Spark when compute is called
  with temporary = TRUE
- soft deprecate asDate in favor of as.Date
- soft deprecate assertWriteSchema since cdm object are now required to
  always have a write schema
- remove support for Capr cohort objects in generate_cohort_set to pass
  CRAN checks

## CDMConnector 1.4.0

CRAN release: 2024-05-03

- fix issue on cran server
- new omopgenerics methods

## CDMConnector 1.3.1

CRAN release: 2024-04-02

- Bug fix for collapse cohort
- dbplyr 2.5.0 fixes

## CDMConnector 1.3.0

CRAN release: 2024-02-05

- Incorporate omopgenerics

## CDMConnector 1.2.1

CRAN release: 2024-01-18

- Fix failing CRAN tests

## CDMConnector 1.2.0

CRAN release: 2023-10-31

- Compatibility with dbplyr v2.4.0

## CDMConnector 1.1.4

CRAN release: 2023-10-20

- Fix recursive edge cases with cdm reference issue.

## CDMConnector 1.1.3

CRAN release: 2023-10-14

- Fix recursive cdm reference issue. Thanks
  [@catalamarti](https://github.com/catalamarti)!

## CDMConnector 1.1.2

CRAN release: 2023-08-22

- fix failing test on CRAN

## CDMConnector 1.1.1

CRAN release: 2023-08-22

- add some bigquery support

August 22, 2023

## CDMConnector 1.1.0

CRAN release: 2023-08-17

- add `copy_cdm_to`, `copyCdmTo`
- add `generate_concept_cohort_set`, `generateConceptCohortSet`
- add more example CDM datasets
- add `record_cohort_attrition`, `recordCohortAttrition`
- improve database test coverage
- update vignettes

Released Aug 17, 2023

## CDMConnector 1.0.0

CRAN release: 2023-06-12

- remove `write_prefix` in `cdm_from_con` in favor of using the
  `write_schema` argument for prefixing
- remove `cdm_tables` argument from `cdm_from_con` in favor of
  `cdm_select_tbl` selection function
- add attributes to the cdm object to communicate downstream temp table
  preferences

Released June 7, 2023

## CDMConnector 0.6.0

CRAN release: 2023-05-05

- generate Capr cohorts with `generateCohortSet`
- add datepart function for extracting year, month, day parts of dates
  in dplyr
- fix datediff logic for years so it now returns number of complete
  years between two date
- add “write_prefix” attribute to cdm objects to support a namespace
  within the write_schema

Released May 5, 2023

## CDMConnector 0.5.1

CRAN release: 2023-03-22

- Add both camelCase and snake_case versions of all functions
  ([@Tsemharb](https://github.com/Tsemharb))
- Add cdm object attributes to `cdm_snapshot` output
- use `number_records` and `number_subjects` in cohort counts table
- Minor bug fixes

Released on March 20, 2023

## CDMConnector 0.5.0

CRAN release: 2023-03-09

- Define `generatedCohortSet` class
- Add `summarize_quantile` for cross database quantile queries
- Add `GeneratedCohortSet` object, constructor, and attribute accessor
  functions
- Add vignette on cohort generation
- Add `cdmSubset`, `cdmSubsetCohort`, `cdmSample` for subsetting a cdm
- Add `cdmFlatten` for transforming a cdm into a single flat table of
  observations
- Improve test coverage
- Make Java dependency optional
- `cdm_schema` is now required in `cdm_from_con` except for duckdb
  connections
- Remove visit_detail from default cdm tables
- Deprecate `computePermanent`
- Improve package website

Substantial contributions by
[@edward-burn](https://github.com/edward-burn),
[@catalamarti](https://github.com/catalamarti),
[@Tsemharb](https://github.com/Tsemharb) who are now package co-authors.

Released Mar 8, 2023

## CDMConnector 0.4.1

CRAN release: 2023-01-24

- Add `computeQuery` function for creating temp and persistent tables
  from dplyr queries
- Download Eunomia from darwin-eu repository as zipped csv files
- Add passing tests for Oracle and Spark
- Add `asDate` wrapper that provides correct as.Date dbplyr translations
- Incorporate code review suggestions from
  [@mvankessel-EMC](https://github.com/mvankessel-EMC)

Released Jan 21, 2023

## CDMConnector 0.3.0

CRAN release: 2022-11-29

- Add computePermanent and appendPermanent functions
- Add readCohortSet
- Add generateCohortSet
- Extract Eunomia dataset so that it is downloaded separately

Released Nov 29, 2022

## CDMConnector 0.2.0

- Add support for OMOP v5.3 and v5.4
- Add datediff and dateadd functions that can be used in dplyr::mutate()
- Add assert_tables function for checking that a cdm object has required
  tables
- Add camelCase versions of several functions

Released Nov 15, 2022

## CDMConnector 0.1.0

CRAN release: 2022-09-29

- Initial release Sept 11, 2022
- Added a `NEWS.md` file to track changes to the package.
