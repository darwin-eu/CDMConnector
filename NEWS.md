# CDMConnector 2.3.0

- Add empty CDM v5.4 example dataset @ginberg
- Support integer64 type for personId argument in `cdmSubset` @elinrow
- Support local CDM objects in `generateConceptCohortSet` and date functions @catalamarti
- Fix bug with cohortCodelist creation for some cohorts @sulevR
- Update cohortCollapse code @ablack3

# CDMConnector 2.2.0

- Test using DatabaseConnector version 7 JDBC connections on all supported databases @IoannaNika
- Change summariseQuantile2 output columns to match DrugExposureDiagnostics (e.g. q05, q10)
- Add `computeDataHash` argument to `snapshot()`
- Add `computeDataHashByTable` function to compute a hash for each CDM table
- Update connection examples documentation 
- Change Eunomia dataset download link
- Add Synthea27NjParquet example dataset

# CDMConnector 2.1.1

- fix bug with cohort generation @catalamarti
- fix bug in summariseQuantile2 @ablack3

# CDMConnector 2.1.0

-   Add support for Redshift and Spark with DatabaseConnector driver (pending version 7) @IoannaNika
-   Update readme instructions to use camel case functions @ginberg
-   New `summariseQuantile2` function that supports multiple variables @ablack3
-   Fixed the returned table references @mvankessel-EMC
-   Validate the writePrefix @xihang-chen

# CDMConnector 2.0.0

-   Remove snake case functions in favor of only camel case style
-   Remove cohort table manipulation functions
-   Add row to cohort attrition table that accounts for record collapsing cohort eras step in Atlas/Circe cohort generation
-   Increase test coverage
-   Fix bugs with `cdmFlatten`
-   Add support for snowflake with DatabaseConnector driver
-   Add attrition record for cohort era collapse when generating Atlas cohorts

# CDMConnector 1.7.0

-   Add support for Big Query using bigrquery DBI package (pending PRs on omopgenerics and bigrquery) @IoannaNika
-   Add support for the DatabaseConnector postgresql JDBC driver @ablack3
-   By default automatically detect CDM version @catalamarti
-   Deprecate snake case functions @IoannaNika
-   Deprecate cohort manipulation functions @ablack3
-   Deprecate validation functions @ablack3

# CDMConnector 1.6.1

-   use BIGINT for subject_id column of cohort tables

# CDMConnector 1.6.0

-   fix bug in copyCdmTo where attribute tables were not being copied #231 @catalamarti
-   check that overwrite argument works in compute when using temp tables #222 @ablack3
-   added synpuf1k with Achilles tables to example datasets #230 @ablack3
-   add requireEunomia function #481 @catalamarti

# CDMConnector 1.5.0

-   Get all tests passing on Databricks/Spark using odbc driver
-   Emulate temporary tables on Databricks/Spark when compute is called with temporary = TRUE
-   soft deprecate asDate in favor of as.Date
-   soft deprecate assertWriteSchema since cdm object are now required to always have a write schema
-   remove support for Capr cohort objects in generate_cohort_set to pass CRAN checks

# CDMConnector 1.4.0

-   fix issue on cran server
-   new omopgenerics methods

# CDMConnector 1.3.1

-   Bug fix for collapse cohort
-   dbplyr 2.5.0 fixes

# CDMConnector 1.3.0

-   Incorporate omopgenerics

# CDMConnector 1.2.1

-   Fix failing CRAN tests

# CDMConnector 1.2.0

-   Compatibility with dbplyr v2.4.0

# CDMConnector 1.1.4

-   Fix recursive edge cases with cdm reference issue.

# CDMConnector 1.1.3

-   Fix recursive cdm reference issue. Thanks @catalamarti!

# CDMConnector 1.1.2

-   fix failing test on CRAN

# CDMConnector 1.1.1

-   add some bigquery support

August 22, 2023

# CDMConnector 1.1.0

-   add `copy_cdm_to`, `copyCdmTo`
-   add `generate_concept_cohort_set`, `generateConceptCohortSet`
-   add more example CDM datasets
-   add `record_cohort_attrition`, `recordCohortAttrition`
-   improve database test coverage
-   update vignettes

Released Aug 17, 2023

# CDMConnector 1.0.0

-   remove `write_prefix` in `cdm_from_con` in favor of using the `write_schema` argument for prefixing
-   remove `cdm_tables` argument from `cdm_from_con` in favor of `cdm_select_tbl` selection function
-   add attributes to the cdm object to communicate downstream temp table preferences

Released June 7, 2023

# CDMConnector 0.6.0

-   generate Capr cohorts with `generateCohortSet`
-   add datepart function for extracting year, month, day parts of dates in dplyr
-   fix datediff logic for years so it now returns number of complete years between two date
-   add "write_prefix" attribute to cdm objects to support a namespace within the write_schema

Released May 5, 2023

# CDMConnector 0.5.1

-   Add both camelCase and snake_case versions of all functions (@Tsemharb)
-   Add cdm object attributes to `cdm_snapshot` output
-   use `number_records` and `number_subjects` in cohort counts table
-   Minor bug fixes

Released on March 20, 2023

# CDMConnector 0.5.0

-   Define `generatedCohortSet` class
-   Add `summarize_quantile` for cross database quantile queries
-   Add `GeneratedCohortSet` object, constructor, and attribute accessor functions
-   Add vignette on cohort generation
-   Add `cdmSubset`, `cdmSubsetCohort`, `cdmSample` for subsetting a cdm
-   Add `cdmFlatten` for transforming a cdm into a single flat table of observations
-   Improve test coverage
-   Make Java dependency optional
-   `cdm_schema` is now required in `cdm_from_con` except for duckdb connections
-   Remove visit_detail from default cdm tables
-   Deprecate `computePermanent`
-   Improve package website

Substantial contributions by @edward-burn, @catalamarti, @Tsemharb who are now package co-authors.

Released Mar 8, 2023

# CDMConnector 0.4.1

-   Add `computeQuery` function for creating temp and persistent tables from dplyr queries
-   Download Eunomia from darwin-eu repository as zipped csv files
-   Add passing tests for Oracle and Spark
-   Add `asDate` wrapper that provides correct as.Date dbplyr translations
-   Incorporate code review suggestions from @mvankessel-EMC

Released Jan 21, 2023

# CDMConnector 0.3.0

-   Add computePermanent and appendPermanent functions
-   Add readCohortSet
-   Add generateCohortSet
-   Extract Eunomia dataset so that it is downloaded separately

Released Nov 29, 2022

# CDMConnector 0.2.0

-   Add support for OMOP v5.3 and v5.4
-   Add datediff and dateadd functions that can be used in dplyr::mutate()
-   Add assert_tables function for checking that a cdm object has required tables
-   Add camelCase versions of several functions

Released Nov 15, 2022

# CDMConnector 0.1.0

-   Initial release Sept 11, 2022
-   Added a `NEWS.md` file to track changes to the package.
