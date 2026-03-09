# Package index

### CDM Object

Create or transform a CDM reference object. These accept and return cdm
objects.

- [`cdmCommentContents()`](https://darwin-eu.github.io/CDMConnector/reference/cdmCommentContents.md)
  : Insert Patient CDM Contents as Aligned Comments in RStudio
- [`cdmCon()`](https://darwin-eu.github.io/CDMConnector/reference/cdmCon.md)
  : Get underlying database connection
- [`cdmDisconnect(`*`<db_cdm>`*`)`](https://darwin-eu.github.io/CDMConnector/reference/cdmDisconnect.db_cdm.md)
  : Disconnect the connection of the cdm object
- [`cdmFlatten()`](https://darwin-eu.github.io/CDMConnector/reference/cdmFlatten.md)
  : Flatten a cdm into a single observation table
- [`cdmFromCohortSet()`](https://darwin-eu.github.io/CDMConnector/reference/cdmFromCohortSet.md)
  : Build a Synthetic CDM from a Cohort Set
- [`cdmFromCon()`](https://darwin-eu.github.io/CDMConnector/reference/cdmFromCon.md)
  : Create a CDM reference object from a database connection
- [`cdmSample()`](https://darwin-eu.github.io/CDMConnector/reference/cdmSample.md)
  : Subset a cdm object to a random sample of individuals
- [`cdmSubset()`](https://darwin-eu.github.io/CDMConnector/reference/cdmSubset.md)
  : Subset a cdm object to a set of persons
- [`cdmSubsetCohort()`](https://darwin-eu.github.io/CDMConnector/reference/cdmSubsetCohort.md)
  : Subset a cdm to the individuals in one or more cohorts
- [`cdmWriteSchema()`](https://darwin-eu.github.io/CDMConnector/reference/cdmWriteSchema.md)
  : Get cdm write schema
- [`computeDataHashByTable()`](https://darwin-eu.github.io/CDMConnector/reference/computeDataHashByTable.md)
  : Compute a hash for each CDM table
- [`copyCdmTo()`](https://darwin-eu.github.io/CDMConnector/reference/copyCdmTo.md)
  : Copy a cdm object from one database to another
- [`dbSource()`](https://darwin-eu.github.io/CDMConnector/reference/dbSource.md)
  : Create a source for a cdm in a database.
- [`dropTable(`*`<db_cdm>`*`)`](https://darwin-eu.github.io/CDMConnector/reference/dropTable.db_cdm.md)
  : Drop table from a database backed cdm object
- [`snapshot()`](https://darwin-eu.github.io/CDMConnector/reference/snapshot.md)
  : Extract CDM metadata
- [`tblGroup()`](https://darwin-eu.github.io/CDMConnector/reference/tblGroup.md)
  : CDM table selection helper
- [`version()`](https://darwin-eu.github.io/CDMConnector/reference/version.md)
  : Get the CDM version

### Cohort Creation and Transformation

A cohort is a set of person-days representing the time during which
people in a CDM exhibited some observable characteristics. Cohorts are
often the foundation of downstream analyses.

- [`generateCohortSet()`](https://darwin-eu.github.io/CDMConnector/reference/generateCohortSet.md)
  : Generate a cohort set on a cdm object
- [`generateCohortSet2()`](https://darwin-eu.github.io/CDMConnector/reference/generateCohortSet2.md)
  : Generate a cohort set on a CDM object (optimized, no Java
  dependency)
- [`generateConceptCohortSet()`](https://darwin-eu.github.io/CDMConnector/reference/generateConceptCohortSet.md)
  : Create a new generated cohort set from a list of concept sets
- [`readCohortSet()`](https://darwin-eu.github.io/CDMConnector/reference/readCohortSet.md)
  : Read a set of cohort definitions into R
- [`summariseQuantile()`](https://darwin-eu.github.io/CDMConnector/reference/summariseQuantile.md)
  : Quantile calculation using dbplyr
- [`summariseQuantile2()`](https://darwin-eu.github.io/CDMConnector/reference/summariseQuantile2.md)
  : Quantile calculation using dbplyr

### dbplyr workarounds

Functions that can be used in cross database dplyr pipelines

- [`appendPermanent()`](https://darwin-eu.github.io/CDMConnector/reference/appendPermanent.md)
  : Run a dplyr query and add the result set to an existing
- [`asDate()`](https://darwin-eu.github.io/CDMConnector/reference/asDate.md)
  : as.Date dbplyr translation wrapper
- [`computeQuery()`](https://darwin-eu.github.io/CDMConnector/reference/computeQuery.md)
  : Execute dplyr query and save result in remote database
- [`dateadd()`](https://darwin-eu.github.io/CDMConnector/reference/dateadd.md)
  : Add days or years to a date in a dplyr query
- [`datediff()`](https://darwin-eu.github.io/CDMConnector/reference/datediff.md)
  : Compute the difference between two days
- [`datepart()`](https://darwin-eu.github.io/CDMConnector/reference/datepart.md)
  : Extract the day, month or year of a date in a dplyr pipeline
- [`inSchema()`](https://darwin-eu.github.io/CDMConnector/reference/inSchema.md)
  : Helper for working with compound schema

### DBI connection

Functions that accept DBI connections and are useful in cross database
DBI code

- [`dbms()`](https://darwin-eu.github.io/CDMConnector/reference/dbms.md)
  : Get the database management system (dbms) from a cdm_reference or
  DBI connection
- [`listTables()`](https://darwin-eu.github.io/CDMConnector/reference/listTables.md)
  : List tables in a schema

### Eunomia example CDM

Easily create and use example CDMs in a [duckdb](https://duckdb.org/)
database

- [`downloadEunomiaData()`](https://darwin-eu.github.io/CDMConnector/reference/downloadEunomiaData.md)
  : Download Eunomia data files

- [`eunomiaDir()`](https://darwin-eu.github.io/CDMConnector/reference/eunomiaDir.md)
  : Create a copy of an example OMOP CDM dataset

- [`eunomiaIsAvailable()`](https://darwin-eu.github.io/CDMConnector/reference/eunomiaIsAvailable.md)
  : Has the Eunomia dataset been cached?

- [`exampleDatasets()`](https://darwin-eu.github.io/CDMConnector/reference/exampleDatasets.md)
  : List the available example CDM datasets

- [`requireEunomia()`](https://darwin-eu.github.io/CDMConnector/reference/requireEunomia.md)
  :

  Require eunomia to be available. The function makes sure that you can
  later create a eunomia database with
  [`eunomiaDir()`](https://darwin-eu.github.io/CDMConnector/reference/eunomiaDir.md).

### Benchmarking

Run benchmarking of simple queries against your CDM reference

- [`benchmarkCDMConnector()`](https://darwin-eu.github.io/CDMConnector/reference/benchmarkCDMConnector.md)
  : Run benchmark of tasks using CDMConnector

### Concept discovery

Find concepts that co-occur with given OMOP concepts using the Columbia
Open Health Data (COHD) API.

- [`cohdSimilarConcepts()`](https://darwin-eu.github.io/CDMConnector/reference/cohdSimilarConcepts.md)
  : Get similar concepts from Columbia Open Health Data (COHD) API
