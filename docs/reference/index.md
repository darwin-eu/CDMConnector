# Package index

### CDM Object

Create or transform a CDM reference object. These accept and return cdm
objects.

- [`cdmCon()`](cdmCon.md) : Get underlying database connection
- [`cdmDisconnect(`*`<db_cdm>`*`)`](cdmDisconnect.db_cdm.md) :
  Disconnect the connection of the cdm object
- [`cdmFlatten()`](cdmFlatten.md) : Flatten a cdm into a single
  observation table
- [`cdmFromCon()`](cdmFromCon.md) : Create a CDM reference object from a
  database connection
- [`cdmSample()`](cdmSample.md) : Subset a cdm object to a random sample
  of individuals
- [`cdmSubset()`](cdmSubset.md) : Subset a cdm object to a set of
  persons
- [`cdmSubsetCohort()`](cdmSubsetCohort.md) : Subset a cdm to the
  individuals in one or more cohorts
- [`cdmWriteSchema()`](cdmWriteSchema.md) : Get cdm write schema
- [`computeDataHashByTable()`](computeDataHashByTable.md) : Compute a
  hash for each CDM table
- [`copyCdmTo()`](copyCdmTo.md) : Copy a cdm object from one database to
  another
- [`dbSource()`](dbSource.md) : Create a source for a cdm in a database.
- [`dropTable(`*`<db_cdm>`*`)`](dropTable.db_cdm.md) : Drop table from a
  database backed cdm object
- [`snapshot()`](snapshot.md) : Extract CDM metadata
- [`tblGroup()`](tblGroup.md) : CDM table selection helper
- [`version()`](version.md) : Get the CDM version

### Cohort Creation and Transformation

A cohort is a set of person-days representing the time during which
people in a CDM exhibited some observable characteristics. Cohorts are
often the foundation of downstream analyses.

- [`generateCohortSet()`](generateCohortSet.md) **\[experimental\]** :
  Generate a cohort set on a cdm object
- [`generateConceptCohortSet()`](generateConceptCohortSet.md) : Create a
  new generated cohort set from a list of concept sets
- [`readCohortSet()`](readCohortSet.md) : Read a set of cohort
  definitions into R
- [`summariseQuantile()`](summariseQuantile.md) : Quantile calculation
  using dbplyr
- [`summariseQuantile2()`](summariseQuantile2.md) : Quantile calculation
  using dbplyr

### dbplyr workarounds

Functions that can be used in cross database dplyr pipelines

- [`appendPermanent()`](appendPermanent.md) : Run a dplyr query and add
  the result set to an existing
- [`asDate()`](asDate.md) : as.Date dbplyr translation wrapper
- [`computeQuery()`](computeQuery.md) : Execute dplyr query and save
  result in remote database
- [`dateadd()`](dateadd.md) : Add days or years to a date in a dplyr
  query
- [`datediff()`](datediff.md) : Compute the difference between two days
- [`datepart()`](datepart.md) : Extract the day, month or year of a date
  in a dplyr pipeline
- [`inSchema()`](inSchema.md) : Helper for working with compound schemas

### DBI connection

Functions that accept DBI connections and are useful in cross database
DBI code

- [`dbms()`](dbms.md) : Get the database management system (dbms) from a
  cdm_reference or DBI connection
- [`listTables()`](listTables.md) : List tables in a schema

### Eunomia example CDM

Easily create and use example CDMs in a [duckdb](https://duckdb.org/)
database

- [`downloadEunomiaData()`](downloadEunomiaData.md) : Download Eunomia
  data files

- [`eunomiaDir()`](eunomiaDir.md) : Create a copy of an example OMOP CDM
  dataset

- [`eunomiaIsAvailable()`](eunomiaIsAvailable.md) : Has the Eunomia
  dataset been cached?

- [`exampleDatasets()`](exampleDatasets.md) : List the available example
  CDM datasets

- [`requireEunomia()`](requireEunomia.md) :

  Require eunomia to be available. The function makes sure that you can
  later create a eunomia database with
  [`eunomiaDir()`](../reference/eunomiaDir.md).

### Benchmarking

Run benchmarking of simple queries against your CDM reference

- [`benchmarkCDMConnector()`](benchmarkCDMConnector.md) : Run benchmark
  of tasks using CDMConnector
