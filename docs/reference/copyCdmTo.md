# Copy a cdm object from one database to another

It may be helpful to be able to easily copy a small test cdm from a
local database to a remote for testing. copyCdmTo takes a cdm object and
a connection. It copies the cdm to the remote database connection. CDM
tables can be prefixed in the new database allowing for multiple cdms in
a single shared database schema.

## Usage

``` r
copyCdmTo(con, cdm, schema, overwrite = FALSE)
```

## Arguments

- con:

  A DBI database connection created by
  [`DBI::dbConnect`](https://dbi.r-dbi.org/reference/dbConnect.html)

- cdm:

  A cdm reference object created by
  [`CDMConnector::cdmFromCon`](cdmFromCon.md) or
  `CDMConnector::cdm_from_con`

- schema:

  schema name in the remote database where the user has write permission

- overwrite:

  Should the cohort table be overwritten if it already exists? TRUE or
  FALSE (default)

## Value

A cdm reference object pointing to the newly created cdm in the remote
database
