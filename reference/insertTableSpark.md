# Fast bulk insert of a local table on Spark / Databricks

Uploads a local data frame to Spark/Databricks using multi-row
`INSERT INTO ... VALUES (...), (...), ...` statements. This is the same
mechanism
[`insertTable()`](https://darwin-eu.github.io/omopgenerics/reference/insertTable.html)
now uses by default on Spark, so you only need `insertTableSpark()`
directly when you want to tune `batchSize`. Multi-row VALUES inserts are
dramatically faster than the `INSERT ... SELECT ... UNION ALL` approach
Spark's planner struggles with (benchmarked ~50x faster at 1000 rows).

## Usage

``` r
insertTableSpark(cdm, name, table, overwrite = TRUE, batchSize = 5000L)
```

## Arguments

- cdm:

  A `cdm_reference` or `db_cdm` source object backed by a
  Spark/Databricks connection. Must have a `writeSchema`.

- name:

  Name of the destination table (single character).

- table:

  A local data frame to upload.

- overwrite:

  If `TRUE` (default), drop the table first if it exists.

- batchSize:

  Number of rows per `INSERT` statement. Default 5000. Larger batches
  reduce round trips but Spark imposes a query-size limit (~16MB) —
  reduce if you hit "query too large" errors with very wide tables.

## Value

A `cdm_table` referencing the newly inserted table.

## Details

Only intended for Spark connections. For other dialects use
[`insertTable()`](https://darwin-eu.github.io/omopgenerics/reference/insertTable.html).
