# Assert that cdm has a writable schema

A cdm object can optionally contain a single schema in a database with
write access. assert_write_schema checks that the cdm contains the
"write_schema" attribute and tests that local dataframes can be written
to tables in this schema.

## Usage

``` r
assert_write_schema(cdm, add = NULL)

assertWriteSchema(cdm, add = NULL)
```

## Arguments

- cdm:

  A cdm object

- add:

  An optional AssertCollection created by
  [`checkmate::makeAssertCollection()`](https://mllg.github.io/checkmate/reference/AssertCollection.html)
  that errors should be added to.

## Value

Invisibly returns the cdm object

## Details

**\[deprecated\]**
