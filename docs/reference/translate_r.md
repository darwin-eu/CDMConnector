# Translate SQL to target dialect (Pure R implementation — kept as fallback)

Translate SQL to target dialect (Pure R implementation — kept as
fallback)

## Usage

``` r
translate_r(
  sql,
  targetDialect,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  oracleTempSchema = NULL
)
```

## Arguments

- sql:

  SQL string.

- targetDialect:

  Target dialect.

- tempEmulationSchema:

  Temp schema for emulation.

- oracleTempSchema:

  Oracle temp schema.
