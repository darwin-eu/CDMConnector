# Build numeric range clause

Build numeric range clause

## Usage

``` r
build_numeric_range_clause(sql_expression, numeric_range, format = NULL)
```

## Arguments

- sql_expression:

  SQL column expression

- numeric_range:

  List with op, value, extent

- format:

  Optional format for floats

## Value

SQL WHERE clause or NULL
