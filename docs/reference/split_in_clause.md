# Split IN clause for large value lists (Oracle 1000 limit)

Split IN clause for large value lists (Oracle 1000 limit)

## Usage

``` r
split_in_clause(column_name, values, max_length = 1000)
```

## Arguments

- column_name:

  SQL column name

- values:

  Integer vector

- max_length:

  Max values per chunk

## Value

SQL expression string
