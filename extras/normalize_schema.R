# Internal function to normalize representation of database schema
# A schema can have 3 possible components: catalog, schema, prefix
# It can be passed in using a few different representations.

# A named vector, an unnamed vector, a single string with '.' separating the components, or a DBI::Id
# This function accepts any one of these and returns a DBI::Id
normalize_schema <- function(schema) {
  # length 1, 2, or 3 vector (named or unnamed)
  # DBI::Id

  if (is.null(schema)) {
    return(list(schema = NULL, prefix = NULL))
  }

  if (methods::is(schema, "Id")) {
    # convert Id to named vector
    schema <- attr(schema, "name")
  }

  checkmate::assert_character(schema, min.len = 1, max.len = 3)

  # handle schema names like 'schema.dbo' by splitting on '.'
  if (!is.null(schema) && length(schema) == 1) {
    schema <- strsplit(schema, "\\.")[[1]]
    checkmate::assert_character(schema, min.len = 1, max.len = 2)
  }

  # check that all elements are named or none are named
  if (length(names(schema)) > 0) {
    checkmate::check_names(schema, type = "strict")
    checkmate::check_subset(names(schema), c("catalog", "schema", "prefix"))
  } else {
    # add names if they don't already exist
    switch (length(schema),
            names(schema) <- "schema",
            names(schema) <- c("catalog", "schema"),
            names(schema) <- c("catalog", "schema", "prefix")
    )
  }

  # extract out each part
  prefix <- if(is.na(schema["prefix"])) NULL else schema["prefix"] %>% unname
  checkmate::assert_character(prefix, len = 1, min.chars = 0, pattern = "[_a-z]+", null.ok = TRUE)
  catalog <- if(is.na(schema["catalog"])) NULL else schema["catalog"] %>% unname
  schema <- schema["schema"] %>% unname

  return(list(schema = c(catalog, schema), prefix = prefix))
}
