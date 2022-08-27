# CDMConnector

The CDMConnector package provides tools for working OMOP Common Data
Model (CDM) tables in a pipe friendly syntax. CDM table references are
stored in a single compound object along with CDM specific metadata.

The main function provided by the package is `cdm_from_con` which
creates a CDM connection object that can be used with dplyr verbs. In
the examples that we will use a duckdb database which is embedded in the
CDMConnector package.

    library(CDMConnector)

    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
    cdm <- cdm_from_con(con)
    cdm
    #> # OMOP CDM reference
    #> 
    #> Tables: person, observation_period, visit_occurrence, visit_detail, condition_occurrence, drug_exposure, procedure_occurrence, device_exposure, measurement, observation, death, note, note_nlp, specimen, fact_relationship, location, care_site, provider, payer_plan_period, cost, drug_era, dose_era, condition_era, concept, vocabulary, concept_relationship, concept_ancestor, drug_strength

Individual CDM table references can be accessed using `$` and piped to
dplyr verbs.

    library(dplyr, warn.conflicts = FALSE)
    cdm$person %>% 
      count()
    #> # Source:   SQL [1 x 1]
    #> # Database: DuckDB 0.3.5-dev1410 [root@Darwin 21.6.0:R 4.2.0//var/folders/xx/01v98b6546ldnm1rg1_bvk000000gn/T//RtmpQAyaPL/cdm.duckdb]
    #>       n
    #>   <dbl>
    #> 1  2694

    cdm$drug_exposure %>% 
      left_join(cdm$concept, by = c("drug_concept_id" = "concept_id")) %>% 
      count(drug = concept_name, sort = TRUE)
    #> # Source:     SQL [?? x 2]
    #> # Database:   DuckDB 0.3.5-dev1410 [root@Darwin 21.6.0:R 4.2.0//var/folders/xx/01v98b6546ldnm1rg1_bvk000000gn/T//RtmpQAyaPL/cdm.duckdb]
    #> # Ordered by: desc(n)
    #>    drug                                                                        n
    #>    <chr>                                                                   <dbl>
    #>  1 Acetaminophen 325 MG Oral Tablet                                         9365
    #>  2 poliovirus vaccine, inactivated                                          7977
    #>  3 tetanus and diphtheria toxoids, adsorbed, preservative free, for adult…  7430
    #>  4 Aspirin 81 MG Oral Tablet                                                4380
    #>  5 Amoxicillin 250 MG / Clavulanate 125 MG Oral Tablet                      3851
    #>  6 hepatitis A vaccine, adult dosage                                        3211
    #>  7 Acetaminophen 160 MG Oral Tablet                                         2158
    #>  8 zoster vaccine, live                                                     2125
    #>  9 Acetaminophen 21.7 MG/ML / Dextromethorphan Hydrobromide 1 MG/ML / dox…  1993
    #> 10 hepatitis B vaccine, adult dosage                                        1916
    #> # … with more rows
    #> # ℹ Use `print(n = ...)` to see more rows

To send SQL to the CDM use the connection object. The cdm reference also
contains the connection as an attribute.

    DBI::dbGetQuery(con, "select count(*) as person_count from main.person")
    #>   person_count
    #> 1         2694
    DBI::dbGetQuery(attr(cdm, "con"), "select count(*) as person_count from main.person")
    #>   person_count
    #> 1         2694

# Subsetting the CDM

If you do not need references to all tables you can easily select only a
subset of tables to include in the cdm reference. The `select` argument
of `cdm_from_con` supports the [tidyselect selection
language](https://tidyselect.r-lib.org/reference/language.html) and
provides a new selection helper: `tbl_group`.

    cdm_from_con(con, select = starts_with("concept"))
    #> # OMOP CDM reference
    #> 
    #> Tables: concept, concept_class, concept_relationship, concept_synonym, concept_ancestor
    cdm_from_con(con, select = contains("era"))
    #> # OMOP CDM reference
    #> 
    #> Tables: drug_era, dose_era, condition_era
    cdm_from_con(con, select = tbl_group("vocab"))
    #> # OMOP CDM reference
    #> 
    #> Tables: concept, vocabulary, domain, concept_class, concept_relationship, relationship, concept_synonym, concept_ancestor, source_to_concept_map, drug_strength

`tbl_group` supports several subsets of the CDM: “all”, “clinical”,
“vocab”, “derived”, and “default”.

The default set of CDM tables included in a CDM object is:

    tbl_group("default")
    #>  [1] "person"               "observation_period"   "visit_occurrence"    
    #>  [4] "visit_detail"         "condition_occurrence" "drug_exposure"       
    #>  [7] "procedure_occurrence" "device_exposure"      "measurement"         
    #> [10] "observation"          "death"                "note"                
    #> [13] "note_nlp"             "specimen"             "fact_relationship"   
    #> [16] "location"             "care_site"            "provider"            
    #> [19] "payer_plan_period"    "cost"                 "drug_era"            
    #> [22] "dose_era"             "condition_era"        "concept"             
    #> [25] "vocabulary"           "concept_relationship" "concept_ancestor"    
    #> [28] "drug_strength"

# Closing connections

Close the database connection with `dbDisconnect`. After the connection
is closed the cdm object can no longer be used.

    DBI::dbDisconnect(con)
    cdm$person
    #> Error: bad_weak_ptr

# Delaying connections

Sometimes you may need to delay the creation of a cdm connection or
create and close many connections during execution. CDMConnector
provides the ability to store connection information that can be passed
to `dbConnect` to create a connection. The typical use case is to create
a new connection inside a function and then close the connection before
the function exits.

    connection_details <- dbConnectDetails(duckdb::duckdb(), dbdir = eunomia_dir())

    self_contained_query <- function(connection_details) {
      # create a new connection
      con <- DBI::dbConnect(connection_details)
      # close the connection before exiting 
      on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
      # use the connection
      DBI::dbGetQuery(con, "select count(*) as n from main.person")
    }

    self_contained_query(connection_details)
    #>      n
    #> 1 2694
