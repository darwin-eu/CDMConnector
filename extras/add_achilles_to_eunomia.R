

CDMConnector::example_datasets()

CDMConnector::eunomia_dir("synthea-covid19-10k", database_file = "~/Desktop/synthea-covid19-10k-achilles.duckdb")

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "duckdb",
  server = "~/Desktop/synthea-covid19-10k-achilles.duckdb"
)

debugonce(Achilles::achilles)

Achilles::achilles(
  connectionDetails,
  cdmDatabaseSchema = "main",
  resultsDatabaseSchema = cdmDatabaseSchema,
  scratchDatabaseSchema = resultsDatabaseSchema,
  vocabDatabaseSchema = cdmDatabaseSchema,
  tempEmulationSchema = resultsDatabaseSchema,
  sourceName = "Synthea Covid 19",
  # analysisIds,
  createTable = TRUE,
  smallCellCount = 0,
  cdmVersion = "5.3",
  createIndices = TRUE,
  numThreads = 1,
  tempAchillesPrefix = "tmpach",
  dropScratchTables = TRUE,
  sqlOnly = FALSE,
  outputFolder = "~/Desktop/output",
  verboseMode = TRUE,
  optimizeAtlasCache = FALSE,
  defaultAnalysesOnly = TRUE,
  updateGivenAnalysesOnly = FALSE,
  # excludeAnalysisIds,
  sqlDialect = "duckdb"
)


s <- c('-- DDL FOR THE ACHILLES_ANALYSIS TABLE
DROP TABLE IF EXISTS main.achilles_analysis;
CREATE TABLE main.ACHILLES_ANALYSIS (
	analysis_id     INTEGER,
	analysis_name   VARCHAR(255),
	stratum_1_name  VARCHAR(255),
	stratum_2_name  VARCHAR(255),
	stratum_3_name  VARCHAR(255),
	stratum_4_name  VARCHAR(255),
	stratum_5_name  VARCHAR(255),
	is_default      INTEGER,
	category        VARCHAR(255)
);')

DBI::dbExecute(con, s)


con <- DBI::dbConnect(duckdb::duckdb(CDMConnector::eunomia_dir("synthea-snf-10k")))
Achilles::getAnalysisDetails()
