#### CREATE ETL LOGGING TABLE ####

#### Set up global parameter and call in libraries ####
library(odbc) # Read to and write from SQL
library(configr) # Read in YAML files
library(glue) # Piece together queries

db_apde <- dbConnect(odbc(), "PH_APDEStore50")

### Set up functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")

### Run function
create_table_f(conn = db_apde, 
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/general/metadata/tables/create_metadata.etl_log.yaml",
               overall = T, ind_yr = F)
