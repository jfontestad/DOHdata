#### MASTER CODE TO RUN A FULL BIRTH DATA REFRESH
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

#### SET UP GLOBAL PARAMETERS AND CALL IN LIBRARIES ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170)

library(vroom) # Read in data appropriately
library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(data.table) # Manipulate data

bir_path <- "//phdata01/DROF_DATA/DOH DATA/Births"
db_apde <- dbConnect(odbc(), "PH_APDEStore50")


#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/general/scripts_general/etl_log.R")


#### SET UP USEFUL REF TABLES ####
bir_field_names_map <- dbGetQuery(db_apde, "SELECT * FROM ref.bir_field_name_map")


#### LOAD_RAW: 2003-2016 ####
### Pull in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_2003_2016.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/qa_load_raw.bir_wa_2003_2016.R")

### Pull in config files to define variable types and metadata
table_config_create_bir_wa_2003_2016 <- yaml::yaml.load(getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/create_load_raw.bir_wa_2003_2016.yaml"))
table_config_load_bir_wa_2003_2016 <- yaml::yaml.load(getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_2003_2016.yaml"))


### Create table
create_table_f(conn = db_apde,
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/create_load_raw.bir_wa_2003_2016.yaml",
               overall = T, ind_yr = F, overwrite = T)


### Run function to import and load data
load_bir_wa_2003_2016_output <- load_load_raw.bir_wa_2003_2016_f(
  table_config_create = table_config_create_bir_wa_2003_2016,
  table_config_load = table_config_load_bir_wa_2003_2016,
  bir_path_inner = bir_path,
  conn = db_apde)


### Run function to QA loaded data
qa_load_raw_bir_wa_2003_2016_f(conn = db_apde, load_only = T)



#### LOAD_RAW: 2017-20xx ####
### Pull in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_2017_20xx.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/qa_load_raw.bir_wa_2017_20xx.R")


### Pull in config files to define variable types and metadata
table_config_create_bir_wa_2017_20xx <- yaml::yaml.load(getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/create_load_raw.bir_wa_2017_20xx.yaml"))
table_config_load_bir_wa_2017_20xx <- yaml::yaml.load(getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_2017_20xx.yaml"))


### Create table
create_table_f(conn = db_apde,
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/create_load_raw.bir_wa_2017_20xx.yaml",
               overall = T, ind_yr = F, overwrite = T)


### Run function to import and load data
load_bir_wa_2017_20xx_output <- load_load_raw.bir_wa_2017_20xx_f(
  table_config_create = table_config_create_bir_wa_2017_20xx,
  table_config_load = table_config_load_bir_wa_2017_20xx,
  bir_path_inner = bir_path,
  conn = db_apde)


### Run function to QA loaded data
qa_load_raw_bir_wa_2017_20xx_f(conn = db_apde, load_only = T)



#### STAGE ####
### Pull in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/stage/load_stage.bir_wa.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/stage/qa_stage.bir_wa.R")


### Create table
create_table_f(conn = db_apde,
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/stage/create_stage.bir_wa.yaml",
               overall = T, ind_yr = F, overwrite = T)

### Load table
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/stage/load_stage.bir_wa.R")



