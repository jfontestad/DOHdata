#### MASTER CODE TO RUN A FULL BIRTH DATA REFRESH
#
# Alastair Matheson, PHSKC (APDE)
#
# Created: 2019-05
#
# Updated: 2019-11-27 by Danny Colombara (PHSKC/APDE)

#### SET UP GLOBAL PARAMETERS AND CALL IN LIBRARIES ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)

rm(list=ls())

library(vroom) # Read in data appropriately
library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(data.table) # Manipulate data
library(rads)

bir_path <- "//phdata01/DROF_DATA/DOH DATA/Births"
bir_path_geo <- "//phdata01/EPE_DATA/GEOCODING (restricted)/birth"
db_apde <- dbConnect(odbc(), "PH_APDEStore50")


#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/general/scripts_general/etl_log.R")


#############################
#### LOAD_RAW: 2003-2016 ####
#############################

### Pull in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_2003_2016.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/qa_load_raw.bir_wa_2003_2016.R")

### Pull in config files to define variable types and metadata
table_config_create_bir_wa_2003_2016 <- yaml::yaml.load(getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/create_load_raw.bir_wa_2003_2016.yaml"))
table_config_load_bir_wa_2003_2016 <- yaml::yaml.load(getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_2003_2016.yaml"))


### Create table
### Now using overwrite = T in dbWriteTable so this should not be necessary
# create_table_f(conn = db_apde,
#                config_url = "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/create_load_raw.bir_wa_2003_2016.yaml",
#                overall = T, ind_yr = F, overwrite = T)


### Run function to import and load data
load_bir_wa_2003_2016_output <- load_load_raw.bir_wa_2003_2016_f(
  table_config_create = table_config_create_bir_wa_2003_2016,
  table_config_load = table_config_load_bir_wa_2003_2016,
  bir_path_inner = bir_path,
  conn = db_apde)


### Run function to QA loaded data
qa_load_raw_bir_wa_2003_2016_f(conn = db_apde, load_only = T)


#################################
#### LOAD_RAW: 2003-2016 GEO ####
#################################

### Pull in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_geo_2003_2016.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/qa_load_raw.bir_wa_geo_2003_2016.R")

### Pull in config files to define variable types and metadata
table_config_create_bir_wa_geo_2003_2016 <- yaml::yaml.load(getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/create_load_raw.bir_wa_geo_2003_2016.yaml"))
table_config_load_bir_wa_geo_2003_2016 <- yaml::yaml.load(getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_geo_2003_2016.yaml"))

### Create table
create_table_f(conn = db_apde,
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/create_load_raw.bir_wa_geo_2003_2016.yaml",
               overall = T, ind_yr = F, overwrite = T)

### Run function to import and load data
# Note: This is currently generating an error about column names not matching the parser.
# This seems to be associated with the vroom data import but the reason is not fully clear.
# The data are still loaded correctly to SQL though.
load_bir_wa_geo_2003_2016_output <- load_load_raw.bir_wa_geo_2003_2016_f(
  table_config_create = table_config_create_bir_wa_geo_2003_2016,
  table_config_load = table_config_load_bir_wa_geo_2003_2016,
  bir_path_inner = bir_path_geo,
  conn = db_apde)

### Run function to QA loaded data
qa_load_raw_bir_wa_geo_2003_2016_f(conn = db_apde, load_only = T)


#############################
#### LOAD_RAW: 2017-20xx ####
#############################

### Pull in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_2017_20xx.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/qa_load_raw.bir_wa_2017_20xx.R")


### Pull in config files to define variable types and metadata
table_config_load_bir_wa_2017_20xx <- yaml::yaml.load(getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_2017_20xx.yaml"))

### Run function to import and load data
load_bir_wa_2017_20xx_output <- load_load_raw.bir_wa_2017_20xx_f(
  table_config_load = table_config_load_bir_wa_2017_20xx,
  bir_path_inner = bir_path,
  conn = db_apde)


### Run function to QA loaded data
qa_load_raw_bir_wa_2017_20xx_f(conn = db_apde, load_only = T)

### Clean up
rm(table_config_load_bir_wa_2017_20xx, load_load_raw.bir_wa_2017_20xx_f,
   qa_load_raw_bir_wa_2017_20xx_f)


#################################
#### LOAD_RAW: 2017-20xx GEO ####
#################################
### Pull in functions
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_geo_2017_20xx.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/qa_load_raw.bir_wa_geo_2017_20xx.R")

### Pull in config files to define variable types and metadata
table_config_load_bir_wa_geo_2017_20xx <- yaml::yaml.load(getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/load_load_raw.bir_wa_geo_2017_20xx.yaml"))


### Run function to import and load data
load_bir_wa_geo_2017_20xx_output <- load_load_raw.bir_wa_geo_2017_20xx_f(
  table_config_load = table_config_load_bir_wa_geo_2017_20xx,
  bir_path_inner = bir_path_geo,
  conn = db_apde)

### Run function to QA loaded data
qa_load_raw_bir_wa_geo_2017_20xx_f(conn = db_apde, load_only = T)

### Clean up
rm(table_config_load_bir_wa_geo_2017_20xx, load_load_raw.bir_wa_geo_2017_20xx_f,
   qa_load_raw_bir_wa_geo_2017_20xx_f)


###############
#### STAGE ####
###############
#### BIR_WA_GEO ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/stage/load_stage.bir_wa_geo.R")

#### BIR_WA ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/stage/load_stage.bir_wa.R")

### QA for birth data overall (consider running manually so can assess quality before loading to SQL ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/stage/qa_stage.bir_wa.R")


