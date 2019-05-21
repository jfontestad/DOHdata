#### MASTER CODE TO RUN A FULL BIRTH DATA REFRESH
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-05

#### SET UP GLOBAL PARAMETERS AND CALL IN LIBRARIES ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170)

library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code

bir_path <- "//phdata01/DROF_DATA/DOH DATA/Births"
db_apde <- dbConnect(odbc(), "PH_APDEStore50")


#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")


#### BATCH ID ####


#### LOAD_RAW ####
### Create tables
create_table_f(conn = db_apde,
               config_url = "",
               overall = T, ind_yr = T, overwrite = T)

### 

