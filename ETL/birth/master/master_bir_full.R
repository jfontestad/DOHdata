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

bir_path <- "//phdata01/DROF_DATA/DOH DATA/Births"
db_apde <- dbConnect(odbc(), "PH_APDEStore50")


#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/general/scripts_general/etl_log.R")


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
  db_conn = db_apde)


### Run QA to check loaded data
qa_load_raw_bir_wa_2003_2016_f(conn = db_apde, load_only = T)



#### LOAD_RAW: 2017-20xx ####












create_table_f(conn = db_apde,
               config_url = "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/load_raw/create_load_raw.bir_wa_2017_20xx.yaml",
               overall = T, ind_yr = F, overwrite = T)


#### Load 2017+ data into R ####
bir_file_names_2017_20xx <- list.files(path = file.path(bir_path, "DATA/raw"), 
                                       pattern = "birth_20(1[7-9]{1}|2[0-9]{1}).(asc|csv|xls|xlsx)$",
                                       full.names = T)
bir_names_2017_20xx <- lapply(bir_file_names_2017_20xx, function(x)
  str_sub(x,
          start = str_locate(x, "birth_20")[1], 
          end = str_locate(x, "birth_20[0-9]{2}")[2])
)

# Bring in data
bir_files_2017_20xx <- lapply(bir_file_names_2017_20xx, function(x) {
  vroom::vroom(file = x, col_select = names(col_type_list_2003_2016),
               col_types = col_type_list_2003_2016,
               n_max = 100)
})


temp <- openxlsx::read.xlsx("K:/Births/DATA/BIRTH/BirthStatF2017_mod2018-1126.xlsx")
temp2 <- vroom::vroom(file = "K:/Births/DATA/raw/birth_2017.csv",
                      .name_repair = ~ janitor::make_clean_names(., case = "snake"))
temp3 <- read.csv(file = "K:/Births/DATA/raw/birth_2017.csv")



#### Load to SQL ####
tbl_id_meta <- DBI::Id(schema = "load_raw", table = "bir_wa_2003_2016")
dbWriteTable(db_apde, bir_2003_2016)

