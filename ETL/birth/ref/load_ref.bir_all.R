#### CODE TO LOAD BIRTH DATA REFERENCE TABLES
#
# Alastair Matheson, PHSKC (APDE)
#
# 2019-06

library(vroom) # Read in data appropriately
library(tidyverse) # Manipulate data
library(odbc) # Read to and write from SQL
library(stringi) # Fix up character formats

db_apde <- dbConnect(odbc(), "PH_APDEStore50")

#### SET UP FUNCTIONS ####
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/scripts_general/create_table.R")


#### BIRTH FIELD NAMES ####
# Bring in data from Github repo
bir_field_names_map <- vroom("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/ref/ref.bir_field_name_map.csv")

# Fix encoding issues
bir_field_names_map <- bir_field_names_map %>%
  mutate(description = stri_encode(description, from = "WINDOWS-1252", to = "UTF8"))

# Load table to SQL
tbl_id_ref_bir_field_name_map <- DBI::Id(schema = "ref", table = "bir_field_name_map")
dbWriteTable(db_apde, tbl_id_ref_bir_field_name_map, bir_field_names_map, overwrite = T,
             field.types = c(description = "VARCHAR(1000)"))

# Remove data
rm(tbl_id_ref_bir_field_name_map, bir_field_names_map)

