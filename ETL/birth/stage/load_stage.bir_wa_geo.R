#### CODE TO COMBINE BIRTH GEO DATA FROM THE BEDROCK (2003-2016) AND WHALES (2017-) SYSTEMS
# Alastair Matheson, PHSKC (APDE)
#
# 2019-07


#### Free memory ####
rm(list=setdiff(ls(), "db_apde")) # only keep SQL connection
gc()

#### PULL IN TABLE CONFIG FILE FOR VAR TYPE INFO ####
table_config_stage_bir_wa_geo <- yaml::yaml.load(httr::GET(
  "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/stage/create_stage.bir_wa_geo.yaml"))


#### PULL IN BOTH DATA SETS ####
tbl_id_geo_2013_2016 <- DBI::Id(schema = "load_raw", table = "bir_wa_geo_2003_2016")
bir_geo_2013_2016 <- DBI::dbReadTable(db_apde, tbl_id_geo_2013_2016)

tbl_id_geo_2017_20xx <- DBI::Id(schema = "load_raw", table = "bir_wa_geo_2017_20xx")
bir_geo_2017_20xx <- DBI::dbReadTable(db_apde, tbl_id_geo_2017_20xx)


#### REMOVE FIELDS IN BEDROCK NOT COLLECTED AFTER 2003 ####
### Remove variables not collected after 2003 
bir_geo_2013_2016 <- bir_geo_2013_2016 %>%
  select(-tract90, -blgrp90, -xtract90, -xblgrp90,
         # Also drop lat and long since they are not included in this file
         -latitude, -longitude)


#### STANDARDIZE NAMES ####
### Bring in reference table
bir_field_map <- vroom::vroom("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/ref/ref.bir_field_name_map.csv")

data.table::setnames(bir_geo_2013_2016, 
                     bir_field_map$field_name_apde[match(names(bir_geo_2013_2016), 
                                                      bir_field_map$field_name_bedrock)])



#### BRING NEW AND OLD DATA TOGETHER ####
bir_geo_combined <- bind_rows(bir_geo_2017_20xx, bir_geo_2013_2016)

# Also trim white space
bir_geo_combined <- bir_geo_combined %>%
  mutate(res_geo_source = trimws(res_geo_source))

#### ALIGN BEDROCK VARIABLES WITH WHALES STRUCTURE ####
bir_geo_combined <- bir_geo_combined %>% 
  mutate(
    date_of_birth_year = as.numeric(str_sub(birth_cert_encrypt, 1, 4)),
    mother_residence_county_wa_code = case_when(
      date_of_birth_year < 2017 ~ str_sub(geocity, 1, 2),
      TRUE ~ mother_residence_county_wa_code),
    mother_residence_city_wa_code = case_when(
      date_of_birth_year < 2017 ~ geocity,
      TRUE ~ mother_residence_city_wa_code),
    #### CHECK WHEN ZCTA IS LIKELY TO HAVE SWITCHED OVER ####
    res_geo_zcta_2000 = case_when(date_of_birth_year <= 2010 ~ zcta,
                                  TRUE ~ res_geo_zcta_2000),
    res_geo_zcta_2010 = case_when(
      between(date_of_birth_year, 2011, 2016) ~ zcta,
      TRUE ~ res_geo_zcta_2010),
    res_geo_county_2010 = case_when(
      is.na(mother_residence_county_wa_code) ~ NA_character_,
      as.numeric(mother_residence_county_wa_code) > 0 ~ as.character(as.numeric(mother_residence_county_wa_code) * 2 - 1),
      TRUE ~ "00"),
    res_geo_census_full_2000 = case_when(
      !is.na(res_geo_census_block_2000) ~ paste0(
        "530", res_geo_county_2010, # Keep as 2010 because there is only one county var
        str_replace(res_geo_census_tract_2000, "\\.", ""),
        res_geo_census_block_grp_2000,
        res_geo_census_block_2000),
      TRUE ~ NA_character_),
    res_geo_census_full_2010 = case_when(
      !is.na(res_geo_census_block_2010) ~ paste0(
        "530", res_geo_county_2010,
        str_replace(res_geo_census_tract_2010, "\\.", ""),
        res_geo_census_block_grp_2010,
        res_geo_census_block_2010),
      TRUE ~ NA_character_),
    # Make a flag to indicate whether we should use this record for routine
    # analyses that are broken down by geography
    res_geo_use = case_when(
      is.na(res_geo_census_full_2000) & is.na(res_geo_census_full_2010) ~ 0,
      res_geo_match_score < 70 & res_geo_source != "CNTY EXCEP" ~ 0,
      res_geo_source == "CNTY EXCEP" | res_geo_match_score >= 70  ~ 1,
      TRUE ~ NA_real_
    )
  ) %>%
  select(birth_cert_encrypt, birth_cert_type, date_of_birth_year, 
         mother_residence_county_wa_code, mother_residence_city_wa_code,
         residence_zip_code, reported_zip, res_geo_source, res_geo_match_score,
         res_geo_school_district, res_geo_census_full_2000,
         res_geo_census_tract_2000, res_geo_census_block_grp_2000,
         res_geo_census_block_2000, res_geo_zcta_2000,
         res_geo_census_full_2010, res_geo_census_tract_2010,
         res_geo_census_block_grp_2010, res_geo_census_block_2010,
         res_geo_zcta_2010, res_geo_use, etl_batch_id)


#### REORDER ROWS ####
bir_geo_combined <- bir_geo_combined %>% arrange(birth_cert_encrypt)


#### LOAD TO SQL ####
# Need to manually truncate table so can use overwrite = F below (so column types work)
tbl_id_geo <- DBI::Id(schema = table_config_stage_bir_wa_geo$schema, 
                            table = table_config_stage_bir_wa_geo$table)
dbWriteTable(db_apde, tbl_id_geo, value = as.data.frame(bir_geo_combined),
             overwrite = T, append = F,
             field.types = unlist(table_config_stage_bir_wa_geo$vars))



#### TIDY UP ####
rm(table_config_stage_bir_wa_geo)
rm(tbl_id_geo_2013_2016)
rm(tbl_id_geo_2017_20xx)
rm(bir_geo_2013_2016)
rm(bir_geo_2017_20xx)
rm(tbl_id_geo)

