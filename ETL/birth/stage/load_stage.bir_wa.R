#### CODE TO COMBINE BIRTH DATA FROM THE BEDROCK (2003-2016) AND WHALES (2017-) SYSTEMS
# Alastair Matheson, PHSKC (APDE)
#
# 2019-06


# Recode groups of variables where <group_name>1-<group_nameX> has been replaced
#     by specific flags rather than codes
# Standardize missing to be NULL
# Standardize country/county/state/city codes (not needed as there is literal and code now)

#### Set-up environment ----
rm(list=ls())

library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(tidyverse) # Manipulate data
library(data.table) # Manipulate data quickly / efficiently

db_apde <- dbConnect(odbc(), "APDESQL50") ##Connect to SQL server

#### PULL IN TABLE CONFIG FILE FOR VAR TYPE INFO ####
table_config_stage_bir_wa <- yaml::yaml.load(getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/stage/create_stage.bir_wa.yaml"))


#### PULL IN BOTH DATA SETS ####
tbl_id_2013_2016 <- DBI::Id(schema = "load_raw", table = "bir_wa_2003_2016")
bir_2013_2016 <- DBI::dbReadTable(db_apde, tbl_id_2013_2016)

tbl_id_2017_20xx <- DBI::Id(schema = "load_raw", table = "bir_wa_2017_20xx")
bir_2017_20xx <- DBI::dbReadTable(db_apde, tbl_id_2017_20xx)


#### REMOVE FIELDS IN BEDROCK NOT COLLECTED AFTER 2003 ####
### NB. Need to do this BEFORE renaming variables because otherwise two 
###   WIC variables are created
### Remove variables not collected after 2003
bir_2013_2016 <- bir_2013_2016 %>%
  select(-priorprg, -fd_lt20, -fd_ge20, -apgar1, -ind_num, -malf_sam, -herpes, 
         -smokenum, -drinknum, -contains("amnio"), -dmeth2, -dmeth3, -dmeth4, 
         -contains("complab"), -racecdes, -hispcdes, -carepay, -wic, -firsteps, 
         -afdc, -localhd, -contains("ubleed"), -smoking, -drinking, -lb_f_nl, 
         -lb_f84, -lb_p_nl, -lb_p84, -lb_p_nd, -lb_f_nd, -fd_2036, -fd_ge37, 
         -contains("malficd"), -contains("mrfnon"), -ind_mo, -ind_yr, -lfd_mo, 
         -lfd_yr, -loth_mo, -loth_yr, -lpp_mo, -lpp_yr, -trib_res) %>%
  # Also remove geozip, which is in the geocoded birth file
  select(-geozip)


#### STANDARDIZE NAMES FROM BEDROCK TO WHALES ####
### Bring in reference table
field_maps <- vroom::vroom("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/ref/ref.bir_field_name_map.csv")

data.table::setnames(bir_2013_2016, 
                     field_maps$field_name_apde[match(names(bir_2013_2016), 
                                                      field_maps$field_name_bedrock)])


#### SPLIT OUT VARIABLES THAT ARE NOW INDIVIDUAL FLAGS ####
#### Medical risk factors ####
# Can rely on the fact that the code for diabetes (01) only ever went in mrf1
# and code for hypertension (02) only ever went in mrf1 and mrf2
# Also rely on the fact that if mrf1 = "09" or "99" then all other mrf fields are NA
bir_2013_2016 <- bir_2013_2016 %>% 
  mutate(
    prepreg_diabetes = case_when(
      mrf1 == "01" & diabetes == "E" ~ "Y",
      mrf1 == "01" & diabetes == "U" ~ "U",
      mrf1 == "01" & diabetes == "G" ~ "N",
      mrf1 != "01" & mrf1 != "99" ~ "N",
      TRUE ~ "U"
    ),
    gestational_diabetes = case_when(
      mrf1 == "01" & diabetes == "G" ~ "Y",
      mrf1 == "01" & diabetes == "U" ~ "U",
      mrf1 == "01" & diabetes == "E" ~ "N",
      mrf1 != "01" & mrf1 != "99" ~ "N",
      TRUE ~ "U"
    ),
    prepreg_hypertension = case_when(
      (mrf1 == "02" | mrf2 == "02") & hyperflg == "E" ~ "Y",
      (mrf1 == "02" | mrf2 == "02") & hyperflg == "U" ~ "U",
      (mrf1 == "02" | mrf2 == "02") & hyperflg == "G" ~ "N",
      mrf1 != "02" & mrf1 != "99" & mrf2 != "02" ~ "N",
      TRUE ~ "U"
    ),
    gestational_hypertention = case_when(
      (mrf1 == "02" | mrf2 == "02") & hyperflg == "G" ~ "Y",
      (mrf1 == "02" | mrf2 == "02") & hyperflg == "U" ~ "U",
      (mrf1 == "02" | mrf2 == "02") & hyperflg == "E" ~ "N",
      mrf1 != "02" & mrf1 != "99" & mrf2 != "02" ~ "N",
      TRUE ~ "U"
    ),
    risk_factors_none = case_when(
      mrf1 == "09" ~ "Y",
      mrf1 != "09" & mrf1 != "99" ~ "N",
      TRUE ~ "U"
    ),
    risk_factors_unknown = case_when(
      mrf1 == "99" ~ "Y",
      mrf1 != "99" ~ "N",
      TRUE ~ "U"
    )
  )

# Other medical risk factors are more straight forward
# First set up lists of new vars and their equivalent code
mrf_name <- list("hypertension_eclampsia", "preterm_births", "poor_preg_outcomes",
                 "vaginal_bleeding", "fertility_treatment", "assisted_reproduction",
                 "previous_cesarean", "group_b_strep")
mrf_num <- list("02", "03", "04", "05", "06", "06", "07", "08")

# Write a function to recode new var
mrf_f <- function(df, name, num) {
  df <- df %>%
    mutate(!!name := case_when(
      (mrf1 == num | mrf2 == num | mrf3 == num | mrf4 == num | mrf5 == num | mrf6 == num) ~ "Y",
      mrf1 == "09" ~ "N",
      mrf1 != num & mrf1 != "99" & 
        (mrf2 != num | is.na(mrf2)) & (mrf3 != num | is.na(mrf3)) &
        (mrf4 != num | is.na(mrf4)) & (mrf5 != num | is.na(mrf5)) & 
        (mrf6 != num | is.na(mrf6)) ~ "N",
      TRUE ~ "U"
    )) %>%
    # Just keep the new column and the cert number to join on
    select(birth_cert_encrypt, !!name)
}

# Run function and collapse list of results into a single df
mrf_out <- purrr::map2(mrf_name, mrf_num, 
                       ~ mrf_f(df = bir_2013_2016, name = .x, num = .y)) %>%
  purrr::reduce(left_join, by = "birth_cert_encrypt")

# Bind new columns to existing data frame and remove old vars
bir_2013_2016 <- left_join(bir_2013_2016, mrf_out, by = "birth_cert_encrypt") %>%
  select(-contains("mrf"), -diabetes, -hyperflg)

# Clean up objects
rm(mrf_name, mrf_num, mrf_f, mrf_out)


#### Maternal infections ####
# First set up lists of new vars and their equivalent code
minfect_name <- list("gonorrhea", "syphilis", "herpes", "chlamydia", 
                     "hep_b", "hep_c", "hiv", "infections_other")
minfect_num <- list("01", "02", "03", "04", "05", "06", "07", "08")

# Write a function to recode new var
minfect_f <- function(df, name, num) {
  df <- df %>%
    mutate(!!name := case_when(
      (minfect1 == num | minfect2 == num | minfect3 == num | minfect4 == num | 
         minfect5 == num | minfect6 == num | minfect7 == num) ~ "Y",
      minfect1 == "09" | minfect1 == "9 " ~ "N",
      minfect1 != num & minfect1 != "99" & 
        (minfect2 != num | is.na(minfect2)) & (minfect3 != num | is.na(minfect3)) &
        (minfect4 != num | is.na(minfect4)) & (minfect5 != num | is.na(minfect5)) & 
        (minfect6 != num | is.na(minfect6)) & (minfect7 != num | is.na(minfect7)) ~ "N",
      TRUE ~ "U"
    )) %>%
    # Just keep the new column and the cert number to join on
    select(birth_cert_encrypt, !!name)
}

# Run function and collapse list of results into a single df
minfect_out <- purrr::map2(minfect_name, minfect_num, 
                           ~ minfect_f(df = bir_2013_2016, name = .x, num = .y)) %>%
  purrr::reduce(left_join, by = "birth_cert_encrypt")

# Bind new columns to existing data frame
bir_2013_2016 <- left_join(bir_2013_2016, minfect_out, by = "birth_cert_encrypt")

# Make infections_none and infections_unknown and remove old vars
bir_2013_2016 <- bir_2013_2016 %>%
  mutate(
    infections_none = case_when(
      minfect1 %in% c("09", "9 ") ~ "Y",
      minfect1 != "09" & minfect1 != "99" ~ "N",
      TRUE ~ "U"
    ),
    infections_unknown = case_when(
      minfect1 == "99" ~ "Y",
      minfect1 != "99" ~ "N",
      TRUE ~ "U"
    )
  ) %>%
  select(-contains("minfect"))

# Clean up objects
rm(minfect_name, minfect_num, minfect_f, minfect_out)



#### Obstetric procedures ####
bir_2013_2016 <- bir_2013_2016 %>% 
  mutate(
    cervical_cerclage = case_when(
      obproc1 == "1" ~ "Y",
      !(obproc1 %in% c("8", "9")) ~ "N",
      TRUE ~ "U"
    ),
    tocolysis = case_when(
      obproc1 == "2" | obproc2 == "2" ~ "Y",
      !obproc1 %in% c("8", "9") ~ "N",
      TRUE ~ "U"
    ),
    ecv_success = case_when(
      (obproc1 == "3" | obproc2 == "3" | obproc2 == "3" | obproc2 == "3") & 
        cephaflg == "S" ~ "Y",
      (obproc1 == "3" | obproc2 == "3" | obproc2 == "3" | obproc2 == "3") & 
        cephaflg == "F" ~ "N",
      (obproc1 != "3" & !obproc2 %in% c("8", "9")) & 
        (obproc2 != "3" | is.na(obproc2)) & 
        (obproc3 != "3" | is.na(obproc3)) ~ "N",
      TRUE ~ "U"
    ),
    ecv_failed = case_when(
      (obproc1 == "3" | obproc2 == "3" | obproc2 == "3" | obproc2 == "3") & 
        cephaflg == "F" ~ "Y",
      (obproc1 == "3" | obproc2 == "3" | obproc2 == "3" | obproc2 == "3") & 
        cephaflg == "S" ~ "N",
      (obproc1 != "3" & !obproc2 %in% c("8", "9")) & 
        (obproc2 != "3" | is.na(obproc2)) & 
        (obproc3 != "3" | is.na(obproc3)) ~ "N",
      TRUE ~ "U"
    ),
    obstet_proc_none = case_when(
      obproc1 == "4" ~ "Y",
      (obproc1 != "4" & obproc1 != "9") ~ "N",
      TRUE ~ "U"
    ),
    obstet_proc_unknown = case_when(
      obproc1 == "9" ~ "Y",
      !obproc1 %in% c("8", "9") ~ "N",
      TRUE ~ "U"
    )
  ) %>%
  select(-contains("obproc"), -cephaflg)

#### Labor onset ####
bir_2013_2016 <- bir_2013_2016 %>% 
  mutate(
    ruptured_membranes = case_when(
      labons1 == "01" ~ "Y",
      !labons1 %in% c("99", "09") ~ "N",
      TRUE ~ "U"
    ),
    precipitous_labor = case_when(
      labons1 == "02" | labons2 == "02" ~ "Y",
      !labons1 %in% c("99", "09") ~ "N",
      TRUE ~ "U"
    ),
    prolonged_labor = case_when(
      labons1 == "03" | labons2 == "03" | labons3 == "03" ~ "Y",
      !labons1 %in% c("99", "09") ~ "N",
      TRUE ~ "U"
    ),
    labor_onset_none = case_when(
      labons1 == "04" ~ "Y",
      labons1 != "04" & !labons1 %in% c("99", "09") ~ "N",
      TRUE ~ "U"
    ),
    labor_onset_unknown = case_when(
      labons1 %in% c("99", "09") ~ "Y",
      labons1 != "99" ~ "N",
      TRUE ~ "U"
    )
  ) %>%
  select(-contains("labons"))


#### Characteristics of labor ####
# First set up lists of new vars and their equivalent code
labchar_name <- list("induction", "augmentation", "non_vertex_presentation", 
                     "anesthesia", "steroids", "antibiotics", "chorioamnionitis", 
                     "meconium_stain", "fetal_intolerance")
labchar_num <- list("01", "02", "03", "04", "05", "06", "07", "08", "09")

# Write a function to recode new var
labchar_f <- function(df, name, num) {
  df <- df %>%
    mutate(!!name := case_when(
      (labchar1 == num | labchar2 == num | labchar3 == num | labchar4 == num | 
         labchar5 == num | labchar6 == num | labchar7 == num) ~ "Y",
      labchar1 == "10" ~ "N",
      labchar1 != num & labchar1 != "99" & 
        (labchar2 != num | is.na(labchar2)) & (labchar3 != num | is.na(labchar3)) &
        (labchar4 != num | is.na(labchar4)) & (labchar5 != num | is.na(labchar5)) & 
        (labchar6 != num | is.na(labchar6)) & (labchar7 != num | is.na(labchar7)) ~ "N",
      TRUE ~ "U"
    )) %>%
    # Just keep the new column and the cert number to join on
    select(birth_cert_encrypt, !!name)
}

# Run function and collapse list of results into a single df
labchar_out <- purrr::map2(labchar_name, labchar_num, 
                           ~ labchar_f(df = bir_2013_2016, name = .x, num = .y)) %>%
  purrr::reduce(left_join, by = "birth_cert_encrypt")

# Bind new columns to existing data frame
bir_2013_2016 <- left_join(bir_2013_2016, labchar_out, by = "birth_cert_encrypt")

# Make infections_none and infections_unknown and remove old vars
bir_2013_2016 <- bir_2013_2016 %>%
  mutate(
    mat_labor_characteristics_none = case_when(
      labchar1 == "10" ~ "Y",
      labchar1 != "10" & labchar1 != "99" ~ "N",
      TRUE ~ "U"
    ),
    mat_labor_characteristics_unknow = case_when(
      labchar1 == "99" ~ "Y",
      labchar1 != "99" ~ "N",
      TRUE ~ "U"
    )
  ) %>%
  select(-contains("labchar"))

# Clean up objects
rm(labchar_name, labchar_num, labchar_f, labchar_out)


#### Maternal morbidity ####
# First set up lists of new vars and their equivalent code
mmorbid_name <- list("maternal_transfusion", "perineal_laceration", "ruptured_uterus", 
                     "hysterectomy", "intensive_care", "operation")
mmorbid_num <- list("01", "02", "03", "04", "05", "06")

# Write a function to recode new var
mmorbid_f <- function(df, name, num) {
  df <- df %>%
    mutate(!!name := case_when(
      mmorbid1 == num | mmorbid2 == num | mmorbid3 == num | 
        mmorbid4 == num | mmorbid5 == num ~ "Y",
      mmorbid1 == "07" ~ "N",
      mmorbid1 != num & mmorbid1 != "99" & 
        (mmorbid2 != num | is.na(mmorbid2)) & (mmorbid3 != num | is.na(mmorbid3)) &
        (mmorbid4 != num | is.na(mmorbid4)) & (mmorbid5 != num | is.na(mmorbid5)) ~ "N",
      TRUE ~ "U"
    )) %>%
    # Just keep the new column and the cert number to join on
    select(birth_cert_encrypt, !!name)
}

# Run function and collapse list of results into a single df
mmorbid_out <- purrr::map2(mmorbid_name, mmorbid_num, 
                           ~ mmorbid_f(df = bir_2013_2016, name = .x, num = .y)) %>%
  purrr::reduce(left_join, by = "birth_cert_encrypt")

# Bind new columns to existing data frame
bir_2013_2016 <- left_join(bir_2013_2016, mmorbid_out, by = "birth_cert_encrypt")

# Make infections_none and infections_unknown and remove old vars
bir_2013_2016 <- bir_2013_2016 %>%
  mutate(
    maternal_morbidity_none = case_when(
      mmorbid1 == "07" ~ "Y",
      mmorbid1 != "07" & mmorbid1 != "99" ~ "N",
      TRUE ~ "U"
    ),
    maternal_morbidity_unknown = case_when(
      mmorbid1 == "99" ~ "Y",
      mmorbid1 != "99" ~ "N",
      TRUE ~ "U"
    )
  ) %>%
  select(-contains("mmorbid"))

# Clean up objects
rm(mmorbid_name, mmorbid_num, mmorbid_f, mmorbid_out)


#### Abnormal conditions of newborn ####
# First set up lists of new vars and their equivalent code
abcond_name <- list("assist_vent", "assist_vent_6", "nicu_admission", "surfactant", 
                    "child_antibiotics", "child_seizure", "child_injury")
abcond_num <- list("01", "02", "03", "04", "05", "06", "07")

# Write a function to recode new var
abcond_f <- function(df, name, num) {
  df <- df %>%
    mutate(!!name := case_when(
      abcond1 == num | abcond2 == num | abcond3 == num | 
        abcond4 == num | abcond5 == num | abcond6 == num ~ "Y",
      abcond1 == "08" ~ "N",
      abcond1 != num & abcond1 != "99" & 
        (abcond2 != num | is.na(abcond2)) & (abcond3 != num | is.na(abcond3)) &
        (abcond4 != num | is.na(abcond4)) & (abcond5 != num | is.na(abcond5)) &
        (abcond6 != num | is.na(abcond6)) ~ "N",
      TRUE ~ "U"
    )) %>%
    # Just keep the new column and the cert number to join on
    select(birth_cert_encrypt, !!name)
}

# Run function and collapse list of results into a single df
abcond_out <- purrr::map2(abcond_name, abcond_num, 
                          ~ abcond_f(df = bir_2013_2016, name = .x, num = .y)) %>%
  purrr::reduce(left_join, by = "birth_cert_encrypt")

# Bind new columns to existing data frame
bir_2013_2016 <- left_join(bir_2013_2016, abcond_out, by = "birth_cert_encrypt")

# Make infections_none and infections_unknown and remove old vars
bir_2013_2016 <- bir_2013_2016 %>%
  mutate(
    abnormal_conditions_none = case_when(
      abcond1 == "08" ~ "Y",
      abcond1 != "08" & !abcond1 %in% c("99", "09") ~ "N",
      TRUE ~ "U"
    ),
    abnormal_conditions_unknown = case_when(
      abcond1 %in% c("99", "09") ~ "Y",
      !abcond1 %in% c("99", "09") ~ "N",
      TRUE ~ "U"
    )
  ) %>%
  select(-contains("abcond"))

# Clean up objects
rm(abcond_name, abcond_num, abcond_f, abcond_out)


#### Congenital malformations ####
# First set up lists of new vars and their equivalent code
malf_name <- list("anencephaly", "msb", "congenital_heart", 
                  "congenital_hernia", "omphalocele", "gastroschisis",
                  "limb_defect", "cleft_lip", "cleft_palate",
                  "down_syndrome", "other_chrom", "hypospadias")
malf_num <- list("01", "02", "03", "04", "05", "06", "07", "08", "09",
                 "10", "11", "12")

# Write a function to recode new var
malf_f <- function(df, name, num) {
  df <- df %>%
    mutate(!!name := case_when(
      malf1 == num | malf2 == num | malf3 == num | malf4 == num | 
        malf5 == num | malf6 == num | malf7 == num ~ "Y",
      malf1 == "08" ~ "N",
      malf1 != num & malf1 != "99" & 
        (malf2 != num | is.na(malf2)) & (malf3 != num | is.na(malf3)) &
        (malf4 != num | is.na(malf4)) & (malf5 != num | is.na(malf5)) &
        (malf6 != num | is.na(malf6)) & (malf7 != num | is.na(malf7)) ~ "N",
      TRUE ~ "U"
    )) %>%
    # Just keep the new column and the cert number to join on
    select(birth_cert_encrypt, !!name)
}

# Run function and collapse list of results into a single df
malf_out <- purrr::map2(malf_name, malf_num, 
                        ~ malf_f(df = bir_2013_2016, name = .x, num = .y)) %>%
  purrr::reduce(left_join, by = "birth_cert_encrypt")

# Bind new columns to existing data frame
bir_2013_2016 <- left_join(bir_2013_2016, malf_out, by = "birth_cert_encrypt")

# Make other flags + infections_none and infections_unknown and remove old vars
bir_2013_2016 <- bir_2013_2016 %>%
  mutate(
    down_confirm = case_when(
      down_syndrome == "Y" & downsflg %in% c("C", "Y") ~ "Y",
      down_syndrome == "Y" & downsflg == "P" ~ "N",
      TRUE ~ "U"
    ),
    down_pending = case_when(
      down_syndrome == "Y" & downsflg == "P" ~ "Y",
      down_syndrome == "Y" & downsflg %in% c("C", "Y") ~ "N",
      TRUE ~ "U"
    ),
    other_chrom_confirm = case_when(
      other_chrom == "Y" & downsflg == "C" ~ "Y",
      other_chrom == "Y" & downsflg == "P" ~ "N",
      TRUE ~ "U"
    ),
    other_chrom_pending = case_when(
      other_chrom == "Y" & downsflg == "P" ~ "Y",
      other_chrom == "Y" & downsflg == "C" ~ "N",
      TRUE ~ "U"
    ),
    congenital_anomaly_none = case_when(
      malf1 == "13" ~ "Y",
      malf1 != "13" & !malf1 %in% c("99", "18") ~ "N",
      TRUE ~ "U"
    ),
    congenital_anomaly_unknown = case_when(
      malf1 %in% c("99", "18") ~ "Y",
      !malf1 %in% c("99", "18") ~ "N",
      TRUE ~ "U"
    )
  ) %>%
  select(-contains("malf"), -chromflg, -downsflg)

# Clean up objects
rm(malf_name, malf_num, malf_f, malf_out)



#### RENAME FIELDS THAT DO NOT EXIST IN THE NEW SYSTEM ####
bir_2013_2016 <- bir_2013_2016 %>%
  rename(nchs_new_record = nchsnew) # This appears to always be NA


#### FIX UP MOTHER/FATHER PLACE OF BIRTH ####
# birplmom and birpldad need to be converted to ISO-3166 codes and renamed to mother/father_birthplace_state_fips

### Bring in reference tables
iso_3166 <- vroom::vroom("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/general/ref/ref.iso_3166_country_subcountry_codes.csv")
nchs <- vroom::vroom("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/general/ref/ref.nchs_country_state.csv")

# Restrict to relevant level
iso_3166 <- iso_3166 %>% filter(iso3166_1_name == "United States")
nchs <- nchs %>% filter(nchs_level == "state")

### Join together and make some manual additions
ref_country <- left_join(nchs, iso_3166, by = c("nchs_name" = "iso3166_2_name")) %>%
  mutate(
    iso3166_1_code = case_when(
      nchs_name == "Canada" ~ "CA",
      nchs_name == "Cuba" ~ "CU",
      nchs_name == "Mexico" ~ "MX",
      nchs_name %in% c("Marianas", "United States (Unspecified)") ~ "US",
      TRUE ~ iso3166_1_code
    ),
    iso3166_1_name = case_when(
      nchs_name == "Canada" ~ "Canada",
      nchs_name == "Cuba" ~ "Cuba",
      nchs_name == "Mexico" ~ "Mexico",
      nchs_name %in% c("Marianas", "United States (Unspecified)") ~ "United States",
      TRUE ~ iso3166_1_name
    ),
    iso3166_2_full_code = case_when(
      nchs_name == "Marianas" ~ "US-MP",
      TRUE ~ iso3166_2_full_code
    ),
    iso3166_2_code = case_when(
      nchs_name == "Marianas" ~ "MP",
      TRUE ~ iso3166_2_code
    )
  )

### Pull in just mother/father birth place columns  and cert no
bir_place <- bir_2013_2016 %>% select(birth_cert_encrypt, birplmom, birpldad) %>%
  # Join to ref table for mother's bir place
  left_join(., select(ref_country, nchs_code, iso3166_2_code), 
            by = c("birplmom" = "nchs_code")) %>%
  # Join to ref table for father's birplace
  left_join(., select(ref_country, nchs_code, iso3166_2_code), 
            by = c("birpldad" = "nchs_code")) %>%
  rename(mother_birthplace_state_fips = iso3166_2_code.x,
         father_birthplace_state_fips = iso3166_2_code.y) %>%
  select(birth_cert_encrypt, mother_birthplace_state_fips, father_birthplace_state_fips)

### Join back to main data and drop old fields
bir_2013_2016 <- left_join(bir_2013_2016, bir_place, by = "birth_cert_encrypt") %>%
  select(-birplmom, -birpldad)

### Remove objects
rm(iso_3166, nchs, ref_country, bir_place)


#### ALIGN VARIABLE TYPES ####
    setnames(bir_2013_2016, 
             c("res_yprt", "res_mprt", "dmeth1", "attclass", "crt_clas", "gestflag"), 
             c("mother_years_at_residence", "mother_months_at_residence", "delivery_method_calculation", "attendant_class", "certifier_class", "gestation_calculated_flag") 
              )
    
    bir_2013_2016 <- bir_2013_2016 %>%
      mutate_at(vars(mother_years_at_residence, mother_months_at_residence,
                     delivery_method_calculation, attendant_class, certifier_class),
                funs(as.numeric(.)))
    setDT(bir_2013_2016)
    
    
    bir_2017_20xx <- bir_2017_20xx %>%
      mutate_at(vars(gestation_calculated_flag),
                funs(as.numeric(.)))
    setDT(bir_2017_20xx)

#### BRING NEW AND OLD DATA TOGETHER ####
bir_combined <- bind_rows(bir_2017_20xx, bir_2013_2016)
bir_combined <- setDT(bir_combined)

#### STANDARDIZE MISSING TO BE NULL ####
# Can catch a lot of variables in one go 
col_char <- names(bir_combined)[sapply(bir_combined, is.character)]
for (j in col_char) {
  set(bir_combined, i = which(bir_combined[[j]] %in% c("U", "XX", "ZZ", "99:99", "99")), 
      j = j, value = NA_character_)
}

col_num <- names(bir_combined)[sapply(bir_combined, is.numeric)]
col_num <- col_num[!col_num == "mother_weight_gain"] # Remove as 99 is legitimate 
for (j in col_num) {
  set(bir_combined, i = which(bir_combined[[j]] %in% c(9999, 999, 99.9, 99)), 
      j = j, value = NA_integer_)
}


# Now get specific variables
# (can't use 9 in the catch-all above because it is a non-NA code for some vars)
col_char_9 <- c("child_calculated_race", "mother_race_calculation", "father_race_calculation")
for (j in col_char_9) {
  set(bir_combined, i = which(bir_combined[[j]] %in% c("9", "09")), j = j, value = NA_character_)
}

col_num_9 <- c("facility_type", "intended_facility", "child_calculated_ethnicity",
               "mother_hispanic", "mother_education", "mother_educ_8th_grade_or_less",
               "father_hispanic", "father_education", "father_educ_8th_grade_or_less",
               "mother_height_feet", "source_of_payment", "plurality", "birth_order",
               "attendant_class", "certifier_class")
for (j in col_num_9) {
  set(bir_combined, i = which(bir_combined[[j]] == 9), j = j, value = NA_integer_)
}


# Clean up objects
rm(col_char, col_num, col_char_9, col_num_9, bir_2013_2016, bir_2017_20xx)


#### CHANGE COLUMN TYPES TO INTEGER WHERE POSSIBLE ####
  to.numeric <- function(my.dt){
    my.cols <- names(my.dt)
    for(i in 1:length(my.cols)){
      added.NAs <- nrow(my.dt[is.na(get(my.cols[i])), ]) - 
        suppressWarnings(nrow(my.dt[is.na(as.numeric(gsub(" ", "", get(my.cols[i])))), ])) # Additional NAs if converted to numeric
      if(added.NAs==0){
        my.dt[, my.cols[i] := suppressWarnings(as.numeric(get(my.cols[i])))]
      }
    }    
    
    return(my.dt)
  }
  
  bir_combined <- to.numeric(bir_combined)





#### LOAD TO SQL ####
# Need to manually truncate table so can use overwrite = F below (so column types work)
dbGetQuery(conn, glue_sql("TRUNCATE TABLE {`table_config_stage_bir_wa$schema`}.{`table_config_stage_bir_wa$table`}",
                          .con = conn))

tbl_id_2013_2016 <- DBI::Id(schema = table_config_stage_bir_wa$schema, 
                            table = table_config_stage_bir_wa$table)
dbWriteTable(conn, tbl_id_2013_2016, value = as.data.frame(bir_combined),
             overwrite = F,
             append = T,
             field.types = paste(names(table_config_stage_bir_wa$vars), 
                                 table_config_stage_bir_wa$vars, 
                                 collapse = ", ", sep = " = "))





