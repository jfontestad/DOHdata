#### Header ----
  # Author(s): Alastair Matheson / Danny Colombara, PHSKC (APDE)
  #
  # Date: 2019-06
  #
  # Purpose: COMBINE BIRTH DATA FROM THE BEDROCK (2003-2016) AND WHALES (2017-) SYSTEMS
  #          Clean and recode variables needed for APDE analyses
  
  # Recode groups of variables where <group_name>1-<group_nameX> has been replaced
  #     by specific flags rather than codes
  # Standardize missing to be NULL
  # Standardize country/county/state/city codes (not needed as there is literal and code now)


#### Free memory ####
  rm(list=setdiff(ls(), "db_apde")) # only keep SQL connection
  gc()

#### Load additional functions ####
source("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/stage/enact_recoding_function.R")

#### LOAD REFERENCE DATA ####
table_config_stage_bir_wa <- yaml::yaml.load(getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/stage/create_stage.bir_wa.yaml"))

recodes <- data.table::fread("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/ref/ref.bir_recodes_simple.csv")
recodes[new_label %in% c("", " "), new_label := NA] # CRITICAL, because fread reads empty strings as "" and recode function tries to use "" as a label, which causes it to fail

chi.geographies <- data.table::fread("https://raw.githubusercontent.com/PHSKC-APDE/reference-data/master/spatial_data/chi_hra_xwalk.csv")

iso_3166 <- data.table::fread("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/general/ref/ref.iso_3166_country_subcountry_codes.csv", colClasses="character")
iso_3166.us <- iso_3166[iso3166_1_name == "United States", ]
iso_3166.us <- iso_3166.us[!iso3166_2_name %in% c("American Samoa",	"Guam",	"Northern Mariana Islands", "Puerto Rico", "United States Minor Outlying Islands", "Virgin Islands")]$iso3166_2_code

#### SET UP PATH to SQL OUTPUT TABLE ---- 
    tbl_id_2003_20xx <- DBI::Id(schema = table_config_stage_bir_wa$schema, 
                            table = table_config_stage_bir_wa$table)

#### ________________________________________________________----    
####              COMBINE BEDROCK & WHALES                   ----    
#### ________________________________________________________----  

#### PULL IN BOTH DATA SETS ####
    tbl_id_2003_2016 <- DBI::Id(schema = "load_raw", table = "bir_wa_2003_2016")
    bir_2003_2016 <- DBI::dbReadTable(db_apde, tbl_id_2003_2016)
    
    tbl_id_2017_20xx <- DBI::Id(schema = "load_raw", table = "bir_wa_2017_20xx")
    bir_2017_20xx <- DBI::dbReadTable(db_apde, tbl_id_2017_20xx)

#### REMOVE FIELDS IN BEDROCK NOT COLLECTED AFTER 2003 ####
### NB. Need to do this BEFORE renaming variables because otherwise two 
###   WIC variables are created
### Remove variables not collected after 2003 or are 100% missing
bir_2003_2016 <- bir_2003_2016 %>%
  select(-fd_lt20, -fd_ge20, -apgar1, -ind_num, -malf_sam, -herpes, 
         -smokenum, -drinknum, -contains("amnio"), -dmeth2, -dmeth3, -dmeth4, 
         -contains("complab"), -racecdes, -hispcdes, -carepay, -wic, -firsteps, 
         -afdc, -localhd, -contains("ubleed"), -smoking, -drinking, -lb_f_nl, 
         -lb_f84, -lb_p_nl, -lb_p84, -lb_p_nd, -lb_f_nd, -fd_2036, -fd_ge37, 
         -contains("malficd"), -contains("mrfnon"), -ind_mo, -ind_yr, -lfd_mo, 
         -lfd_yr, -loth_mo, -loth_yr, -lpp_mo, -lpp_yr, -trib_res,
         -nchsnew) %>%
  # Also remove geozip, which is in the geocoded birth file
  select(-geozip)


#### STANDARDIZE NAMES FROM BEDROCK TO WHALES ####
### Bring in reference table
field_maps <- data.table::fread("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/ref/ref.bir_field_name_map.csv")

data.table::setnames(bir_2003_2016, 
                     field_maps$field_name_apde[match(names(bir_2003_2016), 
                                                      field_maps$field_name_bedrock)])

#### SPLIT OUT VARIABLES THAT ARE NOW INDIVIDUAL FLAGS ####
#### Medical risk factors ####
# Can rely on the fact that the code for diabetes (01) only ever went in mrf1
# and code for hypertension (02) only ever went in mrf1 and mrf2
# Also rely on the fact that if mrf1 = "09" or "99" then all other mrf fields are NA
bir_2003_2016 <- bir_2003_2016 %>% 
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
                       ~ mrf_f(df = bir_2003_2016, name = .x, num = .y)) %>%
  purrr::reduce(left_join, by = "birth_cert_encrypt")

# Bind new columns to existing data frame and remove old vars
bir_2003_2016 <- left_join(bir_2003_2016, mrf_out, by = "birth_cert_encrypt") %>%
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
                           ~ minfect_f(df = bir_2003_2016, name = .x, num = .y)) %>%
  purrr::reduce(left_join, by = "birth_cert_encrypt")

# Bind new columns to existing data frame
bir_2003_2016 <- left_join(bir_2003_2016, minfect_out, by = "birth_cert_encrypt")

# Make infections_none and infections_unknown and remove old vars
bir_2003_2016 <- bir_2003_2016 %>%
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
bir_2003_2016 <- bir_2003_2016 %>% 
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
bir_2003_2016 <- bir_2003_2016 %>% 
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
                           ~ labchar_f(df = bir_2003_2016, name = .x, num = .y)) %>%
  purrr::reduce(left_join, by = "birth_cert_encrypt")

# Bind new columns to existing data frame
bir_2003_2016 <- left_join(bir_2003_2016, labchar_out, by = "birth_cert_encrypt")

# Make infections_none and infections_unknown and remove old vars
bir_2003_2016 <- bir_2003_2016 %>%
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
                           ~ mmorbid_f(df = bir_2003_2016, name = .x, num = .y)) %>%
  purrr::reduce(left_join, by = "birth_cert_encrypt")

# Bind new columns to existing data frame
bir_2003_2016 <- left_join(bir_2003_2016, mmorbid_out, by = "birth_cert_encrypt")

# Make infections_none and infections_unknown and remove old vars
bir_2003_2016 <- bir_2003_2016 %>%
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
                          ~ abcond_f(df = bir_2003_2016, name = .x, num = .y)) %>%
  purrr::reduce(left_join, by = "birth_cert_encrypt")

# Bind new columns to existing data frame
bir_2003_2016 <- left_join(bir_2003_2016, abcond_out, by = "birth_cert_encrypt")

# Make infections_none and infections_unknown and remove old vars
bir_2003_2016 <- bir_2003_2016 %>%
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
                        ~ malf_f(df = bir_2003_2016, name = .x, num = .y)) %>%
  purrr::reduce(left_join, by = "birth_cert_encrypt")

# Bind new columns to existing data frame
bir_2003_2016 <- left_join(bir_2003_2016, malf_out, by = "birth_cert_encrypt")

# Make other flags + infections_none and infections_unknown and remove old vars
bir_2003_2016 <- bir_2003_2016 %>%
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

#### FIX UP MOTHER/FATHER PLACE OF BIRTH ####
# birplmom and birpldad need to be converted to ISO-3166 codes and renamed to mother/father_birthplace_state_fips

### Bring in reference tables
iso_3166 <- data.table::fread("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/general/ref/ref.iso_3166_country_subcountry_codes.csv", colClasses="character")
nchs <- data.table::fread("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/general/ref/ref.nchs_country_state.csv", colClasses="character")

# Restrict to relevant level
iso_3166 <- iso_3166 %>% filter(iso3166_1_name == "United States")
nchs.country <- nchs %>% filter(nchs_level == "country")
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
bir_place <- bir_2003_2016 %>% select(birth_cert_encrypt, birplmom, birpldad) %>%
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
bir_2003_2016 <- left_join(bir_2003_2016, bir_place, by = "birth_cert_encrypt") %>%
  select(-birplmom, -birpldad)

#### Identify the countries whose ids are given as mother_birthplace_cntry_wa_code ----
setDT(nchs.country) # convert to data.table
nchs.country[, nchs_level := NULL]
nchs.country[, nchs_name := toupper(nchs_name)]
setnames(nchs.country, names(nchs.country), c("mother_birthplace_cntry_wa_code", "mother_birthplace_country"))
bir_2003_2016<-merge(bir_2003_2016, nchs.country, by = "mother_birthplace_cntry_wa_code", all.x = TRUE, all.y = FALSE)
setnames(nchs.country, names(nchs.country), c("father_birthplace_cntry_wa_code", "father_birthplace_country"))
bir_2003_2016<-merge(bir_2003_2016, nchs.country, by = "father_birthplace_cntry_wa_code", all.x = TRUE, all.y = FALSE)

### Remove objects
rm(iso_3166, nchs, ref_country, bir_place)

#### ALIGN VARIABLE TYPES ####
bir_2003_2016 <- bir_2003_2016 %>%
  mutate_at(vars(mother_years_at_residence, mother_months_at_residence,
                 delivery_method_calculation, attendant_class, certifier_class),
            list( ~ as.numeric(.)))

bir_2017_20xx <- bir_2017_20xx %>%
  mutate_at(vars(gestation_calculated_flag),
            list( ~ as.numeric(.)))

#### BRING NEW AND OLD DATA TOGETHER ####
bir_combined <- setDT(bind_rows(bir_2017_20xx, bir_2003_2016))

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

# Clean up objects to free memory
  rm(col_char, col_num, col_char_9, col_num_9, bir_2003_2016, bir_2017_20xx)
  gc() 

#### CHANGE COLUMN TYPES TO INTEGER WHERE POSSIBLE ####
  # Replace blank with NA in character columns
  my.cols <- names(bir_combined)[sapply(bir_combined, is.character)] 
  for (jj in 1:ncol(bir_combined)) set(bir_combined, i = which(bir_combined[[jj]]==""), j = jj, v = NA) 
  for (jj in 1:ncol(bir_combined)) set(bir_combined, i = which(bir_combined[[jj]]==" "), j = jj, v = NA)
  for (jj in 1:ncol(bir_combined)) set(bir_combined, i = which(bir_combined[[jj]]=="  "), j = jj, v = NA)
  for (jj in 1:ncol(bir_combined)) set(bir_combined, i = which(bir_combined[[jj]]=="   "), j = jj, v = NA)
  for (jj in 1:ncol(bir_combined)) set(bir_combined, i = which(bir_combined[[jj]]=="    "), j = jj, v = NA)
  
  to.numeric <- function(my.dt){
    my.cols <- names(my.dt)[sapply(my.dt, is.character)] # get vector of all character columns
    my.cols <- setdiff(my.cols, grep("race_nchs", my.cols, value = TRUE)) # These race vars should remain characters
    my.cols <- setdiff(my.cols, grep("_month$", my.cols, value = TRUE)) # Alastair coded all months as characters
    my.cols <- setdiff(my.cols, grep("_day$", my.cols, value = TRUE))   # Alastair coded all days as characters
    my.cols <- setdiff(my.cols, c("birthplace_county_city_wa_code", "mother_residence_city_wa_code"))
    for(i in 1:length(my.cols)){
      
      message(paste0("Testing ", i, " of ", length(my.cols), ": ", my.cols[i], " ...", gsub(Sys.Date(), "", Sys.time())))
    
      added.NAs <- nrow(my.dt[is.na(get(my.cols[i])), ]) - 
        suppressWarnings(nrow(my.dt[is.na(as.numeric(gsub(" ", "", get(my.cols[i])))), ])) # Additional NAs if converted to numeric
      
      if(added.NAs==0){
        message(paste0("     Converting ", my.cols[i], " to numeric"))
        my.dt[, my.cols[i] := suppressWarnings(as.numeric(get(my.cols[i])))]
      }
    }    
    
    #return(my.dt)
  }
  
  to.numeric(bir_combined)
  
#### FINAL SMALL DATA TWEAKS ----
    # FIX YEAR FOR 2009 because needed for automated recoding that follows ----
      bir_combined[date_of_birth_year==9, date_of_birth_year := 2009]  
    
    # CHANGE DATE COLUMNS TO DATE TYPE ----
      date.cols <- c("date_first_prenatal_visit",	"date_last_menses",	"date_last_prenatal_visit",	"father_date_of_birth",	"mother_date_of_birth")
      bir_combined[, (date.cols) := lapply(.SD, as.Date), .SDcols = date.cols]
      
      # clean dates -- replace with NA when date is after the year of birth (not possible)
        for(i in 1:length(date.cols)){
          bir_combined[year(get(date.cols[i])) > date_of_birth_year, date.cols[i] := NA]
        }  
    
    # FIX CERT NUMBER FOR 2012 ----
    # Warning: max integer size is 2147483648 so this code will break in ~140 years
    bir_combined[date_of_birth_year == 2012, 
                 birth_cert_encrypt := as.integer(paste0(2012L, birth_cert_encrypt))]   
  
#### ________________________________________________________----    
####                     RECODING                            ----    
#### ________________________________________________________----    
    
#### FREE UP MEMORY ----
  gc()

#### Prep simple recode instructions ----
  complex.vars <- recodes[recode_type=="complex"]$new_var # save list of vars made with complex recodes
  recodes <- recodes[recode_type != "complex"] # drop complex recodes from list of simple recodes
  recodes <- recodes[, .(old_var, new_var, old_value, new_value, new_label, start_year, end_year, var_label)] # keep columns needed for function

#### Process simple recodes with package ----
  bir_combined <- enact_recoding(data = bir_combined, 
                                 parse_recode_instructions(recodes, catch_NAs = T), 
                                 ignore_case = T, 
                                 hypothetical = F) 
  
#### Custom code for complex recoding ----
  # Not in alphabetical order because some vars are dependant upon other vars
  # bw_low_sing ----
    bir_combined[!is.na(bw_low), bw_low_sing := 0]
    bir_combined[bw_low == 1 & singleton == 1, bw_low_sing := 1]
  
  # bw_vlow_sing ----
    bir_combined[!is.na(bw_vlow), bw_vlow_sing := 0]
    bir_combined[bw_vlow == 1 & singleton == 1, bw_vlow_sing := 1]
    
  # chi_geo_zip5  ----
    bir_combined[, chi_geo_zip5 := mother_residence_zip]
    bir_combined[chi_geo_zip5 == "99999", chi_geo_zip5 := NA]
    
  # cigarettes_smoked_3_months_prior ----
    bir_combined[is.na(cigarettes_smoked_3_months_prior) & chi_year >=2017, cigarettes_smoked_3_months_prior := 0]
    bir_combined[cigarettes_smoked_3_months_prior == 0, smokeprior := 0]
  
  # chi_race_aic_asian ----
    # no simple way to get this. Use the aic_ variables created with simple recoding above
    bir_combined[is.na(chi_race_aic_asianother) + is.na(chi_race_aic_chinese) + is.na(chi_race_aic_filipino) +
                   is.na(chi_race_aic_japanese) + is.na(chi_race_aic_korean) + is.na(chi_race_aic_vietnamese) < 6, chi_race_aic_asian := 0]
    bir_combined[(chi_race_aic_asianother + chi_race_aic_chinese + chi_race_aic_filipino +
                       chi_race_aic_japanese + chi_race_aic_korean + chi_race_aic_vietnamese) > 0, chi_race_aic_asian := 1]
    
  # cigarettes_smoked_1st_tri ----
    bir_combined[is.na(cigarettes_smoked_1st_tri) & chi_year >=2017, cigarettes_smoked_1st_tri := 0]
    bir_combined[cigarettes_smoked_1st_tri == 0, smoke1 := 0]
  
  # cigarettes_smoked_2nd_tri ----
    bir_combined[is.na(cigarettes_smoked_2nd_tri) & chi_year >=2017, cigarettes_smoked_2nd_tri := 0]
    bir_combined[cigarettes_smoked_2nd_tri == 0, smoke2 := 0]
  
  # cigarettes_smoked_3rd_tri ----
    bir_combined[is.na(cigarettes_smoked_3rd_tri) & chi_year >=2017, cigarettes_smoked_3rd_tri := 0]      
    bir_combined[cigarettes_smoked_3rd_tri == 0, smoke3 := 0]       
  
  # diab_no (Diabetes-No) ----
    bir_combined[diab_gest == 0 & diab_prepreg == 0, diab_no := 1] 
    bir_combined[diab_gest == 1 | diab_prepreg == 1, diab_no := 0]
  
  # htn_no (Hypertension-No) ----
    bir_combined[htn_gest == 0 & htn_prepreg == 0, htn_no := 1] 
    bir_combined[htn_gest == 1 | htn_prepreg == 1, htn_no := 0]
  
  # nullip (nulliparous) ----
    bir_combined[prior_live_births_living == 0 & prior_live_births_deceased == 0, nullip := 1]
    bir_combined[prior_live_births_living %in% c(1:98) | prior_live_births_deceased %in% c(1:98), nullip := 0]
  
  # vertex (vertex birth position) ----
    bir_combined[fetal_pres == 1, vertex := 0]
    bir_combined[fetal_pres %in% 2:3, vertex := 1]
    bir_combined[non_vertex_presentation=="N", vertex := 1]
    bir_combined[, vertex := factor(vertex, levels = c(0, 1), labels = c("Breech/Oth", "Vertex"))]
  
  # ntsv (nulliparous, term, singleton, vertex birth) ----
    bir_combined[, ntsv := 0] 
    bir_combined[(nullip == 1 & term == 1 & plurality == 1 & vertex == "Vertex"), ntsv := 1]
  
  # csec_lowrisk (Cesarean section among low risk deliveries) ----
    bir_combined[delivery_final %in% c(4:6), csec_lowrisk := 0] # any c-section == 0 
    bir_combined[delivery_final %in% c(4:6) & ntsv==1, csec_lowrisk:=1]    
  
  # mother_birthplace_usa ----
    bir_combined[is.na(mother_birthplace_country) + is.na(mother_birthplace_state_fips) != 0, mother_birthplace_usa := 0] # default to zero if have mother birth location data
    bir_combined[mother_birthplace_country == "UNITED STATES", mother_birthplace_usa := 1]
    bir_combined[mother_birthplace_state_fips %in% iso_3166.us, mother_birthplace_usa := 1]
  
  # mother_birthplace_foreign ----
    bir_combined[mother_birthplace_usa == 1, mother_birthplace_foreign := 0]
    bir_combined[mother_birthplace_usa == 0, mother_birthplace_foreign := 1]
  
  # nativity ----
    bir_combined[mother_birthplace_usa == 0, chi_nativity := "Foreign born"]
    bir_combined[mother_birthplace_usa == 1, chi_nativity := "Born in US"]
    
  # pnc_lateno (Late or no prenatal care) ----
    bir_combined[month_prenatal_care_began %in% c(1:6), pnc_lateno := 0]
    bir_combined[month_prenatal_care_began %in% c(0, 7:10), pnc_lateno := 1]
  
  # ch_priorpreg ----
    # ch_priorpreg 2003-2016 was done with simple recodes, but coding changed starting with 2017
    bir_combined[chi_year >=2017, ch_priorpreg := prior_live_births_deceased + prior_live_births_living + other_preg_outcomes]
    bir_combined[ch_priorpreg %in% 1:50, ch_priorpreg := 1]
  
  # smoking (Smoking-Yes (before &|or during pregnancy)) ----
    bir_combined[, smoking := NA_integer_]
    bir_combined[(smokeprior==0 & smoke1==0 & smoke2==0 & smoke3==0), smoking := 0]
    bir_combined[(smokeprior==1 & smoke1==1 & smoke2==1 & smoke3==1), smoking := 1]
  
  # smoking_dur (whether mother smoked at all during pregnancy) ----
    bir_combined[, smoking_dur := NA_integer_]
    bir_combined[(smoke1==0 & smoke2==0 & smoke3==0), smoking_dur := 0]
    bir_combined[(smoke1 == 1 | smoke2 == 1 | smoke3 == 1), smoking_dur := 1]
  
  # wtgain (CHAT categories of maternal weight gain) ----
    bir_combined[
      ((mother_bmi<18.5 & mother_weight_gain<28) | 
         (mother_bmi>=18.5 & mother_bmi<=24.99 & mother_weight_gain<25) | 
         (mother_bmi>=25.0 & mother_bmi<=29.99 & mother_weight_gain<15) | 
         (mother_bmi>=30.0 & mother_bmi<99.9 & mother_weight_gain<11)), 
      wtgain := 1]
  
    bir_combined[is.na(wtgain) & 
                   ((mother_bmi<18.5 & mother_weight_gain>=28 & mother_weight_gain<=40) | 
                      (mother_bmi>=18.5 & mother_bmi<=24.9 & mother_weight_gain>=25 & mother_weight_gain<=35) | 
                      (mother_bmi>=25.0 & mother_bmi<=29.9 & mother_weight_gain>=15 & mother_weight_gain<=25) | 
                      (mother_bmi>=30.0 & mother_bmi<99.9 & mother_weight_gain>=11 & mother_weight_gain<=20)), 
                 wtgain := 2]
    
    bir_combined[is.na(wtgain) & 
                   ((mother_bmi<18.5 & mother_weight_gain>40) | 
                      (mother_bmi>=18.5 & mother_bmi<=24.9 & mother_weight_gain>35) | 
                      (mother_bmi>=25.0 & mother_bmi<=29.9 & mother_weight_gain>25) | 
                      (mother_bmi>=30.0 & mother_bmi<99.9 & mother_weight_gain>20 & mother_weight_gain<999)), 
                 wtgain := 3]       
  
    bir_combined[, wtgain := factor(wtgain, levels = c(1:3), labels = c("below recommended", "recommended", "above recommended"))]
  
  # wtgain_rec (WT Gain-Recommended) ----
    bir_combined[wtgain %in% c("below recommended", "above recommended"),  wtgain_rec := 0]
    bir_combined[wtgain=="recommended", wtgain_rec := 1]
  
  # Explanation of Kotelchuck Index ----
    # In 2019 we spent a long time trying to figure out the correct Kotelchuck index
    # We created three indices:
    #	  1) Translating the original SAS code produced by Kotelchuck into R
    #   2) Translating the SQL code produced by WA DOH into R
    #   3) Alastair's method, based on a table founding online (xxx)
    # The end result is that Alastair's method is the simplest and the most comparable
    # to the results founding in CHAT. For this reason, we are going to use Alastair's method
    # unless directed to do otherwise. 
  
  # kotelchuck (Kotelchuck Index Alastair Matheson Method) ----
    # "Expected number of visits" matrix is what was historically presented on the WA DOH website,
    #   The top is the month that care began and the row name is the gestational age
    #   see the following links:
    #     http://publichealth.lacounty.gov/mch/fhop/FHOP06/FHOP06_pdf/Appendix.pdf and
    #     https://web.archive.org/web/20060816041014/https:/www.doh.wa.gov/EHSPHL/CHS/Vista/Statistical_calculations.htm#KOTELCHUCK INDEX
    #     These links stop at gestational age of 38. For gestational age of 39-50, assume that need a weekly prenatal care visit
    #     Though I was unable to find a similar table in an ACOG publication, the pattern matches ACOG recommentations 
  
  # Step 1: created matrix of expected number of visits given month PNC began and the gestational age
    expected.pnc <- matrix(
      c(
        rep(1, 4), rep(2, 4), rep(3, 4), rep(4, 4), rep(5, 4), rep(6, 4), rep(7, 2), rep(8, 2), rep(9, 2), 10, 11, seq(12, 24), # began first month
        rep(NA, 4), rep(1, 4), rep(2, 4), rep(3, 4), rep(4, 4), rep(5, 4), rep(6, 2), rep(7, 2), rep(8, 2), 9, 10, seq(11, 23), # began second month
        rep(NA, 8), rep(1, 4), rep(2, 4), rep(3, 4), rep(4, 4), rep(5, 2), rep(6, 2), rep(7, 2), 8, 9, seq(10, 22), # began third month
        rep(NA, 12), rep(1, 4), rep(2, 4), rep(3, 4), rep(4, 2), rep(5, 2), rep(6, 2), 7, 8, seq(9, 21) # began fourth month
      ), 
      nrow = 45, ncol = 4, byrow = FALSE, 
      dimnames = list(seq(from = 6, to = 50), seq(from = 1, to = 4)))
    
  # Step 2: merge expected number of visits onto the main dataset, based on month PNC began and gestational age at birth 
    bir_combined$expected.pnc <- expected.pnc[cbind(
      match(bir_combined$calculated_gestation, rownames(expected.pnc)), 
      match(bir_combined$month_prenatal_care_began, colnames(expected.pnc))
    )]
  
  # Step 3: Compare actual to the expected to calculate Adequacy of PNC based on kotelchuck          
    # Determine adequacy of prenatal care
      bir_combined[month_prenatal_care_began %in% c(0, 5:14), kotelchuck := 0]  # inadequate
      bir_combined[number_prenatal_visits < 0.8 * expected.pnc, kotelchuck := 0] # less than 80% of expected is inadequate
      bir_combined[number_prenatal_visits >= 0.8 * expected.pnc, kotelchuck := 1] # greater than or equal to 80% is adequate
      bir_combined[, expected.pnc := NULL] # no longer needed, just intermediate to get kotelchuck
    
    # Set to NA when underlying variables are missing or illogical
      bir_combined[is.na(month_prenatal_care_began) | is.na(calculated_gestation) | calculated_gestation == 0, kotelchuck := NA]
      bir_combined[is.na(number_prenatal_visits) | number_prenatal_visits == 99, kotelchuck := NA]
      bir_combined[(month_prenatal_care_began > (calculated_gestation/4)), kotelchuck := NA]
      bir_combined[(month_prenatal_care_began == 0 & number_prenatal_visits >=1) | (number_prenatal_visits == 0 & month_prenatal_care_began >= 1), kotelchuck := NA]
    
# CONVERT CHI VARS TO FACTORS AND THEN TO CHARACTERS WHEN POSSIBLE ----
    for(i in unique(recodes[!is.na(new_value) & !new_label %in% c(NA, "No", "Yes", "")]$new_var)){ # Use dictionary to identify factor variables
      print(i) # helpful for troubleshooting so know where it breaks
      my.levels <- recodes[new_var== i & !is.na(new_value)]$new_value # get levels for the given factor variable
      my.labels <- recodes[new_var == i & !is.na(new_value)]$new_label # get labels for the given factor variable      
      bir_combined[, eval(i) := as.character(factor(get(i), levels = my.levels, labels = my.labels))] # apply the factor label to the factor variable in dt
    }
    
# IDENTIFY & ADD GEOGRAPHIES ----
    # Pull in geocoded data
    geo.data <- odbc::dbGetQuery(db_apde, "SELECT birth_cert_encrypt, res_geo_census_full_2010 FROM [stage].[bir_wa_geo]") # get complete block ids
    
    # Merge on geocoded data to the main birth data
    bir_combined <- merge(bir_combined, geo.data, by.x = "birth_cert_encrypt", by.y = "birth_cert_encrypt", all.x = T, all.y = T) 
    
    # Merge on standard RADS/CHI geographies, linking to 2010 census blocks
    chi.geographies[, geo_id_blk10 := as.character(geo_id_blk10)]
    bir_combined <- merge(bir_combined, chi.geographies, by.x = "res_geo_census_full_2010", by.y = "geo_id_blk10", all.x = T, all.y = F)
    
# FINAL CHANGES TO COL CLASS/TYPE ----
    bir_combined[, birthplace_county_wa_code := formatC(birthplace_county_wa_code, width=2, flag="0")] # make a two digit character
    bir_combined[, mother_residence_county_wa_code := formatC(mother_residence_county_wa_code, width=2, flag="0")] # make a two digit character
    
# DROP USELESS COLUMNS
    bir_combined[, blankblank := NULL]
    bir_combined[, res_geo_census_full_2010 := NULL]

#### ________________________________________________________----    
####                  PUSH TO SQL                            ----    
#### ________________________________________________________----    
    
#### ORDER COLUMNS IN R TO MATCH SQL ----
  # get list and order of colums from YAML file
    column.order <- c(names(table_config_stage_bir_wa$vars), names(table_config_stage_bir_wa$recodes))
  
  # keep only variales listed in YAML file
    bir_combined <- bir_combined[, ..column.order]
    
  # order the columns
    setcolorder(bir_combined, column.order)  
  
#### Ensure col type/class in R matches SQL ----
  r.table <- data.table(
    name = names(bir_combined), 
    r.class = tolower(sapply(bir_combined, class)))
  
    r.table[r.class %in% c("integer", "haven_labelled"), r.class := "numeric"]
    r.table[r.class == "factor", r.class := "character"]
    
  yaml.table <- data.table(
    name =  c(names(table_config_stage_bir_wa$vars), names(table_config_stage_bir_wa$recodes)), 
    yaml.class = c(as.character(table_config_stage_bir_wa$vars), as.character(table_config_stage_bir_wa$recodes)))
  
    yaml.table[, yaml.class := tolower(yaml.class)]
    yaml.table[yaml.class %like% "char", yaml.class := "character"]
    yaml.table[yaml.class == "integer", yaml.class := "numeric"]
    
  compare.classes <- merge(r.table, yaml.table, by = "name", all = TRUE)
  
  if(nrow(compare.classes[r.class != yaml.class ]) > 0)
    stop(View(compare.classes[r.class != yaml.class ]))
  
#### LOAD TO SQL ####
  tbl_id_2003_20xx <- DBI::Id(schema = table_config_stage_bir_wa$schema, 
                              table = table_config_stage_bir_wa$table)
  dbWriteTable(db_apde, 
               tbl_id_2003_20xx, 
               value = as.data.frame(bir_combined[]),
               overwrite = T, 
               append = F,
               field.types = c(unlist(table_config_stage_bir_wa$vars), unlist(table_config_stage_bir_wa$recodes)) )

#### THE END! ----
