## ---------------------------
##
## Script name: covid_essence_linkage
##
## Purpose of script: Link COVID cases with ESSENCE records
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2020-04-09
## Email: alastair.matheson@kingcounty.gov
##
## ---------------------------
##
## Notes:
##
## ---------------------------

#### SET OPTIONS AND BRING IN PACKAGES ####
options(scipen = 6, digits = 4, warning.length = 8170)


if (!require("pacman")) {install.packages("pacman")}
pacman::p_load(tidyverse, glue, data.table, openxlsx, lubridate, MMWRweek,
               jsonlite, keyring, httr, kableExtra, knitr, rmarkdown, odbc, DBI)

httr::set_config(httr::config(ssl_verifypeer = 0L))

# Connect to SQL data for chronic condition codes
db_claims <- DBI::dbConnect(odbc::odbc(), "PHClaims51") 

### Set up folder to work in
shared_path_old <- "//phshare01/cdi_share/Analytics and Informatics Team/Data Requests/2020/COVID RHINO match"
shared_path <- "//phshare01/CDI_SHARE/Outbreaks & Investigations/Outbreaks 2020/2019-nCoV/A&I Team/Syndromic/WDRS Match"


#### FUNCTIONS ####
# Call in from Github repo
source("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/essence/essence_query_functions.R")

# Build a function to assess chronic conditions
ccw_maker <- function(condition = c("ccw_alzheimer_related", "ccw_asthma",
                                    "ccw_atrial_fib", "ccw_chr_kidney_dis", "ccw_copd", 
                                    "ccw_diabetes", "ccw_heart_failure", "ccw_hyperlipid", 
                                    "ccw_hypertension", "ccw_ischemic_heart_dis", "ccw_mi", 
                                    "ccw_stroke"),
                      lookback = c("1", "2", "3"), 
                      dx_num = c("any", "first_two"), 
                      claim_type = c("1", "2", "3")) {
  
  condition_list <- match.arg(condition, several.ok = T)
  lookback <- match.arg(lookback)
  dx_num <- match.arg(dx_num)
  claim_type <- match.arg(claim_type)
  
  df <- chronic_cond %>% 
    # Limit to the conditions of interest
    filter(condition %in% condition_list) %>%
    # See if encounter is in the lookback period
    filter(lubridate::interval(Date, date_max) / ddays(1) < (365 * as.numeric(lookback)))
  
  # Keep only dx codes that are in the first two, if required
  if (dx_num == "first_two") {
    df <- df %>% filter(dx_num %in% c("dx_1", "dx_2"))
  }
  
  # Count up the number of inpatient/outpatient claims, if required
  if (claim_type == "2") {
    inpatient <- df %>%
      filter(HasBeenI == 1) %>%
      group_by(PID, condition) %>% summarise(claims_inpatient = n_distinct(Date)) %>% ungroup()
    
    outpatient <- df %>%
      filter(HasBeenI == 0) %>%
      group_by(PID, condition) %>% summarise(claims_outpatient = n_distinct(Date)) %>% ungroup() 
    
    has_condition <- full_join(inpatient, outpatient, by = c("PID", "condition")) %>%
      mutate(has_condition = case_when(
        claims_inpatient > 1 & !is.na(claims_inpatient) ~ 1L,
        claims_outpatient > 1 & !is.na(claims_outpatient) ~ 1L,
        TRUE ~ 0L)) %>%
      select(PID, condition, has_condition)
  } else if (claim_type == "3") {
    has_condition <- df %>%
      filter(HasBeenI == 1) %>%
      group_by(PID, condition) %>% summarise(claims_inpatient = n_distinct(Date)) %>% ungroup() %>%
      mutate(has_condition = case_when(claims_inpatient > 1 ~ 1L, TRUE ~ 0L)) %>%
      select(PID, condition, has_condition)
  } else {
    has_condition <- df %>%
      group_by(PID, condition) %>% summarise(claims = n_distinct(Date)) %>% ungroup() %>%
      mutate(has_condition = case_when(claims > 1 ~ 1L, TRUE ~ 0L)) %>%
      select(PID, condition, has_condition)
  }
  
  return(has_condition)
}


#### BRING IN REF DATA ####
# Bring in recode files
recodes <- read_csv(file.path(shared_path_old, "essence_recodes.csv"))

# Bring in CCW codes
ccw_dx <- DBI::dbGetQuery(db_claims, 
                          "SELECT dx, dx_ver, ccw_alzheimer_related, ccw_asthma,
                          ccw_atrial_fib, ccw_chr_kidney_dis, ccw_copd, ccw_diabetes,
                          ccw_heart_failure, ccw_hyperlipid, ccw_hypertension,
                          ccw_ischemic_heart_dis, ccw_mi, ccw_stroke, 
                          ccw_stroke_exclude1, ccw_stroke_exclude2
                          FROM ref.dx_lookup 
                          WHERE ccw_alzheimer_related = 1 OR ccw_asthma = 1 OR
                          ccw_atrial_fib = 1 OR ccw_chr_kidney_dis = 1 OR ccw_copd = 1 OR ccw_diabetes = 1 OR
                          ccw_heart_failure = 1 OR ccw_hyperlipid = 1 OR ccw_hypertension = 1 OR
                          ccw_ischemic_heart_dis = 1 OR ccw_mi = 1 OR ccw_stroke = 1 OR 
                          ccw_stroke_exclude1 = 1 OR ccw_stroke_exclude2 = 1")


#### BRING IN CASES TO MATCH ####
covid_orig_old <- read.xlsx(file.path(shared_path_old, "rhino_match.xlsx"))
covid_orig_old <- covid_orig_old %>% distinct() %>% # ~100 duplicates
  rename(case_id = CASE_ID, rhino_id = COL1) %>%
  mutate(case_id = as.integer(case_id)) %>%
  # Note how many RHINO events per case
  group_by(case_id) %>%
  mutate(encounters = n()) %>%
  ungroup()


covid_orig <- read.csv(file.path(shared_path, "kc_linkage.csv"), stringsAsFactors = F)
covid_orig <- covid_orig %>%
  mutate(date = format(as.Date(str_sub(RHINO_ID, 1, 10), format = "%Y.%m.%d"), "%Y-%m-%d"),
         pid = str_sub(RHINO_ID, str_locate(RHINO_ID, "\\.[0-9]{2}\\.[0-9]{2}\\.")[, 2] + 1, 
                       nchar(RHINO_ID)))

# Sometimes the original file has a random column in it
if ("X" %in% names(covid_orig)) {
  covid_orig <- covid_orig %>% select(-X)
}

### Check there is some overlap between the two files 
# Expecting ~1,400 encounters but may be less if the old file had non-KC residents
# who had encounters in KC
inner_join(covid_orig_old, covid_orig, by = c("rhino_id" = "RHINO_ID")) %>%
  summarise(match = n())



#### RETRIEVE DEMOGRAPHIC AND CHRONIC DISEASE DATA ####
case_demogs <- person_query(pid = covid_orig$pid, sdate = "2017-01-01",
                            dx = T, bulk = T, group_size = 200)


#### RECODE DEMOGRAPHIC DATA ####
case_demogs_recodes <- case_demogs %>%
  mutate_at(vars(Age, Height, Weight), list(~ as.numeric(.))) %>%
  # Fix up some formating so join works
  mutate(Ethnicity_flat = str_replace_all(Ethnicity_flat, ";", ""),
         dx = str_replace_all(DischargeDiagnosis, "\\.", ""),
         # Replace leading and final semicolon
         dx = str_replace(dx, ";", ""),
         dx = str_sub(dx, 1, nchar(dx) - 1)) %>%
  left_join(., filter(recodes, category == "ethnicity_flat") %>% select(code, value_display),
            by = c("Ethnicity_flat" = "code")) %>% rename(ethnicity_text = value_display) %>%
  left_join(., filter(recodes, category == "Smoking_Status_Code") %>% select(code, value_display),
            by = c("Smoking_Status_Code" = "code")) %>% rename(smoking_text = value_display) %>%
  mutate(Date = format(as.Date(Date, format = "%m/%d/%Y"), "%Y-%m-%d"),
         age_grp = case_when(Age < 18 ~ "<18",
                             between(Age, 18, 29) ~ "18-29",
                             between(Age, 30, 39) ~ "30-39",
                             between(Age, 40, 49) ~ "40-49",
                             between(Age, 50, 59) ~ "50-59",
                             between(Age, 60, 69) ~ "60-69",
                             between(Age, 70, 79) ~ "70-79",
                             Age >= 80 ~ "80+",
                             TRUE ~ "Unknown"),
         age_grp = factor(age_grp, levels = c("<18", "18-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80+", "Unknown")),
         Sex = case_when(Sex == "F" ~ "Female", Sex == "M" ~ "Male"),
         race_text = case_when(str_detect(Race_flat, "1002-5") ~ "AI/AN",
                               str_detect(Race_flat, "2028-9") ~ "Asian",
                               str_detect(Race_flat, "2054-5") ~ "Black",
                               str_detect(Race_flat, "2076-8") ~ "NH/PI",
                               str_detect(Race_flat, "2131-1") ~ "Other race",
                               str_detect(Race_flat, "2106-3") ~ "White"),
         height_m = case_when(tolower(Height_Units) == "centimeter" ~ Height / 100,
                              tolower(Height_Units) == "meter" ~ Height,
                              tolower(Height_Units) %in% c("inch", "inch [length]") ~ Height * 0.0254,
                              tolower(Height_Units) %in% c("foot", "foot [length]") ~ Height * 0.3048),
         weight_kg = case_when(tolower(Weight_Units) %in% c("kilogram", "kilogram [si mass units]") ~ Weight,
                               tolower(Weight_Units) == "pound" ~ Weight / 2.2,
                               tolower(Weight_Units) == "ounce" ~ Weight / 0.0283495),
         bmi = ifelse(!is.na(height_m) & !is.na(weight_kg) & Age >= 20,
                      round(weight_kg / height_m ^ 2, 3), NA),
         overweight = case_when(bmi >= 25 ~ 1L, bmi < 25 ~ 0L),
         obese = case_when(bmi >= 30 ~ 1L, bmi < 30 ~ 0L),
         obese_severe = case_when(bmi >= 40 ~ 1L, bmi < 40 ~ 0L),
         smoker_current = case_when(smoking_text %in% c("Current every day smoker",
                                                        "Current light tobacco smoker",
                                                        "Current some day smoker") ~ 1L,
                                    smoking_text %in% c("Never smoker", "Former smoker") ~ 0L)) %>%
  group_by(PID) %>%
  mutate(date_max = as.Date(max(Date, na.rm = T))) %>%
  ungroup()


#### LOOK FOR CHRONIC CONDITIONS - NON STROKE ####
# Get a list of all chronic conditions to loop over
ccw_list <- ccw_dx %>% select(starts_with("ccw")) %>% names()
# Loop over and see who has them
chronic_cond <- bind_rows(lapply(ccw_list, function(x) {
  
  # Get relevant DX codes for this CCW condition
  ccw <- ccw_dx %>% filter(!!rlang::sym(x) == 1)
  
  # Find out how many dx columns are needed
  df <- case_demogs_recodes %>% mutate(semi_cnt = str_count(dx, ";"))
  new_col <- max(df$semi_cnt, na.rm = T) + 1 # Add 1 to get correct number
  
  # Just keep relevant columns and split out all the dx codes
  df <- df %>% select(PID, Date, HasBeenE, HasBeenI, dx) %>%
    separate(dx, sep = ";", into = paste0("dx_", 1:new_col), fill = "right", remove = T) %>%
    reshape2::melt(id.vars = c("PID", "Date", "HasBeenE", "HasBeenI"), na.rm = T,
                   variable.name = "dx_num", value.name = "dx") %>% 
    # See which DX codes match the condition in question
    mutate(condition = x, match = dx %in% ccw$dx) %>%
    # Only keep the matches
    filter(match == T) %>%
    arrange(PID, Date, dx_num) %>%
    # Get the max date for the purposes of looking back
    left_join(., distinct(case_demogs_recodes, PID, date_max), by = "PID")
  
  df
}))


### Run function to find people who met the criteria
chronic_cond_1_any_2 <- ccw_maker(condition = c("ccw_asthma", "ccw_copd", 
                                                "ccw_hyperlipid", "ccw_hypertension"), 
                            lookback = "1", dx_num = "any", claim_type = "2")

chronic_cond_1_first2_2 <- ccw_maker(condition = c("ccw_atrial_fib"), 
                            lookback = "1", dx_num = "first_two", claim_type = "2")

chronic_cond_1_first2_3 <- ccw_maker(condition = c("ccw_mi"), 
                                     lookback = "1", dx_num = "first_two", claim_type = "3")

chronic_cond_2_any_2 <- ccw_maker(condition = c("ccw_chr_kidney_dis", "ccw_diabetes", 
                                                "ccw_heart_failure", "ccw_ischemic_heart_dis"), 
                                  lookback = "2", dx_num = "any", claim_type = "2")

chronic_cond_3_any_2 <- ccw_maker(condition = c("ccw_alzheimer_related"), 
                                  lookback = "2", dx_num = "any", claim_type = "1")


#### LOOK FOR CHRONIC CONDITIONS - STROKE ####
# Stroke is a more complicated situation
# 1-year lookback, 1 inpatient or 2 outpatient
# Any dx for inclusion dx and one set of exlcusion dx
# Primary dx for other set of exclusion dx
chronic_stroke_setup <- chronic_cond %>% 
  filter(condition %in% c("ccw_stroke", "ccw_stroke_exclude1", "ccw_stroke_exclude2")) %>%
  filter(lubridate::interval(Date, date_max) / ddays(1) < 365) %>%
  filter((condition == "ccw_stroke_exclude2" & dx_num == "dx_1") |
           condition != "ccw_stroke_exclude2")

chronic_stroke_inpatient <- chronic_stroke_setup %>%
  filter(HasBeenI == 1) %>%
  group_by(PID, Date, condition) %>% summarise(claims_inpatient = n()) %>% ungroup() %>%
  spread(key = condition, value = claims_inpatient)
# When this was first run, no encounters had any valid ccw_stroke_exclude2 fields so 
# the column didn't exist when moving from long to wide
if ("ccw_stroke_exclude2" %in% names(chronic_stroke_inpatient)) {
  chronic_stroke_inpatient <- chronic_stroke_inpatient %>%
    mutate_at(vars(ccw_stroke, ccw_stroke_exclude1, ccw_stroke_exclude2), list(~ replace_na(., 0)))
} else {
  chronic_stroke_inpatient <- chronic_stroke_inpatient %>%
    mutate_at(vars(ccw_stroke, ccw_stroke_exclude1), list(~ replace_na(., 0))) %>%
    mutate(ccw_stroke_exclude2 = 0L)
}
chronic_stroke_inpatient <- chronic_stroke_inpatient %>%
  mutate(has_condition_in = case_when(
    ccw_stroke >= 1 & ccw_stroke_exclude1 == 0 & ccw_stroke_exclude2 == 0 ~ 1L,
    TRUE ~ 0L
  ))

chronic_stroke_outpatient <- chronic_stroke_setup %>%
  filter(HasBeenI == 0) %>%
  group_by(PID, Date, condition) %>% summarise(claims_inpatient = n()) %>% ungroup() %>%
  spread(key = condition, value = claims_inpatient)
# When this was first run, no encounters had any valid ccw_stroke_exclude2 fields so 
# the column didn't exist when moving from long to wide
if ("ccw_stroke_exclude2" %in% names(chronic_stroke_outpatient)) {
  chronic_stroke_outpatient <- chronic_stroke_outpatient %>%
    mutate_at(vars(ccw_stroke, ccw_stroke_exclude1, ccw_stroke_exclude2), list(~ replace_na(., 0)))
} else {
  chronic_stroke_outpatient <- chronic_stroke_outpatient %>%
    mutate_at(vars(ccw_stroke, ccw_stroke_exclude1), list(~ replace_na(., 0))) %>%
    mutate(ccw_stroke_exclude2 = 0L)
}
chronic_stroke_outpatient <- chronic_stroke_outpatient %>%
  mutate(has_condition_out = case_when(
    ccw_stroke >= 2 & ccw_stroke_exclude1 == 0 & ccw_stroke_exclude2 == 0 ~ 1L
  ))


chronic_stroke <- full_join(chronic_stroke_inpatient, chronic_stroke_outpatient, 
                            by = c("PID", "Date")) %>%
  mutate(condition = "ccw_stroke",
         has_condition = case_when(
           has_condition_in == 1 & !is.na(has_condition_in) ~ 1L,
           has_condition_out == 1 & !is.na(has_condition_out) ~ 1L,
           TRUE ~ 0L)) %>%
  group_by(PID, condition) %>% 
  summarise(has_condition = max(has_condition, na.rm = T)) %>%
  ungroup() %>%
  select(PID, condition, has_condition)

rm(chronic_stroke_inpatient, chronic_stroke_outpatient, chronic_stroke_setup)


#### COMBINE ALL CHRONIC INFORMATION ####
chronic_combine <- bind_rows(chronic_cond_1_any_2, chronic_cond_1_first2_2,
                             chronic_cond_1_first2_3, chronic_cond_2_any_2,
                             chronic_cond_3_any_2, chronic_stroke)

### Make a matrix of all conditions and PIDs
chronic <- expand.grid(PID = unique(case_demogs$PID), 
                       condition = ccw_list[!ccw_list %in% c("ccw_stroke_exclude1", "ccw_stroke_exclude2")],
                       stringsAsFactors = F)

# Join to data and reshape
chronic <- left_join(chronic, chronic_combine, by = c("PID", "condition")) %>%
  mutate(has_condition = replace_na(has_condition, 0)) %>%
  spread(key = condition, value = has_condition)


#### BRING ALL DEMOGRAPHICS TOGETHER ####
case_demogs_chronic <- left_join(case_demogs_recodes, chronic, by = "PID") %>%
  select(PID, Age, age_grp, Sex, race_text, ethnicity_text, starts_with("ccw_")) %>%
  distinct()



#### RETRIEVE CLINICAL DATA ####
case_data <- bind_rows(event_query(event_id = covid_orig$RHINO_ID, bulk = T))


#### RECODE CLINICAL DATA ####
case_data_recodes <- left_join(covid_orig, case_data, by = c("RHINO_ID" = "rhino_id")) %>%
  mutate_at(vars(Age, Height, Weight, Initial_Pulse_Oximetry_Calc, HasBeenE, HasBeenI,
                 Systolic_Blood_Pressure, Diastolic_Blood_Pressure, 
                 MinutesFromVisitToDischarge), 
            list(~ as.numeric(.))) %>%
  # Only keep matched
  filter(!is.na(PID)) %>%
  # Fix up some formating so join works
  mutate(Ethnicity_flat = str_replace_all(Ethnicity_flat, ";", "")) %>%
  left_join(., filter(recodes, category == "ethnicity_flat") %>% select(code, value_display),
            by = c("Ethnicity_flat" = "code")) %>% rename(ethnicity_text = value_display) %>%
  left_join(., filter(recodes, category == "Smoking_Status_Code") %>% select(code, value_display),
            by = c("Smoking_Status_Code" = "code")) %>% rename(smoking_text = value_display) %>%
  left_join(., filter(recodes, category == "DischargeDisposition") %>% select(code, value_display),
            by = c("DischargeDisposition" = "code")) %>% rename(discharge_text = value_display) %>%
  mutate(Date = as.Date(Date, format = "%m/%d/%Y"),
         age_grp = case_when(Age < 18 ~ "<18",
                             between(Age, 18, 29) ~ "18-29",
                             between(Age, 30, 39) ~ "30-39",
                             between(Age, 40, 49) ~ "40-49",
                             between(Age, 50, 59) ~ "50-59",
                             between(Age, 60, 69) ~ "60-69",
                             between(Age, 70, 79) ~ "70-79",
                             Age >= 80 ~ "80+",
                             TRUE ~ "Unknown"),
         age_grp = factor(age_grp, levels = c("<18", "18-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80+", "Unknown")),
         Sex = case_when(Sex == "F" ~ "Female", Sex == "M" ~ "Male"),
         race_text = case_when(str_detect(Race_flat, "1002-5") ~ "AI/AN",
                               str_detect(Race_flat, "2028-9") ~ "Asian",
                               str_detect(Race_flat, "2054-5") ~ "Black",
                               str_detect(Race_flat, "2076-8") ~ "NH/PI",
                               str_detect(Race_flat, "2131-1") ~ "Other race",
                               str_detect(Race_flat, "2106-3") ~ "White"),
         height_m = case_when(tolower(Height_Units) == "centimeter" ~ Height / 100,
                              tolower(Height_Units) == "meter" ~ Height,
                              tolower(Height_Units) %in% c("inch", "inch [length]") ~ Height * 0.0254,
                              tolower(Height_Units) %in% c("foot", "foot [length]") ~ Height * 0.3048),
         weight_kg = case_when(tolower(Weight_Units) %in% c("kilogram", "kilogram [si mass units]") ~ Weight,
                               tolower(Weight_Units) == "pound" ~ Weight / 2.2,
                               tolower(Weight_Units) == "ounce" ~ Weight / 0.0283495),
         bmi = ifelse(!is.na(height_m) & !is.na(weight_kg) & Age >= 20,
                      round(weight_kg / height_m ^ 2, 3), NA),
         overweight = case_when(bmi >= 25 ~ 1L, bmi < 25 ~ 0L),
         obese = case_when(bmi >= 30 ~ 1L, bmi < 30 ~ 0L),
         obese_severe = case_when(bmi >= 40 ~ 1L, bmi < 40 ~ 0L),
         smoker_current = case_when(smoking_text %in% c("Current every day smoker",
                                                        "Current light tobacco smoker",
                                                        "Current some day smoker") ~ 1L,
                                    smoking_text %in% c("Never smoker", "Former smoker") ~ 0L),
         # No-one has pregnancy status recorded at the moment
         setting = case_when(str_detect(tolower(Facility_Type_Description), "emergency care") ~ "ED",
                             str_detect(tolower(Facility_Type_Description), "inpatient") ~ "Inpatient",
                             str_detect(tolower(Facility_Type_Description), "urgent") ~ "Urgent care",
                             str_detect(tolower(Facility_Type_Description), "primary care") ~ "Primary care",
                             str_detect(tolower(Facility_Type_Description), "medical specialty") ~ "Medical specialty"),
         ventilated = ifelse(str_detect(tolower(Procedure_Combo), "ventilat"), 1L, 0L),
         pneumonia = ifelse(str_detect(tolower(Diagnosis_Combo), "pneumonia") & 
                              str_detect(tolower(Diagnosis_Combo), "pneumoniae", negate = T), 1L, 0L),
         hypoxic = case_when(between(Initial_Pulse_Oximetry_Calc, 0, 89.99) ~ 1L,
                             Initial_Pulse_Oximetry_Calc >= 90 ~ 0L),
         time_discharge = ifelse(is.na(MinutesFromVisitToDischarge), NA_integer_,
                                 as.integer(floor(MinutesFromVisitToDischarge / 60 / 24))),
         time_discharge_cat = case_when(
           time_discharge == 0 ~ "< 1 day",
           between(time_discharge, 1, 3) ~ "1-3 days",
           between(time_discharge, 4, 6) ~ "4-6 days",
           time_discharge >= 7 ~ "7+ days")
         ) %>%
  # Join to chronic conditions
  left_join(., chronic, by = "PID")



#### SET UP REPORTS ####
# See how many matched overall
matched <- case_data_recodes %>% summarise(encounters = n(), people = n_distinct(CASE_ID))

### Set up the first and most recent clinical encounter per person
first <- case_data_recodes %>%
  arrange(CASE_ID, Date) %>%
  group_by(CASE_ID) %>%
  slice(1) %>%
  ungroup()

last <- case_data_recodes %>%
  arrange(CASE_ID, desc(Date)) %>%
  group_by(CASE_ID) %>%
  slice(1) %>%
  ungroup()


#### Demographics ####
# For demographics, take the earliest record per case
# Rationale: being a case may have changed BMI or smoking status by later encounters.

# Run each demographic
age <- first %>% filter(!is.na(age_grp)) %>% group_by(age_grp) %>% 
  summarise(count = n()) %>% ungroup() %>% 
  mutate(total = sum(count),
         pct = round(count / total * 100, 1)) %>%
  mutate(Demographic = paste0("Age (n = ", format(max(total), big.mark = ','), ")")) %>%
  rename(Group = age_grp)

race <- first %>% filter(!is.na(race_text)) %>% group_by(race_text) %>% 
  summarise(count = n()) %>% ungroup() %>% 
  mutate(total = sum(count),
         pct = round(count / total * 100, 1)) %>%
  mutate(Demographic = paste0("Race (n = ", format(max(total), big.mark = ','), ")")) %>%
  rename(Group = race_text)

ethn <- first %>% filter(!is.na(ethnicity_text)) %>% group_by(ethnicity_text) %>% 
  summarise(count = n()) %>% ungroup() %>% 
  mutate(total = sum(count),
         pct = round(count / total * 100, 1)) %>%
  mutate(Demographic = paste0("Ethnicity (n = ", format(max(total), big.mark = ','), ")")) %>%
  rename(Group = ethnicity_text)

sex <- first %>% filter(!is.na(Sex)) %>% group_by(Sex) %>% 
  summarise(count = n()) %>% ungroup() %>% 
  mutate(total = sum(count),
         pct = round(count / total * 100, 1)) %>%
  mutate(Demographic = paste0("Gender (n = ", format(max(total), big.mark = ','), ")")) %>%
  rename(Group = Sex)

obese <- first %>% filter(!is.na(obese)) %>% group_by(obese) %>% 
  summarise(count = n()) %>% ungroup() %>% 
  mutate(total = sum(count),
         pct = round(count / total * 100, 1)) %>%
  filter(obese == 1) %>%
  mutate(Demographic = paste0("Obesity (n = ", format(max(total), big.mark = ','), ")"), 
         Group = "Obese (BMI 30+)") %>%
  select(-obese)

obese_severe <- first %>% filter(!is.na(obese_severe)) %>% group_by(obese_severe) %>% 
  summarise(count = n()) %>% ungroup() %>% 
  mutate(total = sum(count),
         pct = round(count / total * 100, 1)) %>%
  filter(obese_severe == 1) %>%
  mutate(Demographic = paste0("Obesity (n = ", format(max(total), big.mark = ','), ")"),
         Group = "Severely obese (BMI 40+)") %>%
  select(-obese_severe)

smoker_current <- first %>% filter(!is.na(smoker_current)) %>% group_by(smoker_current) %>% 
  summarise(count = n()) %>% ungroup() %>% 
  mutate(total = sum(count),
         pct = round(count / total * 100, 1)) %>%
  mutate(Demographic = paste0("Smoking status (n = ", format(max(total), big.mark = ','), ")"), 
         Group = case_when(smoker_current == 1 ~ "Current smoker", TRUE ~ "Former/never smoker")) %>%
  select(-smoker_current)

chronic_report <- first %>% select(PID, starts_with("ccw")) %>% distinct() %>%
  pivot_longer(cols = starts_with("ccw"), names_to = "condition", values_to = "has_condition") %>%
  group_by(condition) %>%
  summarise(count = sum(has_condition), total = n()) %>% ungroup() %>% 
  mutate(pct = round(count / total * 100, 1),
         Demographic = paste0("Chronic condition (n = ", format(max(total), big.mark = ','), ")"), 
         Group = case_when(condition == "ccw_alzheimer_related" ~ "Alzheimer's disease", 
                           condition == "ccw_asthma" ~ "Asthma",
                           condition == "ccw_atrial_fib" ~ "Atrial fibrillation",
                           condition == "ccw_chr_kidney_dis" ~ "Chronic kidney disease",
                           condition == "ccw_copd" ~ "Chronic obstructive pulmonary disease",
                           condition == "ccw_diabetes" ~ "Diabetes",
                           condition == "ccw_heart_failure" ~ "Heart failure",
                           condition == "ccw_hyperlipid" ~ "Hyperlipidemia",
                           condition == "ccw_hypertension" ~ "Hypertension",
                           condition == "ccw_ischemic_heart_dis" ~ "Ischemic heart disease",
                           condition == "ccw_mi" ~ "Acute myocardial infarction",
                           condition == "ccw_stroke" ~ "Stroke",
                           TRUE ~ NA_character_)) %>%
  arrange(Group)



# Combine into one data frame
demogs_tot <- bind_rows(age, race, ethn, sex, obese, obese_severe, smoker_current, chronic_report) %>%
  mutate(Number = paste0(format(count, big.mark = ','), " (", pct, "%)")) %>%
  select(Demographic, Group, Number)


#### Clinical features ####
stay_length <- case_data_recodes %>% filter(!is.na(time_discharge_cat) & HasBeenI == 1) %>% 
  group_by(time_discharge_cat) %>% 
  summarise(count = n()) %>% ungroup() %>% 
  mutate(total = sum(count),
         pct = round(count / total * 100, 1)) %>%
  mutate(Event = paste0("Length of stay (n = ", format(max(total), big.mark = ','), ")")) %>%
  rename(Group = time_discharge_cat)

pneumonia <- case_data_recodes %>% filter(!is.na(pneumonia)) %>% 
  group_by(pneumonia) %>% 
  summarise(count = n()) %>% ungroup() %>% 
  mutate(total = sum(count),
         pct = round(count / total * 100, 1)) %>%
  filter(pneumonia == 1) %>%
  mutate(Event = paste0("Pneumonia (n = ", format(max(total), big.mark = ','), ")"),
         Group = "") %>%
  select(-pneumonia)

hypoxic <- case_data_recodes %>% filter(!is.na(hypoxic)) %>% 
  group_by(hypoxic) %>% 
  summarise(count = n()) %>% ungroup() %>% 
  mutate(total = sum(count),
         pct = round(count / total * 100, 1)) %>%
  filter(hypoxic == 1) %>%
  mutate(Event = paste0("Hypoxic (PO < 90%) (n = ", format(max(total), big.mark = ','), ")"),
         Group = "") %>%
  select(-hypoxic)

ventilate <- case_data_recodes %>% filter(!is.na(ventilated)) %>% 
  group_by(ventilated) %>% 
  summarise(count = n()) %>% ungroup() %>% 
  mutate(total = sum(count),
         pct = round(count / total * 100, 1)) %>%
  filter(ventilated == 1) %>%
  mutate(Event = paste0("Ventilated (n = ", format(max(total), big.mark = ','), ")"),
         Group = "") %>%
  select(-ventilated)


died <- last %>% filter(!is.na(C_Death)) %>% group_by(C_Death) %>% 
  summarise(count = n()) %>% ungroup() %>% 
  mutate(total = sum(count),
         pct = round(count / total * 100, 1)) %>%
  filter(C_Death == "Yes") %>%
  mutate(Event = paste0("Died (n = ", format(max(total), big.mark = ','), ")"),
         Group = "") %>%
  select(-C_Death)


# Combine into one data frame
clinic_tot <- bind_rows(stay_length, pneumonia, hypoxic, ventilate, died) %>%
  mutate(Number = paste0(format(count, big.mark = ','), " (", pct, "%)")) %>%
  select(Event, Group, Number)



#### WRITE OUT DATA ####
# Want to pull in Rmd file from Github, using a clunky approach found here: https://gist.github.com/mages/3968939
# plus a function to temporarily create a local Rmd file (https://github.com/rstudio/rmarkdown/issues/110)
rmd_data <- paste(readLines(textConnection(RCurl::getURL(
  "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/essence/covid_essence_linkage.Rmd"))), collapse = "\n")

render_text = function(text, ...) {
  f = file.path(Sys.getenv('R_USER'), "covid_essence_linkage.Rmd")
  on.exit(unlink(f), add = TRUE)
  writeLines(text, f)
  o = rmarkdown::render(f, ...)
}

render_text(rmd_data, output_file = file.path(shared_path, "covid_essence_linkage.html"))
