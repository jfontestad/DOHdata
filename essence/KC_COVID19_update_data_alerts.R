# ESSENCE and NRVESS pulls are automated here. 
# NRVESS is only updated Fridays - you can use lubridate::wday(today(), label = TRUE) == "Fri" to check day if needed.
# generation of plots should be doable here too, depending on needs of plots. 
# question is, should this script be loaded as an environment for the app every time? There must be a smart way of doing that. It can be run in the middle of the night every night
# or whatever, then be loaded into the environment, then the load balancing can take place to handle whatever the needs are there.

# IMPORTANT ###############################################################################################################
#BEFORE RUNNING THE SCRIPT, SAVE YOUR ESSENCE CREDENTIALS TO THE WINDOW CREDENTIAL MANAGER - this prevents the need from typing them into the code below
# 1. Search for Credential Manager on your computer
# 2. Click "Windows Credentials"
# 3. Click "Add a generic credential"
# 4. For Internet or network address, type: essence
# 5. For user name, type: yourESSENCEusername
# 6. For Password, type: yourESSENCEpw
# 7. If you update your ESSENCE password, you will need to come into the credential manager and update it here as well
#######################################################################################################################

#install.packages("janitor")
#install.packages("MMWRweek")
#install.packages("lubridate")
#install.packages("tidyverse")
#install.packages("knitr")
#install.packages("httr")
#install.packages("jsonlite")
#install.packages("keyring")


library(janitor)
library(MMWRweek)
library(lubridate)
library(tidyverse)
library(knitr)
library(httr)
httr::set_config(config(ssl_verifypeer = 0L))
library(jsonlite)
library(keyring)




# date objects for ESSENCE queries and NRVESS data pull
### SET WHETHER THIS IS A HISTORICAL RUN OR NOT
historical <- F

if (historical == F) {
  s_start_date <- as_date("2019-09-29", "%Y-%m-%d")
} else {
  s_start_date <- as_date("2017-08-01", "%Y-%m-%d") 
}

s_end_date <- today() - 1

### Set up folder to work in
output_path <- "//phshare01/cdi_share/Analytics and Informatics Team/Data Requests/2020/372_nCoV Essence Extract/From Natasha on March 13"


#### FUNCTIONS ####
# Call in from Github repo
source("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/essence/essence_query_functions.R")


#### DAILY RUN ####
#### PERSON-LEVEL DATA ####
if (historical == F) {
  message("Running person-level section")
  # Daily pneumonia person-level data - ED visits
  pdly_full_pneumo_ed <- syndrome_person_level_query(frequency = "daily", syndrome = "pneumonia", ed = T,
                                                     sdate = s_start_date, edate = s_end_date)
  write_csv(pdly_full_pneumo_ed, file.path(output_path, "pdly-pneumonia.csv"))
  
  # Daily Pneumonia person-level data - hospitalizations
  pdly_full_pneumo_hosp <- syndrome_person_level_query(frequency = "daily", syndrome = "pneumonia", inpatient = T,
                                                       sdate = s_start_date, edate = s_end_date)
  write_csv(pdly_full_pneumo_hosp, file.path(output_path, "pdly-pneumonia-hosp.csv"))
  
  # Daily ILI person-level data - ED visits
  pdly_full_ili_ed <- syndrome_person_level_query(frequency = "daily", syndrome = "ili", ed = T,
                                                  sdate = s_start_date, edate = s_end_date)
  write_csv(pdly_full_ili_ed, file.path(output_path, "pdly-ILI.csv"))
  
  # Daily ILI person-level data - hospitalalizations
  pdly_full_ili_hosp <- syndrome_person_level_query(frequency = "daily", syndrome = "ili", inpatient = T,
                                                    sdate = s_start_date, edate = s_end_date)
  write_csv(pdly_full_ili_hosp, file.path(output_path, "pdly-ILI-hosp.csv"))
  
  # Daily CLI person-level data - ED Visits 
  pdly_full_cli_ed <- syndrome_person_level_query(frequency = "daily", syndrome = "cli", ed = T,
                                                  sdate = s_start_date, edate = s_end_date)
  write_csv(pdly_full_cli_ed, file.path(output_path, "pdly-CLI.csv"))
  
  # Daily CLI person-level data - hospitalalizations
  pdly_full_cli_hosp <- syndrome_person_level_query(frequency = "daily", syndrome = "cli", inpatient = T,
                                                    sdate = s_start_date, edate = s_end_date)
  write_csv(pdly_full_cli_hosp, file.path(output_path, "pdly-CLI-hosp.csv"))
}



#### DAILY - ALL AGE ####
message("Running daily all-ages section")

all_age_ed_daily <- bind_rows(lapply(c("pneumonia", "ili", "cli", "influenza"), function(x) {
  # Run both queries
  pct <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, hospital = F, 
                              value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, hospital = F, 
                              value = "count", sdate = s_start_date, edate = s_end_date)
  # Bring together
  output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
  return(output)
}))

all_age_ed_uc_daily <- bind_rows(lapply(c("pneumonia", "ili", "cli"), function(x) {
  # Run both queries
  pct <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, hospital = F, 
                              value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, hospital = F, 
                              value = "count", sdate = s_start_date, edate = s_end_date)
  # Bring together
  output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
  return(output)
}))

all_age_hosp_daily <- bind_rows(lapply(c("pneumonia", "ili", "cli", "influenza"), function(x) {
  # Run both queries
  pct <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, hospital = F, 
                              value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, hospital = F, 
                              value = "count", sdate = s_start_date, edate = s_end_date)
  # Bring together
  output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
  return(output)
}))


#### DAILY - BY HOSPITAL ####
all_age_ed_byhosp_daily <- bind_rows(lapply(c("cli"), function(x) {
  # Run both queries
  pct <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, hospital = T, 
                              value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, hospital = T, 
                              value = "count", sdate = s_start_date, edate = s_end_date)
  # Bring together
  output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
  return(output)
}))


#### DAILY - AGE SPECIFIC ####
message("Running daily age-specific section")

### ED visits
age_specific_ed_daily <- bind_rows(lapply(c("pneumonia", "ili", "cli"), function(x) {
  # Run queries
  pct04 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, age = "00-04", hospital = F, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt04 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, age = "00-04", hospital = F, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
  pct517 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, age = "05-17", hospital = F, 
                                 value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt517 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, age = "05-17", hospital = F, 
                                 value = "count", sdate = s_start_date, edate = s_end_date)
  pct1844 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, age = "18-44", hospital = F, 
                                  value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt1844 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, age = "18-44", hospital = F, 
                                  value = "count", sdate = s_start_date, edate = s_end_date)
  pct4564 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, age = "45-64", hospital = F, 
                                  value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt4564 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, age = "45-64", hospital = F, 
                                  value = "count", sdate = s_start_date, edate = s_end_date)
  pct65 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, age = "65-1000", hospital = F, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt65 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, age = "65-1000", hospital = F, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
  pctunk <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, age = "unknown", hospital = F, 
                                 value = "percent", sdate = s_start_date, edate = s_end_date)
  cntunk <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, age = "unknown", hospital = F, 
                                 value = "count", sdate = s_start_date, edate = s_end_date)
  # Bring together
  output <- bind_rows(left_join(pct04, cnt04, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pct517, cnt517, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pct1844, cnt1844, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pct4564, cnt4564, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pct65, cnt65, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pctunk, cntunk, by = c("date", "age", "setting", "query", "syndrome", "hospital")))
  return(output)
}))


### ED and urgent care
age_specific_ed_uc_daily <- bind_rows(lapply(c("pneumonia", "ili", "cli"), function(x) {
  # Run queries
  pct04 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "00-04", hospital = F, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt04 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "00-04", hospital = F, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
  pct517 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "05-17", hospital = F, 
                                 value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt517 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "05-17", hospital = F, 
                                 value = "count", sdate = s_start_date, edate = s_end_date)
  pct1844 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "18-44", hospital = F, 
                                  value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt1844 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "18-44", hospital = F, 
                                  value = "count", sdate = s_start_date, edate = s_end_date)
  pct4564 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "45-64", hospital = F, 
                                  value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt4564 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "45-64", hospital = F, 
                                  value = "count", sdate = s_start_date, edate = s_end_date)
  pct65 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "65-1000", hospital = F, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt65 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "65-1000", hospital = F, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
  pctunk <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "unknown", hospital = F, 
                                 value = "percent", sdate = s_start_date, edate = s_end_date)
  cntunk <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "unknown", hospital = F, 
                                 value = "count", sdate = s_start_date, edate = s_end_date)
  # Bring together
  output <- bind_rows(left_join(pct04, cnt04, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pct517, cnt517, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pct1844, cnt1844, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pct4564, cnt4564, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pct65, cnt65, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pctunk, cntunk, by = c("date", "age", "setting", "query", "syndrome", "hospital")))
  return(output)
}))

### Hospitalization
age_specific_hosp_daily <- bind_rows(lapply(c("pneumonia", "ili", "cli"), function(x) {
  # Run queries
  pct04 <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "00-04", hospital = F, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt04 <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "00-04", hospital = F, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
  pct517 <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "05-17", hospital = F, 
                                 value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt517 <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "05-17", hospital = F, 
                                 value = "count", sdate = s_start_date, edate = s_end_date)
  pct1844 <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "18-44", hospital = F, 
                                  value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt1844 <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "18-44", hospital = F, 
                                  value = "count", sdate = s_start_date, edate = s_end_date)
  pct4564 <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "45-64", hospital = F, 
                                  value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt4564 <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "45-64", hospital = F, 
                                  value = "count", sdate = s_start_date, edate = s_end_date)
  pct65 <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "65-1000", hospital = F, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt65 <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "65-1000", hospital = F, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
  pctunk <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "unknown", hospital = F, 
                                 value = "percent", sdate = s_start_date, edate = s_end_date)
  cntunk <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "unknown", hospital = F, 
                                 value = "count", sdate = s_start_date, edate = s_end_date)
  # Bring together
  output <- bind_rows(left_join(pct04, cnt04, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pct517, cnt517, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pct1844, cnt1844, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pct4564, cnt4564, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pct65, cnt65, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                      left_join(pctunk, cntunk, by = c("date", "age", "setting", "query", "syndrome", "hospital")))
  return(output)
}))


#### DAILY - MAKE AND SAVE COMBNIED DATASET ####
message("Bringing together and saving daily data")

### Combine data
ndly <- bind_rows(
  # DAILY ALL AGE
  all_age_ed_daily, all_age_ed_uc_daily, all_age_hosp_daily,
  # BY HOSPITAL
  all_age_ed_byhosp_daily,
  # AGE SPECIFIC
  age_specific_ed_daily, age_specific_ed_uc_daily, age_specific_hosp_daily)

### Pull out dates and convert to get weeks
ndly_date <- sort(unique(ndly$date))
ndly_date <- cbind(ndly_date, MMWRweek(ndly_date))

### Finish making new variables
ndly <- left_join(ndly, ndly_date, by = c("date" = "ndly_date")) %>%
  rename(year = MMWRyear, week = MMWRweek, day = MMWRday) %>%
  mutate(frequency = "daily",
         season = case_when(
           year == 2017 & week >= 40 ~ "2017-2018",
           year == 2018 & week < 40 ~ "2017-2018",
           year == 2018 & week >= 40 ~ "2018-2019",
           year == 2019 & week < 40 ~ "2018-2019",
           year == 2019 & week >= 40 ~ "2019-2020",
           year == 2020 & week < 40 ~ "2019-2020"),
         week = as.factor(week),
         week = factor(week, levels(week)[c(40:52, 1:39)], ordered = TRUE),
         essence_refresh_date = today()) %>%
  filter(!is.na(season)) %>%
  mutate_at(vars(pct, expected_pct, levels_pct, colorID_pct,
                 cnt, expected_cnt, levels_cnt, colorID_cnt),
            list(~ as.numeric(.)))


### Combine with historical data
if (historical == F) {
  dly <- readRDS(file.path(output_path, "dly.RData"))
  dly <- dly %>% filter(date < s_start_date)
  
  ndly <- bind_rows(dly, ndly)
}


### Set up sort order for Tableau
ndly <- ndly %>%
  arrange(frequency, date, query, season, setting, age, hospital) %>%
  group_by(frequency, query, season, setting, age, hospital) %>%
  mutate(date_sort = row_number()) %>%
  ungroup()


### Write out data
if (historical == F) {
  saveRDS(ndly, file = file.path(output_path, "ndly.RData"))
  write_csv(ndly, file.path(output_path, "ndly.csv"))
} else {
  saveRDS(ndly, file = file.path(output_path, "dly.RData"))
}


#### DAILY - CLEAN UP ####
rm(pdly_full_pneumo_ed, pdly_full_pneumo_hosp, pdly_full_ili_ed, pdly_full_cli_ed, pdly_full_cli_hosp)

rm(dly,
   all_age_ed_daily, all_age_ed_byhosp_daily, all_age_ed_uc_daily, all_age_hosp_daily,
   age_specific_ed_daily, age_specific_ed_uc_daily, age_specific_hosp_daily)


#### WEEKLY RUN ####
### ONLY RUN THIS IF IT'S A MONDAY
if (wday(today(), label = F, week_start = getOption("lubridate.week.start", 1)) == 1) {
  message("It's Monday, time to run the weekly counts")
  
  #### WEEKLY - ALL AGE ####
  message("Running weekly all-ages section")
  
  all_age_ed_weekly <- bind_rows(lapply(c("pneumonia", "ili", "cli", "influenza"), function(x) {
    # Run both queries
    pct <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, hospital = F, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, hospital = F, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
    # Bring together
    output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
    return(output)
  }))
  
  all_age_ed_uc_weekly <- bind_rows(lapply(c("pneumonia", "ili", "cli"), function(x) {
    # Run both queries
    pct <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, hospital = F, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, hospital = F, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
    # Bring together
    output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
    return(output)
  }))
  
  all_age_hosp_weekly <- bind_rows(lapply(c("pneumonia", "ili", "cli", "influenza"), function(x) {
    # Run both queries
    pct <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, hospital = F, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, hospital = F, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
    # Bring together
    output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
    return(output)
  }))
  
  
  #### WEEKLY - BY HOSPITAL ####
  all_age_ed_byhosp_weekly <- bind_rows(lapply(c("cli"), function(x) {
    # Run both queries
    pct <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, hospital = T, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, hospital = T, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
    # Bring together
    output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
    return(output)
  }))
  
  
  #### WEEKLY - AGE SPECIFIC ####
  message("Running weekly age-specific section")
  
  ### ED visits
  age_specific_ed_weekly <- bind_rows(lapply(c("pneumonia", "ili", "cli"), function(x) {
    # Run queries
    pct04 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, age = "00-04", hospital = F, 
                                  value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt04 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, age = "00-04", hospital = F, 
                                  value = "count", sdate = s_start_date, edate = s_end_date)
    pct517 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, age = "05-17", hospital = F, 
                                   value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt517 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, age = "05-17", hospital = F, 
                                   value = "count", sdate = s_start_date, edate = s_end_date)
    pct1844 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, age = "18-44", hospital = F, 
                                    value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt1844 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, age = "18-44", hospital = F, 
                                    value = "count", sdate = s_start_date, edate = s_end_date)
    pct4564 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, age = "45-64", hospital = F, 
                                    value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt4564 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, age = "45-64", hospital = F, 
                                    value = "count", sdate = s_start_date, edate = s_end_date)
    pct65 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, age = "65-1000", hospital = F, 
                                  value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt65 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, age = "65-1000", hospital = F, 
                                  value = "count", sdate = s_start_date, edate = s_end_date)
    pctunk <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, age = "unknown", hospital = F, 
                                   value = "percent", sdate = s_start_date, edate = s_end_date)
    cntunk <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, age = "unknown", hospital = F, 
                                   value = "count", sdate = s_start_date, edate = s_end_date)
    # Bring together
    output <- bind_rows(left_join(pct04, cnt04, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pct517, cnt517, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pct1844, cnt1844, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pct4564, cnt4564, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pct65, cnt65, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pctunk, cntunk, by = c("date", "age", "setting", "query", "syndrome", "hospital")))
    return(output)
  }))
  
  
  ### ED and urgent care
  age_specific_ed_uc_weekly <- bind_rows(lapply(c("pneumonia", "ili", "cli"), function(x) {
    # Run queries
    pct04 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "00-04", hospital = F, 
                                  value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt04 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "00-04", hospital = F, 
                                  value = "count", sdate = s_start_date, edate = s_end_date)
    pct517 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "05-17", hospital = F, 
                                   value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt517 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "05-17", hospital = F, 
                                   value = "count", sdate = s_start_date, edate = s_end_date)
    pct1844 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "18-44", hospital = F, 
                                    value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt1844 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "18-44", hospital = F, 
                                    value = "count", sdate = s_start_date, edate = s_end_date)
    pct4564 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "45-64", hospital = F, 
                                    value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt4564 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "45-64", hospital = F, 
                                    value = "count", sdate = s_start_date, edate = s_end_date)
    pct65 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "65-1000", hospital = F, 
                                  value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt65 <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "65-1000", hospital = F, 
                                  value = "count", sdate = s_start_date, edate = s_end_date)
    pctunk <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "unknown", hospital = F, 
                                   value = "percent", sdate = s_start_date, edate = s_end_date)
    cntunk <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "unknown", hospital = F, 
                                   value = "count", sdate = s_start_date, edate = s_end_date)
    # Bring together
    output <- bind_rows(left_join(pct04, cnt04, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pct517, cnt517, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pct1844, cnt1844, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pct4564, cnt4564, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pct65, cnt65, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pctunk, cntunk, by = c("date", "age", "setting", "query", "syndrome", "hospital")))
    return(output)
  }))
  
  ### Hospitalization
  age_specific_hosp_weekly <- bind_rows(lapply(c("pneumonia", "ili", "cli"), function(x) {
    # Run queries
    pct04 <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "00-04", hospital = F, 
                                  value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt04 <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "00-04", hospital = F, 
                                  value = "count", sdate = s_start_date, edate = s_end_date)
    pct517 <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "05-17", hospital = F, 
                                   value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt517 <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "05-17", hospital = F, 
                                   value = "count", sdate = s_start_date, edate = s_end_date)
    pct1844 <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "18-44", hospital = F, 
                                    value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt1844 <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "18-44", hospital = F, 
                                    value = "count", sdate = s_start_date, edate = s_end_date)
    pct4564 <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "45-64", hospital = F, 
                                    value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt4564 <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "45-64", hospital = F, 
                                    value = "count", sdate = s_start_date, edate = s_end_date)
    pct65 <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "65-1000", hospital = F, 
                                  value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt65 <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "65-1000", hospital = F, 
                                  value = "count", sdate = s_start_date, edate = s_end_date)
    pctunk <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "unknown", hospital = F, 
                                   value = "percent", sdate = s_start_date, edate = s_end_date)
    cntunk <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "unknown", hospital = F, 
                                   value = "count", sdate = s_start_date, edate = s_end_date)
    # Bring together
    output <- bind_rows(left_join(pct04, cnt04, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pct517, cnt517, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pct1844, cnt1844, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pct4564, cnt4564, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pct65, cnt65, by = c("date", "age", "setting", "query", "syndrome", "hospital")),
                        left_join(pctunk, cntunk, by = c("date", "age", "setting", "query", "syndrome", "hospital")))
    return(output)
  }))
  
  
  
  #### WEEKLY - MAKE AND SAVE COMBNIED DATASET ####
  message("Bringing together and saving weekly data")
  
  ### Combine data
  nwkly <- bind_rows(
    # weekly ALL AGE
    all_age_ed_weekly, all_age_ed_uc_weekly, all_age_hosp_weekly,
    # BY HOSPITAL
    all_age_ed_byhosp_weekly,
    # AGE SPECIFIC
    age_specific_ed_weekly, age_specific_ed_uc_weekly, age_specific_hosp_weekly)
  
  ### Pull out dates and convert to get weeks
  nwkly_date <- sort(unique(nwkly$date))
  nwkly_date <- cbind(nwkly_date, MMWRweek(nwkly_date))
  
  ### Finish making new variables
  nwkly <- left_join(nwkly, nwkly_date, by = c("date" = "nwkly_date")) %>%
    rename(year = MMWRyear, week = MMWRweek, day = MMWRday) %>%
    mutate(frequency = "weekly",
           season = case_when(
             year == 2017 & week >= 40 ~ "2017-2018",
             year == 2018 & week < 40 ~ "2017-2018",
             year == 2018 & week >= 40 ~ "2018-2019",
             year == 2019 & week < 40 ~ "2018-2019",
             year == 2019 & week >= 40 ~ "2019-2020",
             year == 2020 & week < 40 ~ "2019-2020"),
           week = as.factor(week),
           week = factor(week, levels(week)[c(40:52, 1:39)], ordered = TRUE),
           essence_refresh_date = today()) %>%
    filter(!is.na(season)) %>%
    mutate_at(vars(pct, expected_pct, levels_pct, colorID_pct,
                   cnt, expected_cnt, levels_cnt, colorID_cnt),
              list(~ as.numeric(.)))
  
  
  ### Combine with historical data
  if (historical == F) {
    wkly <- readRDS(file.path(output_path, "wkly.RData"))
    wkly <- wkly %>% filter(date < s_start_date)
    
    nwkly <- bind_rows(wkly, nwkly)
  }
  
  ### Set up sort order for Tableau
  nwkly <- nwkly %>%
    arrange(frequency, date, query, season, setting, age, hospital) %>%
    group_by(frequency, query, season, setting, age, hospital) %>%
    mutate(date_sort = row_number()) %>%
    ungroup()
  
  
  ### Write out data
  if (historical == F) {
    saveRDS(nwkly, file = file.path(output_path, "nwkly.RData"))
    write_csv(nwkly, file.path(output_path, "nwkly.csv"))
  } else {
    saveRDS(nwkly, file = file.path(output_path, "wkly.RData"))
  }
  
  
  #### WEEKLY - CLEAN UP ####
  rm(nwkly,
     all_age_ed_weekly, all_age_ed_byhosp_weekly, all_age_ed_uc_weekly, all_age_hosp_weekly,
     age_specific_ed_weekly, age_specific_ed_uc_weekly, age_specific_hosp_weekly)
  
}

message("Alert code completed")