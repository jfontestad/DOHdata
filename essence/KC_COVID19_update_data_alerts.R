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

library(httr)
httr::set_config(config(ssl_verifypeer = 0L))
library(jsonlite)
library(keyring)
library(splines)
library(multcomp)
library(tidyverse)

# date objects for ESSENCE queries and NRVESS data pull
### SET WHETHER THIS IS A HISTORICAL RUN OR NOT
historical <- F
historical_current <- T # Set to T if you want the historical run to also overwrite current data

if (historical == F) {
  s_start_date <- as_date("2020-06-01", "%Y-%m-%d")
} else {
  s_start_date <- as_date("2017-10-01", "%Y-%m-%d")
}

s_end_date <- today() - 1

### Set up folder to work in
output_path <- "//phshare01/cdi_share/Analytics and Informatics Team/Data Requests/2020/372_nCoV Essence Extract/From Natasha on March 13"

### Bring in ZIP data
zips <- data.table::fread("https://raw.githubusercontent.com/PHSKC-APDE/reference-data/master/spatial_data/zip_to_region.csv")
zips <- zips %>% mutate(zip = as.character(zip),
                        cc_region = case_when(
                          cc_region == 'east' ~ "East",
                          cc_region == 'north' ~ "North",
                          cc_region == 'seattle' ~ "Seattle",
                          cc_region == 'south' ~ "South",
                          TRUE ~ cc_region
                        ))


#### FUNCTIONS ####
# Call in from Github repo
source("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/essence/essence_query_functions.R")


#### PERSON-LEVEL DATA ####
# Need to run this for ZIP-level and race aggregation below.
# Only write out if historical = F
# Can just filter ED visits to get inpatient
# Only need to run for all ED, can use recoding and filters to find CLI, ILI, etc.
message("Running person-level section")

#### PERSON-LEVEL - ED VISITS ####
if (historical == F) {
  # All
  system.time(pdly_full_all_ed <- syndrome_person_level_query(syndrome = "all", ed = T,
                                                              sdate = s_start_date, edate = s_end_date)) 
} else if (historical == T) {
  ### Break up into smaller chunks to avoid overwhelming things
  date_range <- seq(s_start_date, s_end_date, by = '91 days')
  # All
  # Add in a try for this because it takes a long time to run and we don't want
  # to lose the successful runs. This needs to be run manually and the output inspected
  system.time(pdly_full_all_ed <- lapply(date_range, function(x) {
    try(syndrome_person_level_query(syndrome = "all", ed = T, sdate = x, edate = min(x + 90, s_end_date)))
  }))
  names(pdly_full_all_ed) <- date_range
  # If any list elements are null investigate and rerun those dates
  # Otherwise, convert into a data frame
  pdly_full_all_ed <- bind_rows(pdly_full_all_ed)
}

#### PERSON-LEVEL - RECODE ####
message("Recoding line-level data")
pdly_full_all_ed <- essence_recode(pdly_full_all_ed)


#### PERSON-LEVEL - JOIN TO HISTORICAL DATA ####
if (historical == F) {
  # Pull in existing data and remove dates from before today's run
  pdly_full_all_ed_historical <- readRDS(file = paste0(output_path, "/pdly_full_all_ed_historical.RData"))
  pdly_full_all_ed_historical <- pdly_full_all_ed_historical %>% filter(date < s_start_date) %>% dplyr::select(-date_sort)
  
  # Bind to new data
  pdly_full_all_ed_output <- bind_rows(pdly_full_all_ed_historical, pdly_full_all_ed)
} else if (historical == T) {
  pdly_full_all_ed_output <- pdly_full_all_ed
}

# Set up sort order
sort_order <- pdly_full_all_ed_output %>% 
  distinct(date, season) %>%
  arrange(date, season) %>%
  group_by(season) %>%
  mutate(date_sort = row_number()) %>%
  ungroup()

pdly_full_all_ed_output <- left_join(pdly_full_all_ed_output, sort_order, 
                                     by = c("date", "season"))

# Splines function
# CDC Criteria - Decreases in ED and/or outpatient visits for ILI/CLI
# Downward trajectory of ILI/CLI (or minimal ILI activity or near prepandemic level of CLI ED visits) reported over a 14-day period
#   * Uses a 3-day average in a cubic  smoothing spline 
#   * 14 consecutive days of decline required but can use a 2 – 3 day grace period if data are inconsistent 
#   * Look at both total visits for ILI/CLI and percentage of visits for ILI/CLI 
#   * 14th day must be lower than 1st day 
#   * If near prepandemic level of CLI ED visits has been reached, can meet if prepandemic level is maintained over 14 consecutive days 
#     (2-3 day grace period) 
fnFit <- function(df, group_cat, outcome = c("cnt3day","pct3day")) {
  tmp <- df %>%
    filter(group == group_cat)
  
  out_type <- case_when(outcome == "cnt3day" ~ "cnt",
                        outcome == "pct3day" ~ "pct")
  
  fit <- smooth.spline(x = tmp$date, y = tmp[[outcome]], spar = 0.5)
  
  # Get jack-knife residuals to compute 95% confidence interval
  res <- (fit$yin - fit$y)/(1-fit$lev)      # jackknife residuals
  sigma <- sqrt(var(res))                   # estimate sd
  
  tmp2 <- tmp %>%
    mutate(pred = predict(fit)$y,
           lower = fit$y - 2.0*sigma*sqrt(fit$lev), #lower CI
           upper = fit$y + 2.0*sigma*sqrt(fit$lev)  #upper CI
    ) %>%
    mutate(diff3day = pred - lag(pred))
  
  # Determine if slope is increasing or decreasing today
  inc_or_dec <- case_when(tmp2$diff3day[tmp2$date == max(tmp2$date)] < 0 ~ "Decreasing",
                          TRUE ~ "Increasing")
  
  # Determine how many consecutive days the slope has been declining
  tmp3 <- tmp2 %>%
    mutate(slope_indicator = case_when(inc_or_dec == "Decreasing" ~ diff3day < 0,
                                       TRUE ~ diff3day >= 0))
  
  last_inc_dec <- max(tmp3$date[tmp3$slope_indicator == FALSE], na.rm = T)
  
  tmp3 <- tmp3 %>%
    mutate(consecutive_day_count = cumsum(case_when(date > last_inc_dec ~ 1, TRUE ~ 0)))
  
  streak <- max(tmp3$consecutive_day_count)
  
  # Is 14th day lower than the 1st day?
  compare_first_last14 <- diff(tail(tmp3$pred,14)[c(1,14)])
  
  tmp4 <- tmp3 %>%
    mutate(inc_or_dec = inc_or_dec,
           streak = streak,
           compare_first_last14 = compare_first_last14,
           label = paste0(inc_or_dec, " ", streak, "-day streak")) %>%
    select(-slope_indicator,-consecutive_day_count) %>%
    rename_at(vars(pred:label), function(x) paste0(out_type, "_", x))
  
  return(tmp4)
}

#### PERSON-LEVEL - TIME AGGREGATION ####
### Query to summarise the data
essence_summary_time <- function(df = pdly_full_all_ed_output,
                                 condition = c("all", "pneumonia", "ili", "cli", "cli_pneumo", "telehealth"),
                                 setting = c("ed", "hosp"), 
                                 cat = c("all", "age_grp", "race", "ethnicity", "race_eth",
                                         "smoker", "obese", "setting", "facility", 
                                         "region", "zipcode"),
                                 frequency = c("daily", "weekly")) {
  
  condition <- match.arg(condition)
  setting_text <- match.arg(setting)
  cat_text <- match.arg(cat)
  freq_text <- match.arg(frequency)
  
  if (setting_text == "hosp") {df <- df %>% filter(HasBeenI == 1)}
  
  # Special treatment for ZIPs so that only KC ones are kept
  if (cat_text == "zipcode") {
    df <- df %>% filter(kc_zip == 1) %>% 
      mutate(zipcode = ZipCode)
  }
  
  # Rename certain fields
  if (cat_text == "smoker") {df <- df %>% mutate(smoker = smoker_general)}
  if (cat_text == "facility") {df <- df %>% mutate(facility = HospitalName)}
  if (cat_text == "region") {df <- df %>% mutate(region = cc_region)}
  
  # Set up time frame
  if (freq_text == "weekly") {
    df <- df %>% mutate(date = MMWRdate)
  }
  
  # Set up recodes
  if (condition == "all") {input <- df %>% mutate(query = "all")
  } else if (condition == "pneumonia") {
    input <- df %>% filter(pneumo == 1) %>% mutate(query = "pneumo")
  } else if (condition == "ili") {
    input <- df %>% filter(ili == 1) %>% mutate(query = "ili")
  } else if (condition == "cli") {
    input <- df %>% filter(cli == 1) %>% mutate(query = "cli")
  } else if (condition == "cli_pneumo") {
    input <- df %>% filter(cli_pneumo == 1) %>% mutate(query = "cli_pneumo")
  } else if (condition == "telehealth") {
    input <- df %>% filter(telehealth == 1) %>% mutate(query = "telehealth")
  }
  
  
  # Summarise data
  if (cat_text != "all") {
    cat_quo <- rlang::sym(cat_text)
    input <- input %>% mutate(group = !!cat_quo) 
    df <- df %>% mutate(group = !!cat_quo)
  } else {
    input <- input %>% mutate(group = "all")
    df <- df %>% mutate(group = "all")
  }
  
  # Set up a frame of all date and group combos
  # (avoids issues in Tableau)
  if (freq_text == "weekly") {
    date_grid <- expand.grid(date = seq(min(df$date), max(df$date), by = "1 week"),
                             query = unique(input$query),
                             group = unique(df$group),
                             stringsAsFactors = F)
  } else {
    date_grid <- expand.grid(date = seq(min(df$date), max(df$date), by = "1 day"),
                             query = unique(input$query),
                             group = unique(df$group),
                             stringsAsFactors = F)
  }
  # Add in seasons and dates so they are not missing
  seasons <- df %>% distinct(date, season, year, week)
  date_grid <- date_grid %>% left_join(., seasons, by = "date")

  
  # Summarise data
  cnt <- input %>% group_by(season, year, week, date, query, group) %>% summarise(cnt = n()) %>% ungroup()
  pct <- df %>% group_by(season, year, week, date, group) %>% summarise(tot = n()) %>% ungroup()
  
  # Produce output
  ## Combine count and percent datasets
  combined <- left_join(date_grid, cnt, by = c("season", "year", "week", "date", "query", "group")) %>%
    left_join(., pct, by = c("season", "year", "week", "date", "group")) %>%
    mutate(pct = round(cnt / tot, 6),
           setting = setting_text,
           category = cat_text,
           zipcode = ifelse(category == "zipcode", group, NA_character_)) %>% 
    mutate_at(vars(cnt, tot, pct), list(~ replace_na(., 0))) 
  # %>%
  #   dplyr::select(-tot)

  if (freq_text == "daily") {
    ## Run splines on daily dataset only
    ## Set up 3-day rolling average for count
    
    if (condition %in% c("cli","ili","pneumonia","all") &
        cat_text == "all") {
      
      comb2 <- combined %>%
        group_by(group) %>%
      #  filter(date != max(date)) %>% #
        arrange(group,date) %>% 
        mutate(cnt3day = zoo::rollmean(cnt, 3, na.pad = T, align = "right"),
               tot3day = zoo::rollmean(tot, 3, na.pad = T, align = "right")
        ) %>%
        mutate(pct3day = cnt3day/tot3day) %>%
        select(-tot3day) %>%
        filter(!is.na(cnt3day))
      
      args <- list(group_cat = unique(comb2$group),
                   outcome = c("cnt3day","pct3day")) %>%
        cross_df()
      
      comb3 <- map_dfr(.x = unique(comb2$group),
                       fnFit,
                       df = comb2,
                       outcome = "cnt3day") %>%
        left_join(map_dfr(.x = unique(comb2$group),
                          fnFit,
                          df = comb2,
                          outcome = "pct3day"))
      
      output <- comb3 %>% 
        dplyr::select(-tot) %>%
        rename(cnt_daily = cnt, pct_daily = pct)
    } else {
      output <- combined %>%
        dplyr::select(-tot) %>%
        rename(cnt_daily = cnt, pct_daily = pct)
    }
    
  } else {
    output <- combined %>% 
      dplyr::select(-tot) %>%
      rename(cnt_weekly = cnt, pct_weekly = pct)
  }
  
  output
}

### ED visits
summary_time_ed <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli", "cli_pneumo"), function(x) {
  daily <- bind_rows(
    lapply(c("all", "age_grp", "race", "ethnicity", "race_eth", "smoker", "obese", 
             "setting", "facility", "region"), 
           essence_summary_time, df = pdly_full_all_ed_output,
           condition = x, setting = "ed", frequency = "daily"))
  
  weekly <- bind_rows(
    lapply(c("all", "age_grp", "race", "ethnicity", "race_eth", "smoker", "obese", 
             "setting", "facility", "region", "zipcode"), 
           essence_summary_time, df = pdly_full_all_ed_output, 
           condition = x, setting = "ed", frequency = "weekly"))
  
  total <- full_join(daily, weekly, 
                     by = c("season", "year", "week", "date", "query", 
                            "category", "group", "setting", "zipcode"))
  return(total)
}))


### Hospitalizations
summary_time_hosp <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli", "cli_pneumo"), function(x) {
  daily <- bind_rows(
    lapply(c("all", "age_grp", "race", "ethnicity", "race_eth", "smoker", "obese", 
             "setting", "facility", "region"), 
           essence_summary_time, df = pdly_full_all_ed_output, 
           condition = x, setting = "hosp", frequency = "daily"))
  
  weekly <- bind_rows(
    lapply(c("all", "age_grp", "race", "ethnicity", "race_eth", "smoker", "obese", 
             "setting", "facility", "region", "zipcode"), 
           essence_summary_time, df = pdly_full_all_ed_output, 
           condition = x, setting = "hosp", frequency = "weekly"))
  
  total <- full_join(daily, weekly, 
                     by = c("season", "year", "week", "date", "query", 
                            "category", "group", "setting", "zipcode"))
  return(total)
}))


#### PERSON-LEVEL - DEMOG AGGREGATION ####
# Query to sum up breakdowns of dx, testing, etc.
essence_summary_demog <- function(df = pdly_full_all_ed_output,
                                 condition = c("all", "pneumonia", "ili", "cli", "cli_pneumo", "telehealth"),
                                 setting = c("ed", "hosp"), 
                                 cat = c("all", "age_grp", "race", "ethnicity", "race_eth",
                                         "smoker", "obese", "setting", "facility", 
                                         "region", "zipcode"),
                                 frequency = c("weekly", "daily")) {
  
  condition <- match.arg(condition)
  setting_text <- match.arg(setting)
  cat_text <- match.arg(cat)
  freq_text <- match.arg(frequency)
  
  if (setting_text == "hosp") {df <- df %>% filter(HasBeenI == 1)}
  
  # Special treatment for ZIPs so that only KC ones are kept
  if (cat_text == "zipcode") {
    df <- df %>% filter(kc_zip == 1) %>% 
      mutate(zipcode = ZipCode)
  }
  
  # Rename certain fields
  if (cat_text == "smoker") {df <- df %>% mutate(smoker = smoker_general)}
  if (cat_text == "facility") {df <- df %>% mutate(facility = HospitalName)}
  if (cat_text == "region") {df <- df %>% mutate(region = cc_region)}
  
  # Set up time frame
  if (freq_text == "weekly") {df <- df %>% mutate(date = MMWRdate)}
  
  # Set up recodes
  if (condition == "all") {input <- df %>% mutate(query = "all")}
  else if (condition == "pneumonia") {
    input <- df %>% filter(pneumo == 1) %>% mutate(query = "pneumo")
  } else if (condition == "ili") {
    input <- df %>% filter(ili == 1) %>% mutate(query = "ili")
  } else if (condition == "cli") {
    input <- df %>% filter(cli == 1) %>% mutate(query = "cli")
  } else if (condition == "cli_pneumo") {
    input <- df %>% filter(cli_pneumo == 1) %>% mutate(query = "cli_pneumo")
  } else if (condition == "telehealth") {
    input <- df %>% filter("telehealth" == 1) %>% mutate(query = "telehealth")
  }
  

  # Summarise data
  if (cat_text != "all") {
    cat_quo <- rlang::sym(cat_text)
    input <- input %>% mutate(group = !!cat_quo) 
    df <- df %>% mutate(group = !!cat_quo)
  } else {
    input <- input %>% mutate(group = "all")
    df <- df %>% mutate(group = "all")
  }
  
  # Set up a frame of all date and group combos
  # (avoids issues in Tableau)
  if (freq_text == "weekly") {
    date_grid <- expand.grid(date = seq(min(df$date), max(df$date), by = "1 week"),
                             query = unique(input$query),
                             group = unique(df$group),
                             stringsAsFactors = F)
  } else {
    date_grid <- expand.grid(date = seq(min(df$date), max(df$date), by = "1 day"),
                             query = unique(input$query),
                             group = unique(df$group),
                             stringsAsFactors = F)
  }
  # Add in seasons and dates so they are not missing
  seasons <- df %>% distinct(date, season, year, week)
  date_grid <- date_grid %>% left_join(., seasons, by = "date")
  
  
  # Summarise data
  cnt_dx_narrow <- input %>% filter(covid_dx_narrow == 1) %>%
    group_by(season, year, week, date, query, group) %>% 
    summarise(cnt_dx_narrow = n()) %>% ungroup()
  covid_dx_broad <- input %>% filter(covid_dx_broad == 1) %>%
    group_by(season, year, week, date, query, group) %>% 
    summarise(cnt_dx_broad = n()) %>% ungroup()
  covid_test <- input %>% filter(covid_test == 1) %>%
    group_by(season, year, week, date, query, group) %>% 
    summarise(cnt_test = n()) %>% ungroup()
  
  # Produce output
  output <- left_join(date_grid, cnt_dx_narrow, by = c("season", "year", "week", "date", "query", "group")) %>%
    left_join(., covid_dx_broad, by = c("season", "year", "week", "date", "query", "group")) %>%
    left_join(., covid_test, by = c("season", "year", "week", "date", "query", "group")) %>%
    mutate(setting = setting_text,
           category = cat_text,
           zipcode = ifelse(category == "zipcode", group, NA_character_)) %>% 
    mutate_at(vars(cnt_dx_narrow, cnt_dx_broad, cnt_test), list(~ replace_na(., 0)))
  
  output
}


### ED visits
summary_demog_ed <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli", "cli_pneumo"), function(x) {
  
  weekly_ed <- bind_rows(
    lapply(c("all", "age_grp", "race", "ethnicity", "race_eth", "smoker", "obese", 
             "setting", "facility", "region", "zipcode"), 
           essence_summary_demog, df = pdly_full_all_ed_output, 
           condition = x, setting = "ed", frequency = "weekly"))
  
  return(weekly_ed)
}))


### Hospitalizations
summary_demog_hosp <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli", "cli_pneumo"), function(x) {
  weekly_hosp <- bind_rows(
    lapply(c("all", "age_grp", "race", "ethnicity", "race_eth", "smoker", "obese", 
             "setting", "facility", "region", "zipcode"), 
           essence_summary_demog, df = pdly_full_all_ed_output, 
           condition = x, setting = "hosp", frequency = "weekly"))
  return(weekly_hosp)
}))



#### PERSON LEVEL - WRITE OUT ####
# Write out summarized data for use in the Tableau viz
summary_all <- bind_rows(left_join(summary_time_ed, summary_demog_ed, 
                                   by = c("setting", "season", "year", "week", "date", "query", 
                                          "category", "group", "zipcode")),
                         left_join(summary_time_hosp, summary_demog_hosp, 
                                   by = c("setting", "season", "year", "week", "date", "query", 
                                          "category", "group", "zipcode")))

# Set up sort order
summary_all <- summary_all %>%
  arrange(season, date) %>%
  group_by(setting, season, query, category, group) %>%
  mutate(date_sort = row_number()) %>%
  ungroup()


write.csv(summary_all,
          file = file.path(output_path, "pdly_full_all_ed_summary.csv"), row.names = F)


# Also write out a smaller version of overall data for use in the Tableau viz
# write.csv(dplyr::select(pdly_full_all_ed_output, C_BioSense_ID,
#                  date, year, week, day, MMWRdate, season, date_sort,
#                  setting, HospitalName, ZipCode, C_Patient_County, cc_region, kc_zip,
#                  Age, age_grp, sex, aian, asian, black, nhpi, other, white, race, ethnicity,
#                  bmi, overweight, obese, obese_severe,
#                  smoking_text, smoker_current, smoker_general,
#                  HasBeenE, HasBeenI, HasBeenO, C_Death,
#                  covid_dx_broad, covid_dx_narrow, covid_test, cli, pneumo, cli_pneumo, ili),
#           file = file.path(output_path, "pdly_full_all_ed.csv"), row.names = F)



# Export
if (historical == F) {
  saveRDS(pdly_full_all_ed_output, file = paste0(output_path, "/pdly_full_all_ed.RData"))
} else if (historical == T & historical_current == T) {
  saveRDS(pdly_full_all_ed_output, file = paste0(output_path, "/pdly_full_all_ed.RData"))
  saveRDS(pdly_full_all_ed_output, file = paste0(output_path, "/pdly_full_all_ed_historical.RData"))
} else if (historical == T & historical_current == F) {
  saveRDS(pdly_full_all_ed_output, file = paste0(output_path, "/pdly_full_all_ed_historical.RData"))
}



#### DAILY RUN ####
#### DAILY - ALL AGE ####
message("Running daily all-ages section")


all_age_ed_daily <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli", "influenza"), function(x) {
  # Run both queries
  pct <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, hospital = F, 
                              value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, hospital = F, 
                              value = "count", sdate = s_start_date, edate = s_end_date)
  # Bring together
  output <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital"))
  return(output)
}))

all_age_hosp_daily <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli", "influenza"), function(x) {
  # Run both queries
  pct <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, hospital = F, 
                              value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, hospital = F, 
                              value = "count", sdate = s_start_date, edate = s_end_date)
  # Bring together
  output <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital"))
  return(output)
}))

# all_age_ed_uc_daily <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), function(x) {
#   # Run both queries
#   pct <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, hospital = F, 
#                               value = "percent", sdate = s_start_date, edate = s_end_date)
#   cnt <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, hospital = F, 
#                               value = "count", sdate = s_start_date, edate = s_end_date)
#   # Bring together
#   output <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital"))
#   return(output)
# }))


#### DAILY - ALL ADULTS ####
all_adult_ed_daily <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli", "influenza"), function(x) {
  # Run both queries
  pct <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, 
                              age = c("18-44", "45-64", "65-1000"), hospital = F, 
                              value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, 
                              age = c("18-44", "45-64", "65-1000"), hospital = F, 
                              value = "count", sdate = s_start_date, edate = s_end_date)
  # Bring together
  output <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital"))
  return(output)
}))

all_adult_hosp_daily <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli", "influenza"), function(x) {
  # Run both queries
  pct <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, 
                              age = c("18-44", "45-64", "65-1000"), hospital = F, 
                              value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt <- syndrome_alert_query(frequency = "daily", syndrome = x, inpatient = T, 
                              age = c("18-44", "45-64", "65-1000"), hospital = F, 
                              value = "count", sdate = s_start_date, edate = s_end_date)
  # Bring together
  output <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital"))
  return(output)
}))

# all_adult_ed_uc_daily <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), function(x) {
#   # Run both queries
#   pct <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, 
#                               age = c("18-44", "45-64", "65-1000"), hospital = F, 
#                               value = "percent", sdate = s_start_date, edate = s_end_date)
#   cnt <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, 
#                               age = c("18-44", "45-64", "65-1000"), hospital = F, 
#                               value = "count", sdate = s_start_date, edate = s_end_date)
#   # Bring together
#   output <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital"))
#   return(output)
# }))


#### DAILY - BY HOSPITAL ####
all_age_ed_byhosp_daily <- bind_rows(lapply(c("cli"), function(x) {
  # Run both queries
  pct <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, hospital = T, 
                              value = "percent", sdate = s_start_date, edate = s_end_date)
  cnt <- syndrome_alert_query(frequency = "daily", syndrome = x, ed = T, hospital = T, 
                              value = "count", sdate = s_start_date, edate = s_end_date)
  # Bring together
  output <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital"))
  return(output)
}))


#### DAILY - BY ZIP/REGION ####
# Not running daily now, just weekly due to small numbers


#### DAILY - AGE SPECIFIC ####
message("Running daily age-specific section")

### ED visits
age_specific_ed_daily <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), function(x) {
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
  output <- bind_rows(left_join(pct04, cnt04, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                      left_join(pct517, cnt517, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                      left_join(pct1844, cnt1844, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                      left_join(pct4564, cnt4564, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                      left_join(pct65, cnt65, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                      left_join(pctunk, cntunk, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")))
  return(output)
}))


### Hospitalization
age_specific_hosp_daily <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), function(x) {
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
  output <- bind_rows(left_join(pct04, cnt04, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                      left_join(pct517, cnt517, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                      left_join(pct1844, cnt1844, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                      left_join(pct4564, cnt4564, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                      left_join(pct65, cnt65, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                      left_join(pctunk, cntunk, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")))
  return(output)
}))


### ED and urgent care
# age_specific_ed_uc_daily <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), function(x) {
#   # Run queries
#   pct04 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "00-04", hospital = F, 
#                                 value = "percent", sdate = s_start_date, edate = s_end_date)
#   cnt04 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "00-04", hospital = F, 
#                                 value = "count", sdate = s_start_date, edate = s_end_date)
#   pct517 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "05-17", hospital = F, 
#                                  value = "percent", sdate = s_start_date, edate = s_end_date)
#   cnt517 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "05-17", hospital = F, 
#                                  value = "count", sdate = s_start_date, edate = s_end_date)
#   pct1844 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "18-44", hospital = F, 
#                                   value = "percent", sdate = s_start_date, edate = s_end_date)
#   cnt1844 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "18-44", hospital = F, 
#                                   value = "count", sdate = s_start_date, edate = s_end_date)
#   pct4564 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "45-64", hospital = F, 
#                                   value = "percent", sdate = s_start_date, edate = s_end_date)
#   cnt4564 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "45-64", hospital = F, 
#                                   value = "count", sdate = s_start_date, edate = s_end_date)
#   pct65 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "65-1000", hospital = F, 
#                                 value = "percent", sdate = s_start_date, edate = s_end_date)
#   cnt65 <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "65-1000", hospital = F, 
#                                 value = "count", sdate = s_start_date, edate = s_end_date)
#   pctunk <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "unknown", hospital = F, 
#                                  value = "percent", sdate = s_start_date, edate = s_end_date)
#   cntunk <- syndrome_alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "unknown", hospital = F, 
#                                  value = "count", sdate = s_start_date, edate = s_end_date)
#   # Bring together
#   output <- bind_rows(left_join(pct04, cnt04, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
#                       left_join(pct517, cnt517, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
#                       left_join(pct1844, cnt1844, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
#                       left_join(pct4564, cnt4564, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
#                       left_join(pct65, cnt65, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
#                       left_join(pctunk, cntunk, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")))
#   return(output)
# }))


#### DAILY - RACE/ETHNICITY SPECIFIC ####
message("Running daily race/ethnicity-specific section")
# Use the person-level data because running each time series run is very slow

### Query to summarise the data
race_eth_summary <- function(condition = c("all", "pneumonia", "ili", "cli"),
                         setting = c("ed", "hosp"), type = c("race", "ethnicity"),
                         frequency = c("daily", "weekly")) {
  
  condition <- match.arg(condition)
  setting <- match.arg(setting)
  type <- match.arg(type)
  frequency <- match.arg(frequency)
  
  if (setting == "ed") {df <- pdly_full_all_ed} 
  if (setting == "hosp") {df <- pdly_full_all_ed %>% filter(HasBeenI == 1)}
  
  if (frequency == "weekly") {df <- df %>% mutate(date = MMWRdate)}
  
  # Clean up missing values
  df <- df %>% mutate_at(vars(race, ethnicity), list(~ ifelse(is.na(.), "Unknown", .)))
  
  # Set up dummy variable so grouping below works
  if (type == "race") {df <- df %>% mutate(ethnicity = "all")} 
  else if (type == "ethnicity") {df <- df %>% mutate(race = "all")}
  
  # Set up recodes
  if (condition == "all") {input <- df %>% mutate(query = "all")}
  else if (condition == "pneumonia") {
    input <- df %>% filter(pneumo == 1) %>% mutate(query = "pneumo")
  } else if (condition == "ili") {
    input <- df %>% filter(ili == 1) %>% mutate(query = "ili")
  } else if (condition == "cli") {
    input <- df %>% filter(cli == 1) %>% mutate(query = "cli")
  }
  
  # Summarise data
  cnt <- input %>% group_by(date, query, race, ethnicity) %>%
    summarise(cnt = n()) %>% ungroup()
  
  pct <- df %>% group_by(date, race, ethnicity) %>% summarise(tot = n()) %>% ungroup()
  
  output <- left_join(cnt, pct, by = c("date", "race", "ethnicity")) %>%
    mutate(pct = cnt / tot * 100) %>% dplyr::select(-tot)
  
  output
}


### ED visits
race_specific_ed_daily <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), 
                                           race_eth_summary, setting = "ed", 
                                           type = "race", frequency = "daily"))
eth_specific_ed_daily <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), 
                                          race_eth_summary, setting = "ed", 
                                          type = "ethnicity", frequency = "daily"))
                                    

### Hospitalizations
race_specific_hosp_daily <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), 
                                           race_eth_summary, setting = "hosp", 
                                           type = "race", frequency = "daily"))
eth_specific_hosp_daily <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), 
                                          race_eth_summary, setting = "hosp", 
                                          type = "ethnicity", frequency = "daily"))


### Set up matrix of all date/race permutations so zero values show up
# Relies on running the summary code first
race_specific_grid_daily <- expand.grid(date = unique(race_specific_ed_daily$date), 
                                        race = unique(race_specific_ed_daily$race),
                                        query = unique(race_specific_ed_daily$query),
                                        stringsAsFactors = F)
eth_specific_grid_daily <- expand.grid(date = unique(eth_specific_ed_daily$date), 
                                       ethnicity = unique(eth_specific_ed_daily$ethnicity),
                                       query = unique(eth_specific_ed_daily$query),
                                       stringsAsFactors = F)

### Bring together grid and results
race_specific_ed_daily <- left_join(race_specific_grid_daily, race_specific_ed_daily, by = c("date", "race", "query")) %>%
  mutate(setting = "ed")
eth_specific_ed_daily <- left_join(eth_specific_grid_daily, eth_specific_ed_daily, by = c("date", "ethnicity", "query")) %>%
  mutate(setting = "ed")
race_specific_hosp_daily <- left_join(race_specific_grid_daily, race_specific_hosp_daily, by = c("date", "race", "query")) %>%
  mutate(setting = "hosp")
eth_specific_hosp_daily <- left_join(eth_specific_grid_daily, eth_specific_hosp_daily, by = c("date", "ethnicity", "query")) %>%
  mutate(setting = "hosp")


### Combine in a single df and add missing variables
race_eth_specific_daily <- bind_rows(race_specific_ed_daily, eth_specific_ed_daily, 
                                     race_specific_hosp_daily, eth_specific_hosp_daily) %>%
  mutate(syndrome = case_when(query == "all" ~ "all",
                              query == "pneumo" ~ "Pneumonia",
                              query == "ili" ~ "ILI",
                              query == "cli" ~ "CLI"),
         cnt = replace_na(cnt, 0L),
         pct = replace_na(pct, 0),
         age = "all age", zip = "all", hospital = "all") %>%
  mutate_at(vars(race, ethnicity), list(~ replace_na(., "all"))) %>%
  dplyr::select(date, pct, age, race, ethnicity, zip, setting, query, syndrome, hospital, cnt)


#### DAILY - MAKE AND SAVE COMBINED DATASET ####
message("Bringing together and saving daily data")

### Combine data
ndly <- bind_rows(
  # DAILY ALL AGE
  all_age_ed_daily, all_age_hosp_daily, #all_age_ed_uc_daily, 
  
  # DAILY ALL ADULT
  all_adult_ed_daily, all_adult_hosp_daily, #all_adult_ed_uc_daily,
  # BY HOSPITAL
  all_age_ed_byhosp_daily,
  # BY ZIP/REGION
  
  # AGE SPECIFIC
  age_specific_ed_daily, age_specific_hosp_daily, #age_specific_ed_uc_daily,
  # RACE/ETHNICITY SPECIFIC
  race_eth_specific_daily
  )

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
           year == 2020 & week < 40 ~ "2019-2020",
           year == 2020 & week >= 40 ~ "2020-2021"),
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
  arrange(frequency, date, query, season, setting, age, race, ethnicity, cc_region, hospital) %>%
  group_by(frequency, query, season, setting, age, race, ethnicity, cc_region, hospital) %>%
  mutate(date_sort = row_number()) %>%
  ungroup()


### Write out data
if (historical == F) {
  saveRDS(ndly, file = file.path(output_path, "ndly.RData"))
  write_csv(ndly, file.path(output_path, "ndly.csv"))
} else if (historical == T & historical_current == T) {
  saveRDS(ndly, file = file.path(output_path, "ndly.RData"))
  write_csv(ndly, file.path(output_path, "ndly.csv"))
  
  saveRDS(ndly, file = file.path(output_path, "dly.RData"))
} else if (historical == T & historical_current == F) {
  saveRDS(ndly, file = file.path(output_path, "dly.RData"))
}


#### DAILY - CLEAN UP ####
rm(pdly_full_pneumo_ed, pdly_full_pneumo_hosp, pdly_full_ili_ed, pdly_full_cli_ed, pdly_full_cli_hosp)

rm(dly,
   all_age_ed_daily, all_age_hosp_daily, all_age_ed_uc_daily, 
   all_adult_ed_daily, all_adult_ed_uc_daily, all_adult_hosp_daily,
   all_age_ed_byhosp_daily, all_age_byzip_daily,
   age_specific_ed_daily, age_specific_ed_uc_daily, age_specific_hosp_daily,
   race_eth_specific_daily)


#### WEEKLY RUN ####
### ONLY RUN THIS IF IT'S A TUESDAY
if (wday(today(), label = F, week_start = getOption("lubridate.week.start", 1)) == 2) {
  message("It's Tuesday, time to run the weekly counts")
  
  ### Look back to find the most recent Saturday so we don't capture this week's data
  # Could just use s_end_date <- today() - 3 but this way is more robust in case
  #   the code is manually run on a non-Tuesday
  sat_diff <- 6 - wday(today(), label = F, week_start = getOption("lubridate.week.start", 1))
  if (sat_diff > 0) {
    s_end_date <- today() - (7 - sat_diff)
  } else {
    s_end_date <- today() - abs(sat_diff)
  }
  
  
  #### WEEKLY - ALL AGE ####
  message("Running weekly all-ages section")
  
  all_age_ed_weekly <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli", "influenza"), function(x) {
    # Run both queries
    pct <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, hospital = F, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, hospital = F, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
    # Bring together
    output <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital"))
    return(output)
  }))
  
  all_age_hosp_weekly <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli", "influenza"), function(x) {
    # Run both queries
    pct <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, hospital = F, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, hospital = F, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
    # Bring together
    output <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital"))
    return(output)
  }))
  
  # all_age_ed_uc_weekly <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), function(x) {
  #   # Run both queries
  #   pct <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, hospital = F, 
  #                               value = "percent", sdate = s_start_date, edate = s_end_date)
  #   cnt <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, hospital = F, 
  #                               value = "count", sdate = s_start_date, edate = s_end_date)
  #   # Bring together
  #   output <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital"))
  #   return(output)
  # }))

  
  #### WEEKLY - ALL ADULTS ####
  all_adult_ed_weekly <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli", "influenza"), function(x) {
    # Run both queries
    pct <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, 
                                age = c("18-44", "45-64", "65-1000"), hospital = F, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, 
                                age = c("18-44", "45-64", "65-1000"), hospital = F, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
    # Bring together
    output <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital"))
    return(output)
  })) 
  
  all_adult_hosp_weekly <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli", "influenza"), function(x) {
    # Run both queries
    pct <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, 
                                age = c("18-44", "45-64", "65-1000"), hospital = F, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, 
                                age = c("18-44", "45-64", "65-1000"), hospital = F, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
    # Bring together
    output <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital"))
    return(output)
  }))
  
  # all_adult_ed_uc_weekly <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), function(x) {
  #   # Run both queries
  #   pct <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, 
  #                               age = c("18-44", "45-64", "65-1000"), hospital = F, 
  #                               value = "percent", sdate = s_start_date, edate = s_end_date)
  #   cnt <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed_uc = T, 
  #                               age = c("18-44", "45-64", "65-1000"), hospital = F, 
  #                               value = "count", sdate = s_start_date, edate = s_end_date)
  #   # Bring together
  #   output <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital"))
  #   return(output)
  # }))

  
  #### WEEKLY - BY HOSPITAL ####
  all_age_ed_byhosp_weekly <- bind_rows(lapply(c("cli"), function(x) {
    # Run both queries
    pct <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, hospital = T, 
                                value = "percent", sdate = s_start_date, edate = s_end_date)
    cnt <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, hospital = T, 
                                value = "count", sdate = s_start_date, edate = s_end_date)
    # Bring together
    output <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital"))
    return(output)
  }))
  
  
  #### WEEKLY - BY ZIP/REGION ####
  # Collapsing to region for now because of small numbers each week in some ZIPs
  regions_to_run <- zips %>% filter(!is.na(cc_region)) %>% distinct(cc_region)
  
  
  all_age_ed_byzip_weekly <- bind_rows(lapply(c("cli", "pneumo"), function(x) {
    # Loop over each ZIP for a given syndrome
    output <- bind_rows(lapply(regions_to_run$cc_region, function(y) {
      zips_to_run <- zips %>% filter(cc_region == y) %>% distinct(zip)
      message("Working on ", y)
      # Run both queries
      pct <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, hospital = F, zip = zips_to_run$zip, 
                                  value = "percent", sdate = s_start_date, edate = s_end_date)
      cnt <- syndrome_alert_query(frequency = "weekly", syndrome = x, ed = T, hospital = F, zip = zips_to_run$zip,
                                  value = "count", sdate = s_start_date, edate = s_end_date)
      # Bring together
      zip_specific <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")) %>%
        mutate(cc_region = y, kc_zip = 1)
      zip_specific
    }))
    return(output)
  }))
  
  # Don't need to keep the long list of ZIPs so remove
  all_age_ed_byzip_weekly <- all_age_ed_byzip_weekly %>% mutate(zip = NA)
  
  
  all_age_hosp_byzip_weekly <- bind_rows(lapply(c("cli", "pneumo"), function(x) {
    # Loop over each ZIP for a given syndrome
    output <- bind_rows(lapply(regions_to_run$cc_region, function(y) {
      zips_to_run <- zips %>% filter(cc_region == y) %>% distinct(zip)
      message("Working on ", y)
      # Run both queries
      pct <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, hospital = F, zip = zips_to_run$zip, 
                                  value = "percent", sdate = s_start_date, edate = s_end_date)
      cnt <- syndrome_alert_query(frequency = "weekly", syndrome = x, inpatient = T, hospital = F, zip = zips_to_run$zip,
                                  value = "count", sdate = s_start_date, edate = s_end_date)
      # Bring together
      zip_specific <- left_join(pct, cnt, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")) %>%
        mutate(cc_region = y, kc_zip = 1)
      zip_specific
    }))
    return(output)
  }))
  
  # Don't need to keep the long list of ZIPs so remove
  all_age_hosp_byzip_weekly <- all_age_hosp_byzip_weekly %>% mutate(zip = NA)
  
  
  
  #### WEEKLY - AGE SPECIFIC ####
  message("Running weekly age-specific section")
  
  ### ED visits
  age_specific_ed_weekly <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), function(x) {
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
    output <- bind_rows(left_join(pct04, cnt04, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pct517, cnt517, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pct1844, cnt1844, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pct4564, cnt4564, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pct65, cnt65, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pctunk, cntunk, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")))
    return(output)
  }))
  
  
  ### ED and urgent care
  age_specific_ed_uc_weekly <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), function(x) {
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
    output <- bind_rows(left_join(pct04, cnt04, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pct517, cnt517, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pct1844, cnt1844, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pct4564, cnt4564, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pct65, cnt65, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pctunk, cntunk, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")))
    return(output)
  }))
  
  ### Hospitalization
  age_specific_hosp_weekly <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), function(x) {
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
    output <- bind_rows(left_join(pct04, cnt04, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pct517, cnt517, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pct1844, cnt1844, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pct4564, cnt4564, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pct65, cnt65, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")),
                        left_join(pctunk, cntunk, by = c("date", "age", "race", "ethnicity",  "zip", "setting", "query", "syndrome", "hospital")))
    return(output)
  }))
  
  
  #### WEEKLY - RACE/ETHNICITY SPECIFIC ####
  message("Running weekly race/ethnicity-specific section")
  # Just running CLI for now because this is very slow and we are only graphing CLI
  
  
  ### ED visits
  race_specific_ed_weekly <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), 
                                             race_eth_summary, setting = "ed", 
                                             type = "race", frequency = "weekly"))
  eth_specific_ed_weekly <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), 
                                            race_eth_summary, setting = "ed", 
                                            type = "ethnicity", frequency = "weekly"))
  
  
  ### Hospitalizations
  race_specific_hosp_weekly <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), 
                                               race_eth_summary, setting = "hosp", 
                                               type = "race", frequency = "weekly"))
  eth_specific_hosp_weekly <- bind_rows(lapply(c("all", "pneumonia", "ili", "cli"), 
                                              race_eth_summary, setting = "hosp", 
                                              type = "ethnicity", frequency = "weekly"))
  
  
  ### Set up matrix of all date/race permutations so zero values show up
  # Relies on running the summary code first
  race_specific_grid_weekly <- expand.grid(date = unique(race_specific_ed_weekly$date), 
                                          race = unique(race_specific_ed_weekly$race),
                                          query = unique(race_specific_ed_weekly$query),
                                          stringsAsFactors = F)
  eth_specific_grid_weekly <- expand.grid(date = unique(eth_specific_ed_weekly$date), 
                                         ethnicity = unique(eth_specific_ed_weekly$ethnicity),
                                         query = unique(eth_specific_ed_weekly$query),
                                         stringsAsFactors = F)
  
  ### Bring together grid and results
  race_specific_ed_weekly <- left_join(race_specific_grid_weekly, race_specific_ed_weekly, by = c("date", "race", "query")) %>%
    mutate(setting = "ed")
  eth_specific_ed_weekly <- left_join(eth_specific_grid_weekly, eth_specific_ed_weekly, by = c("date", "ethnicity", "query")) %>%
    mutate(setting = "ed")
  race_specific_hosp_weekly <- left_join(race_specific_grid_weekly, race_specific_hosp_weekly, by = c("date", "race", "query")) %>%
    mutate(setting = "hosp")
  eth_specific_hosp_weekly <- left_join(eth_specific_grid_weekly, eth_specific_hosp_weekly, by = c("date", "ethnicity", "query")) %>%
    mutate(setting = "hosp")
  
  
  ### Combine in a single df and add missing variables
  race_eth_specific_weekly <- bind_rows(race_specific_ed_weekly, eth_specific_ed_weekly, 
                                       race_specific_hosp_weekly, eth_specific_hosp_weekly) %>%
    mutate(syndrome = case_when(query == "all" ~ "all",
                                query == "pneumo" ~ "Pneumonia",
                                query == "ili" ~ "ILI",
                                query == "cli" ~ "CLI"),
           cnt = replace_na(cnt, 0L),
           pct = replace_na(pct, 0),
           age = "all age", zip = "all", hospital = "all") %>%
    mutate_at(vars(race, ethnicity), list(~ replace_na(., "all"))) %>%
    dplyr::select(date, pct, age, race, ethnicity, zip, setting, query, syndrome, hospital, cnt) %>%
    filter(date <= s_end_date)
  
  
  #### WEEKLY - MAKE AND SAVE COMBINED DATASET ####
  message("Bringing together and saving weekly data")

  ### Combine data
  nwkly <- bind_rows(
    # WEEKLY ALL AGE
    all_age_ed_weekly, all_age_hosp_weekly, #all_age_ed_uc_weekly,
    # WEEKLY ALL ADULT
    all_adult_ed_weekly, all_adult_hosp_weekly, #all_adult_ed_uc_weekly,
    # BY HOSPITAL
    all_age_ed_byhosp_weekly,
    # BY ZIP/REGION
    all_age_ed_byzip_weekly, all_age_hosp_byzip_weekly,
    # AGE SPECIFIC
    age_specific_ed_weekly, age_specific_hosp_weekly, #age_specific_ed_uc_weekly,
    # RACE/ETHNICITY SPECIFIC
    race_eth_specific_weekly
    )
  
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
    arrange(frequency, date, query, season, setting, age, race, ethnicity, cc_region, hospital) %>%
    group_by(frequency, query, season, setting, age, race, ethnicity, cc_region, hospital) %>%
    mutate(date_sort = row_number()) %>%
    ungroup()
  
  
  ### Write out data
  if (historical == F) {
    saveRDS(nwkly, file = file.path(output_path, "nwkly.RData"))
    write_csv(nwkly, file.path(output_path, "nwkly.csv"))
  } else if (historical == T & historical_current == T) {
    saveRDS(nwkly, file = file.path(output_path, "nwkly.RData"))
    write_csv(nwkly, file.path(output_path, "nwkly.csv"))
    
    saveRDS(nwkly, file = file.path(output_path, "wkly.RData"))
  } else if (historical == T & historical_current == F) {
    saveRDS(nwkly, file = file.path(output_path, "wkly.RData"))
  }
  
  
  #### WEEKLY - CLEAN UP ####
  rm(nwkly,
     all_age_ed_weekly, all_age_ed_uc_weekly, all_age_hosp_weekly,
     all_adult_ed_weekly, all_adult_ed_uc_weekly, all_adult_hosp_weekly,
     all_age_ed_byhosp_weekly, all_age_byzip_weekly, 
     age_specific_ed_weekly, age_specific_ed_uc_weekly, age_specific_hosp_weekly,
     race_eth_specific_weekly)
  
}

message("Alert code completed")
