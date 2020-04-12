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
  s_start_date <- format(as_date("2019-09-29"), "%d%b%Y")
} else {
  s_start_date <- format(as_date("2017-08-01"), "%d%b%Y") 
}

s_end_date <- format(today() - 1, "%d%b%Y")

### Set up folder to work in
setwd("//phshare01/cdi_share/Analytics and Informatics Team/Data Requests/2020/372_nCoV Essence Extract/From Natasha on March 13")


#### FUNCTIONS ####
### Use this querty to get record-level details for each syndrome
person_level_query <- function(user_id = 2769, frequency = c("weekly", "daily"), 
                         syndrome = c("none", "ili", "cli", "pneumonia"),
                         ed = F, inpatient = F) {
  
  frequency <- match.arg(frequency)
  syndrome <- match.arg(syndrome)
  
  if (syndrome == "ili") {
    category <- "&ccddCategory=ili%20ccdd%20v1"
    query <- "ili"
    condition <- "ili"
  } else if (syndrome == "cli") {
    category <- "&ccddCategory=fever%20and%20cough-sob-diffbr%20v1"
    query <- "fevcough"
    condition <- "cli"
  } else if (syndrome == "pneumonia") {
    category <- paste0("&dischargeDiagnosisApplyTo=subsyndromeFreeText&dischargeDiagnosis=", 
                       "%5Epneumonia%5E,or,%5E%5B;/%20%5DJ12.%5B89%5D%5E,or,%5E%5B;/%20%5DJ12%5B89%5D%5E,or,",
                       "%5E%5B;/%20%5DJ168%5E,or,%5E%5B;/%20%5DJ16.8%5E,or,%5E%5B;/%20%5DJ1%5B78%5D%5E,or,",
                       "%5E%5B;/%20%5DJ851%5E,or,%5E%5B;/%20%5DJ85.1%5E,or,%5E%5B;/%20%5DJ15.9%5E,or,",
                       "%5E%5B;/%20%5DJ159%5E,or,%5E%5B;/%20%5D233604007%5E,or,%5E%5B;/%20%5D385093006%5E,or,",
                       "%5E%5B;/%20%5D301000005%5E,or,%5E%5B;/%20%5D301001009%5E,or,%5E%5B;/%20%5D233606009%5E,or,",
                       "%5E%5B;/%20%5D407671000%5E,or,%5E%5B;/%20%5D301003007%5E,or,%5E%5B;/%20%5D75570004%5E,or,",
                       "%5E%5B;/%20%5D300999006%5E,or,%5E%5B;/%20%5D301002002%5E,or,%5E%5B;/%20%5D396285007%5E,or,",
                       "%5E%5B;/%20%5D426696003%5E,or,%5E%5B;/%20%5D312342009%5E,or,%5E%5B;/%20%5D53084003%5E,or,",
                       "%5E%5B;/%20%5D278516003%5E,or,%5E%5B;/%20%5D64667001%5E,or,%5E%5B;/%20%5D236302005%5E,or,",
                       "%5E%5B;/%20%5D196112005%5E,or,%5E%5B;/%20%5D471272001%5E,or,%5E%5B;/%20%5D7063008%5E,or,",
                       "%5E%5B;/%20%5D700250006%5E,or,%5E%5B;/%20%5D425996009%5E,or,%5E%5B;/%20%5D123590007%5E,or,",
                       "%5E%5B;/%20%5D44274007%5E,or,%5E%5B;/%20%5D68409003%5E,or,%5E%5B;/%20%5D55679008%5E,or,",
                       "%5E%5B;/%20%5D441590008%5E,or,%5E%5B;/%20%5D57702005%5E")
    query <- "ncovpneumo"
    condition <- "pneumonia"
  } else if (syndrome == "none") {
    category <- ""
    condition <- ""
  }
  
  # Catch all for filters
  if (ed == T) {
    filter <- "&hasBeenE=1"
    setting <- "ed"
  } else if (inpatient == T) {
    filter <- "&hasBeenE=1&hasBeenI=1"
    setting <- "hosp"
  } else {
    stop("Select only one of 'ED' and 'inpatient'")
  }
  
  # Catch all for percentParam types
  if (syndrome %in% c("ili", "cli", "none")) {
    percent <- "&percentParam=ccddCategory"
  } else if (syndrome %in% c("pneumonia")) {
    percent <- "&percentParam=dischargeDiagnosis"
  }
  
  # Catch all for visit types
  if (syndrome %in% c("none")) {
    visit_types <- "&hospFacilityType=emergency%20care&hospFacilityType=urgent%20care&hospFacilityType=primary%20care"
  } else if (syndrome %in% c("ili", "cli", "pneumonia")) {
    visit_types <- "&hospFacilityType=emergency%20care"
  }
  
  # Catch all for detector types
  if (syndrome %in% c("cli", "none")) {
    detector <- "&detector=nodetectordetector"
  } else if (syndrome %in% c("ili", "pneumonia")) {
    detector <- "&detector=c2"
  }
  
  url <- paste0("https://essence.syndromicsurveillance.org/nssp_essence/api/dataDetails?", 
                # Add in dates and geographies
                "endDate=", s_end_date, "&startDate=", s_start_date, 
                "&geography=wa_king&geographySystem=hospitalregion", 
                # Add in a few other fields including userID
                "&datasource=va_hosp&medicalGroupingSystem=essencesyndromes&userId=", user_id, 
                "&aqtTarget=DataDetails&refValues=true", 
                # Add in percent param, types of visits, frequency, detector
                percent, visit_types, "&timeResolution=", frequency, detector,
                # Add in rowFields
                "&field=age&field=ChiefComplaintParsed&field=DateTime&field=FacilityName&field=Zipcode",
                "&field=Sex&field=Date&field=HospitalName&field=Age&field=CCDD",
                # Add in syndrome and filter
                category, filter)
  
  data_load <- jsonlite::fromJSON(content(
    GET(url, authenticate(key_list("essence")[1, 2], 
                          key_get("essence", key_list("essence")[1, 2]))), as = "text"))
  
  df <- data_load$dataDetails
  
  # Add in details to the data frame
  df <- df %>%
    mutate(condition = condition,
           setting = setting)
  
  return(df)
}


### Use this query to return counts/percentages plus alerts
alert_query <- function(user_id = 520, frequency = c("weekly", "daily"), 
                          syndrome = c("ili", "cli", "pneumonia", "influenza"),
                          ed = F, inpatient = F, ed_uc = F,
                          age = c("None", "00-04", "05-17", "18-44", "45-64", "65-1000", "unknown"),
                          hospital = F, value = c("percent", "count")) {
  
  frequency <- match.arg(frequency)
  syndrome <- match.arg(syndrome)
  age <- match.arg(age)
  value <- match.arg(value)
  
  # Restrict to only one filter type
  # Need to check we never want more than one
  if (ed + inpatient + ed_uc == 0 | ed + inpatient + ed_uc > 1) {
    stop("Select only one of 'ED', 'inpatient', and 'ed_uc'")
  }
  
  if (syndrome == "ili") {
    category <- "&ccddCategory=ili%20ccdd%20v1"
    query <- "ili"
    syndrome_text <- "ILI"
  } else if (syndrome == "cli") {
    category <- "&ccddCategory=fever%20and%20cough-sob-diffbr%20v1"
    query <- "cli"
    syndrome_text <- "CLI"
  } else if (syndrome == "pneumonia") {
    category <- "&ccddCategory=cdc%20pneumonia%20ccdd%20v1"
    query <- "pneumo"
    syndrome_text <- "Pneumonia"
  } else if (syndrome == "influenza") {
    category <- "&ccddCategory=cdc%20influenza%20dd%20v1"
    query <- "fludg"
    syndrome_text <- "Influenza diagnosis"
  }
  
  # Catch all for detector types
  if (syndrome %in% c("influenza") & value == "percent") {
    detector <- "&detector=probrepswitch"
    percent <- "&percentParam=ccddCategory"
  } else if (syndrome %in% c("influenza") & value == "count") {
    detector <- "&detector=probrepswitch"
    percent <- "&percentParam=noPercent"
  } else if (syndrome %in% c("ili", "pneumonia", "cli") & value == "percent") {
    detector <- "&detector=c2"
    percent <- "&percentParam=ccddCategory"
  } else if (syndrome %in% c("ili", "pneumonia", "cli") & value == "count") {
    detector <- "&detector=probregv2"
    percent <- "&percentParam=noPercent"
  }
  
  # Catch all for visit types and other config setup
  if (ed_uc == F) {
    visit_type <- "&hospFacilityType=emergency%20care"
    config <- paste0("&stratVal=year&multiStratVal=&graphOnly=true&seriesPerYear=true&nonZeroComposite=false",
                     "&removeZeroSeries=true&startMonth=30&isPortlet=true&&year=&portletId=137975",
                     "&graphWidth=677&graphWidth=677&portletId=137975&dateconfig=2")
  } else if (ed_uc == T) {
    visit_type <- ""
    config <- paste0("&advVariableList=hospFacilityTypeData~~~~~~~NORMAL",
                     "&aqt=(%5BFACILITYTYPE=%22Emergency%20Care%22%5D%20AND%20%5BHASBEENEMERGENCY=%221%22%5D)%20OR",
                     "%20%5BFACILITYTYPE=%22Urgent%20Care%22%5D&isAQTAdmin=false&saveQueryText=",
                     "&operatorList==&ext-gen1017=07Mar2020&hospFacilityType=AQT&ext-gen1010=23Feb2020", # Always this date?
                     "&filterText=&savetype=unknown&filterType=S&groupOption=OR&advOptionList=Urgent%20Care&showList=false")
  }
  
  # Catch all for filters
  if (ed == T) {
    filter <- "&hasBeenE=1"
    setting <- "ed"
  } else if (inpatient == T) {
    filter <- "&hasBeenE=1&hasBeenI=1"
    setting <- "hosp"
  } else if (ed_uc == T) {
    filter <- "&hasBeenE=AQT"
    setting <- "educ"
  } else {
    stop("Select only one of 'ED', 'inpatient', and 'ed_uc'")
  }
  
  # Catch all for age
  if (age == "None") {
    age_grp <- ""
  } else {
    age_grp <- paste0("&age=", age)
  }
  
  # Set things up for hospitals
  if (hospital == T) {
    geog_system <- "hospital"
    geogs <- c("1255", "1297", "1298", "1247", "27090", "1252", "1269", "30509", 
               "1272", "1277", "1294", "1302", "1303", "1304", "1305", "1307", "1313")
    fields <- "&multiStratVal=geography&graphOptions=multipleSmall"
  } else {
    geog_system <- "hospitalregion"
    geogs <- c("wa_king")
    fields <- "&multiStratVal=hospitalGrouping&graphOptions=facetGrid"
  }
  
  
  output <- bind_rows(lapply(geogs, function(x) {
    url <- paste0("https://essence.syndromicsurveillance.org/nssp_essence/api/timeSeries?", 
                  # Add in dates and geographies
                  "endDate=", s_end_date, "&startDate=", s_start_date, 
                  "&geographySystem=", geog_system, "&geography=", x,
                  # Add in a few other fields including userID
                  "&datasource=va_hosp&medicalGroupingSystem=essencesyndromes&userId=", user_id, 
                  "&aqtTarget=TimeSeries", 
                  # Add in percent param, types of visits, frequency, detector
                  percent, visit_type, "&timeResolution=", frequency, detector,
                  # Add in other config fields
                  config,
                  # Add in syndrome, filter, and age
                  category, filter, age_grp,
                  # Add in hospital grouping
                  "&stratVal=&graphOnly=true&numSeries=0", fields,
                  "&seriesPerYear=false&nonZeroComposite=false&removeZeroSeries=true&startMonth=January"
    )
    
    data_load <- jsonlite::fromJSON(content(
      GET(url, authenticate(key_list("essence")[1, 2], 
                            key_get("essence", key_list("essence")[1, 2]))), as = "text"))
    
    df <- data_load$timeSeriesData
    
    if (value == "percent") {
      df <- df %>% rename(pct = count, expected_pct = expected, levels_pct = levels, 
                          colorID_pct = colorID, color_pct = color) %>%
        mutate_at(vars(expected_pct, levels_pct), list(~ as.numeric(.)))
    } else if (value == "count") {
      df <- df %>% rename(cnt = count, expected_cnt = expected, levels_cnt = levels, 
                          colorID_cnt = colorID, color_cnt = color) %>%
        mutate_at(vars(expected_cnt, levels_cnt), list(~ as.numeric(.)))
    }
    
    # Add in specifics for this data run
    df <- df %>%
      mutate(date = ymd(date),
             age = case_when(age == "None" ~ "all age",
                             age == "00-04" ~ "0-4",
                             age == "05-17" ~ "5-17",
                             age == "18-44" ~ "18-44",
                             age == "45-64" ~ "45-64",
                             age == "65-1000" ~ "65+",
                             age == "unknown" ~ "Unk"),
             setting = setting,
             query = query,
             syndrome = syndrome_text) %>%
      # Remove specifics of query to save space
      # Also remove percent/count because it is calculated in the main code
      select(-details, -altText)
    
    if (hospital == F) {
      df <- df %>% mutate(hospital = "all")
    } else {
      df <- df %>% mutate(hospital = case_when(
        x == 1255 ~ "CHI-FHS Highline Medical Center",
        x == 1297 ~ "CHI-FHS St. Elizabeth Hospital",
        x == 1298 ~ "CHI-FHS St. Francis Hospital",
        x == 1247 ~ "EvergreenHealth Kirkland",
        x == 27090 ~ "EvergreenHealth Redmond ED",
        x == 1252 ~ "Harborview Medical Center",
        x == 1269 ~ "MultiCare Auburn Medical Center",
        x == 30509 ~ "MultiCare Covington Medical Center",
        x == 1272 ~ "Northwest Hospital and Medical Center",
        x == 1277 ~ "Overlake Hospital Medical Center",
        x == 1294 ~ "Snoqualmie Valley Hospital",
        x == 1302 ~ "Swedish Medical Center - Ballard",
        x == 1303 ~ "Swedish Medical Center - Cherry Hill",
        x == 1304 ~ "Swedish Medical Center - First Hill",
        x == 1305 ~ "Swedish Medical Center - Issaquah",
        x == 1307 ~ "Swedish Medical Center - Redmond",
        x == 1313 ~ "University of Washington Medical Center"
      ))
    }
    
    df
  }))
  
  return(output)
}



#### DAILY RUN ####
#### PERSON-LEVEL DATA ####
if (historical == F) {
  message("Running person-level section")
  # Daily pneumonia person-level data - ED visits
  pdly_full_pneumo_ed <- person_level_query(frequency = "daily", syndrome = "pneumonia", ed = T)
  write_csv(pdly_full_pneumo_ed, "pdly-pneumonia.csv")
  
  # Daily Pneumonia person-level data - hospitalizations
  pdly_full_pneumo_hosp <- person_level_query(frequency = "daily", syndrome = "pneumonia", inpatient = T)
  write_csv(pdly_full_pneumo_hosp, "pdly-pneumonia-hosp.csv")
  
  # Daily ILI person-level data - ED visits
  pdly_full_ili_ed <- person_level_query(frequency = "daily", syndrome = "ili", ed = T)
  write_csv(pdly_full_ili_ed, "pdly-ILI.csv")
  
  # Daily ILI person-level data - hospitalalizations
  pdly_full_ili_hosp <- person_level_query(frequency = "daily", syndrome = "ili", inpatient = T)
  write_csv(pdly_full_ili_hosp, "pdly-ILI-hosp.csv")
  
  # Daily CLI person-level data - ED Visits 
  pdly_full_cli_ed <- person_level_query(frequency = "daily", syndrome = "cli", ed = T)
  write_csv(pdly_full_cli_ed, "pdly-CLI.csv")
  
  # Daily CLI person-level data - hospitalalizations
  pdly_full_cli_hosp <- person_level_query(frequency = "daily", syndrome = "cli", inpatient = T)
  write_csv(pdly_full_cli_hosp, "pdly-CLI-hosp.csv")
}



#### DAILY - ALL AGE ####
message("Running daily all-ages section")

all_age_ed_daily <- bind_rows(lapply(c("pneumonia", "ili", "cli", "influenza"), function(x) {
  # Run both queries
  pct <- alert_query(frequency = "daily", syndrome = x, ed = T, hospital = F, value = "percent")
  cnt <- alert_query(frequency = "daily", syndrome = x, ed = T, hospital = F, value = "count")
  # Bring together
  output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
  return(output)
}))

all_age_ed_uc_daily <- bind_rows(lapply(c("pneumonia", "ili", "cli"), function(x) {
  # Run both queries
  pct <- alert_query(frequency = "daily", syndrome = x, ed_uc = T, hospital = F, value = "percent")
  cnt <- alert_query(frequency = "daily", syndrome = x, ed_uc = T, hospital = F, value = "count")
  # Bring together
  output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
  return(output)
}))

all_age_hosp_daily <- bind_rows(lapply(c("pneumonia", "ili", "cli", "influenza"), function(x) {
  # Run both queries
  pct <- alert_query(frequency = "daily", syndrome = x, inpatient = T, hospital = F, value = "percent")
  cnt <- alert_query(frequency = "daily", syndrome = x, inpatient = T, hospital = F, value = "count")
  # Bring together
  output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
  return(output)
}))


#### DAILY - BY HOSPITAL ####
all_age_ed_byhosp_daily <- bind_rows(lapply(c("cli"), function(x) {
  # Run both queries
  pct <- alert_query(frequency = "daily", syndrome = x, ed = T, hospital = T, value = "percent")
  cnt <- alert_query(frequency = "daily", syndrome = x, ed = T, hospital = T, value = "count")
  # Bring together
  output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
  return(output)
}))


#### DAILY - AGE SPECIFIC ####
message("Running daily age-specific section")

### ED visits
age_specific_ed_daily <- bind_rows(lapply(c("pneumonia", "ili", "cli"), function(x) {
  # Run queries
  pct04 <- alert_query(frequency = "daily", syndrome = x, ed = T, age = "00-04", hospital = F, value = "percent")
  cnt04 <- alert_query(frequency = "daily", syndrome = x, ed = T, age = "00-04", hospital = F, value = "count")
  pct517 <- alert_query(frequency = "daily", syndrome = x, ed = T, age = "05-17", hospital = F, value = "percent")
  cnt517 <- alert_query(frequency = "daily", syndrome = x, ed = T, age = "05-17", hospital = F, value = "count")
  pct1844 <- alert_query(frequency = "daily", syndrome = x, ed = T, age = "18-44", hospital = F, value = "percent")
  cnt1844 <- alert_query(frequency = "daily", syndrome = x, ed = T, age = "18-44", hospital = F, value = "count")
  pct4564 <- alert_query(frequency = "daily", syndrome = x, ed = T, age = "45-64", hospital = F, value = "percent")
  cnt4564 <- alert_query(frequency = "daily", syndrome = x, ed = T, age = "45-64", hospital = F, value = "count")
  pct65 <- alert_query(frequency = "daily", syndrome = x, ed = T, age = "65-1000", hospital = F, value = "percent")
  cnt65 <- alert_query(frequency = "daily", syndrome = x, ed = T, age = "65-1000", hospital = F, value = "count")
  pctunk <- alert_query(frequency = "daily", syndrome = x, ed = T, age = "unknown", hospital = F, value = "percent")
  cntunk <- alert_query(frequency = "daily", syndrome = x, ed = T, age = "unknown", hospital = F, value = "count")
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
  pct04 <- alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "00-04", hospital = F, value = "percent")
  cnt04 <- alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "00-04", hospital = F, value = "count")
  pct517 <- alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "05-17", hospital = F, value = "percent")
  cnt517 <- alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "05-17", hospital = F, value = "count")
  pct1844 <- alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "18-44", hospital = F, value = "percent")
  cnt1844 <- alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "18-44", hospital = F, value = "count")
  pct4564 <- alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "45-64", hospital = F, value = "percent")
  cnt4564 <- alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "45-64", hospital = F, value = "count")
  pct65 <- alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "65-1000", hospital = F, value = "percent")
  cnt65 <- alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "65-1000", hospital = F, value = "count")
  pctunk <- alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "unknown", hospital = F, value = "percent")
  cntunk <- alert_query(frequency = "daily", syndrome = x, ed_uc = T, age = "unknown", hospital = F, value = "count")
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
  pct04 <- alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "00-04", hospital = F, value = "percent")
  cnt04 <- alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "00-04", hospital = F, value = "count")
  pct517 <- alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "05-17", hospital = F, value = "percent")
  cnt517 <- alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "05-17", hospital = F, value = "count")
  pct1844 <- alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "18-44", hospital = F, value = "percent")
  cnt1844 <- alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "18-44", hospital = F, value = "count")
  pct4564 <- alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "45-64", hospital = F, value = "percent")
  cnt4564 <- alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "45-64", hospital = F, value = "count")
  pct65 <- alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "65-1000", hospital = F, value = "percent")
  cnt65 <- alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "65-1000", hospital = F, value = "count")
  pctunk <- alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "unknown", hospital = F, value = "percent")
  cntunk <- alert_query(frequency = "daily", syndrome = x, inpatient = T, age = "unknown", hospital = F, value = "count")
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
if (historical == F) {
  ### Load existing data and remove overlapping weeks
  dly <- readRDS("dly.RData")
  dly <- dly %>% filter(date < dmy(s_start_date))
  
  ndly <- bind_rows(
    # EXISTING DATA
    dly,
    # DAILY ALL AGE
    all_age_ed_daily, all_age_ed_uc_daily, all_age_hosp_daily,
    # BY HOSPITAL
    all_age_ed_byhosp_daily,
    # AGE SPECIFIC
    age_specific_ed_daily, age_specific_ed_uc_daily, age_specific_hosp_daily)
} else {
  dly <- bind_rows(
    # DAILY ALL AGE
    all_age_ed_daily, all_age_ed_uc_daily, all_age_hosp_daily,
    # BY HOSPITAL
    all_age_ed_byhosp_daily,
    # AGE SPECIFIC
    age_specific_ed_daily, age_specific_ed_uc_daily, age_specific_hosp_daily)
}


### Pull out dates and convert to get weeks
dly_date <- sort(unique(dly$date))
dly_date <- cbind(dly_date, MMWRweek(dly_date))


### Finish making new variables
dly <- left_join(dly, dly_date, by = c("date" = "dly_date")) %>%
  rename(year = MMWRyear, week = MMWRweek, day = MMWRday) %>%
  mutate(frequency = "daily",
         season = case_when(
           year == 2017 & week >= 30 ~ "2017-2018",
           year == 2018 & week < 30 ~ "2017-2018",
           year == 2018 & week >= 30 ~ "2018-2019",
           year == 2019 & week < 30 ~ "2018-2019",
           year == 2019 & week >= 30 ~ "2019-2020",
           year == 2020 & week < 30 ~ "2019-2020"),
         week = as.factor(week),
         week = factor(week, levels(week)[c(30:52, 1:29)], ordered = TRUE),
         essence_refresh_date = today()) %>%
  filter(!is.na(season)) %>%
  mutate_at(vars(pct, expected_pct, levels_pct, colorID_pct,
                 cnt, expected_cnt, levels_cnt, colorID_cnt),
            list(~ as.numeric(.)))


### Write out data
if (historical == F) {
  saveRDS(ndly, file = "ndly.RData")
  write_csv(ndly, "ndly.csv")
} else {
  saveRDS(dly, file = "dly.RData")
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
    pct <- alert_query(frequency = "weekly", syndrome = x, ed = T, hospital = F, value = "percent")
    cnt <- alert_query(frequency = "weekly", syndrome = x, ed = T, hospital = F, value = "count")
    # Bring together
    output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
    return(output)
  }))
  
  all_age_ed_uc_weekly <- bind_rows(lapply(c("pneumonia", "ili", "cli"), function(x) {
    # Run both queries
    pct <- alert_query(frequency = "weekly", syndrome = x, ed_uc = T, hospital = F, value = "percent")
    cnt <- alert_query(frequency = "weekly", syndrome = x, ed_uc = T, hospital = F, value = "count")
    # Bring together
    output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
    return(output)
  }))
  
  all_age_hosp_weekly <- bind_rows(lapply(c("pneumonia", "ili", "cli", "influenza"), function(x) {
    # Run both queries
    pct <- alert_query(frequency = "weekly", syndrome = x, inpatient = T, hospital = F, value = "percent")
    cnt <- alert_query(frequency = "weekly", syndrome = x, inpatient = T, hospital = F, value = "count")
    # Bring together
    output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
    return(output)
  }))
  
  
  #### WEEKLY - BY HOSPITAL ####
  all_age_ed_byhosp_weekly <- bind_rows(lapply(c("cli"), function(x) {
    # Run both queries
    pct <- alert_query(frequency = "weekly", syndrome = x, ed = T, hospital = T, value = "percent")
    cnt <- alert_query(frequency = "weekly", syndrome = x, ed = T, hospital = T, value = "count")
    # Bring together
    output <- left_join(pct, cnt, by = c("date", "age", "setting", "query", "syndrome", "hospital"))
    return(output)
  }))
  
  
  #### WEEKLY - AGE SPECIFIC ####
  message("Running weekly age-specific section")
  
  ### ED visits
  age_specific_ed_weekly <- bind_rows(lapply(c("pneumonia", "ili", "cli"), function(x) {
    # Run queries
    pct04 <- alert_query(frequency = "weekly", syndrome = x, ed = T, age = "00-04", hospital = F, value = "percent")
    cnt04 <- alert_query(frequency = "weekly", syndrome = x, ed = T, age = "00-04", hospital = F, value = "count")
    pct517 <- alert_query(frequency = "weekly", syndrome = x, ed = T, age = "05-17", hospital = F, value = "percent")
    cnt517 <- alert_query(frequency = "weekly", syndrome = x, ed = T, age = "05-17", hospital = F, value = "count")
    pct1844 <- alert_query(frequency = "weekly", syndrome = x, ed = T, age = "18-44", hospital = F, value = "percent")
    cnt1844 <- alert_query(frequency = "weekly", syndrome = x, ed = T, age = "18-44", hospital = F, value = "count")
    pct4564 <- alert_query(frequency = "weekly", syndrome = x, ed = T, age = "45-64", hospital = F, value = "percent")
    cnt4564 <- alert_query(frequency = "weekly", syndrome = x, ed = T, age = "45-64", hospital = F, value = "count")
    pct65 <- alert_query(frequency = "weekly", syndrome = x, ed = T, age = "65-1000", hospital = F, value = "percent")
    cnt65 <- alert_query(frequency = "weekly", syndrome = x, ed = T, age = "65-1000", hospital = F, value = "count")
    pctunk <- alert_query(frequency = "weekly", syndrome = x, ed = T, age = "unknown", hospital = F, value = "percent")
    cntunk <- alert_query(frequency = "weekly", syndrome = x, ed = T, age = "unknown", hospital = F, value = "count")
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
    pct04 <- alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "00-04", hospital = F, value = "percent")
    cnt04 <- alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "00-04", hospital = F, value = "count")
    pct517 <- alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "05-17", hospital = F, value = "percent")
    cnt517 <- alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "05-17", hospital = F, value = "count")
    pct1844 <- alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "18-44", hospital = F, value = "percent")
    cnt1844 <- alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "18-44", hospital = F, value = "count")
    pct4564 <- alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "45-64", hospital = F, value = "percent")
    cnt4564 <- alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "45-64", hospital = F, value = "count")
    pct65 <- alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "65-1000", hospital = F, value = "percent")
    cnt65 <- alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "65-1000", hospital = F, value = "count")
    pctunk <- alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "unknown", hospital = F, value = "percent")
    cntunk <- alert_query(frequency = "weekly", syndrome = x, ed_uc = T, age = "unknown", hospital = F, value = "count")
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
    pct04 <- alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "00-04", hospital = F, value = "percent")
    cnt04 <- alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "00-04", hospital = F, value = "count")
    pct517 <- alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "05-17", hospital = F, value = "percent")
    cnt517 <- alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "05-17", hospital = F, value = "count")
    pct1844 <- alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "18-44", hospital = F, value = "percent")
    cnt1844 <- alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "18-44", hospital = F, value = "count")
    pct4564 <- alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "45-64", hospital = F, value = "percent")
    cnt4564 <- alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "45-64", hospital = F, value = "count")
    pct65 <- alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "65-1000", hospital = F, value = "percent")
    cnt65 <- alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "65-1000", hospital = F, value = "count")
    pctunk <- alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "unknown", hospital = F, value = "percent")
    cntunk <- alert_query(frequency = "weekly", syndrome = x, inpatient = T, age = "unknown", hospital = F, value = "count")
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
  if (historical == F) {
    ### Load existing data and remove overlapping weeks
    wkly <- readRDS("wkly.RData")
    wkly <- wkly %>% filter(date < dmy(s_start_date))

    nwkly <- bind_rows(
      # EXISTING DATA
      wkly,
      # WEEKLY ALL AGE
      all_age_ed_weekly, all_age_ed_uc_weekly, all_age_hosp_weekly,
      # BY HOSPITAL
      all_age_ed_byhosp_weekly,
      # AGE SPECIFIC
      age_specific_ed_weekly, age_specific_ed_uc_weekly, age_specific_hosp_weekly)
  } else {
    wkly <- bind_rows(
      # WEEKLY ALL AGE
      all_age_ed_weekly, all_age_ed_uc_weekly, all_age_hosp_weekly,
      # BY HOSPITAL
      all_age_ed_byhosp_weekly,
      # AGE SPECIFIC
      age_specific_ed_weekly, age_specific_ed_uc_weekly, age_specific_hosp_weekly)
  }
  
  
  ### Pull out dates and convert to get weeks
  wkly_date <- sort(unique(wkly$date))
  wkly_date <- cbind(wkly_date, MMWRweek(wkly_date))
  
  
  ### Finish making new variables
  wkly <- left_join(wkly, wkly_date, by = c("date" = "wkly_date")) %>%
    rename(year = MMWRyear, week = MMWRweek, day = MMWRday) %>%
    mutate(frequency = "weekly",
           season = case_when(
             year == 2017 & week >= 30 ~ "2017-2018",
             year == 2018 & week < 30 ~ "2017-2018",
             year == 2018 & week >= 30 ~ "2018-2019",
             year == 2019 & week < 30 ~ "2018-2019",
             year == 2019 & week >= 30 ~ "2019-2020",
             year == 2020 & week < 30 ~ "2019-2020"),
           week = as.factor(week),
           week = factor(week, levels(week)[c(30:52, 1:29)], ordered = TRUE),
           essence_refresh_date = today()) %>%
    filter(!is.na(season)) %>%
    mutate_at(vars(pct, expected_pct, levels_pct, colorID_pct,
                   cnt, expected_cnt, levels_cnt, colorID_cnt),
              list(~ as.numeric(.)))
  

  
  if (historical == F) {
    saveRDS(nwkly, file = "nwkly.RData")
    write_csv(nwkly, "nwkly.csv")
  } else {
    saveRDS(wkly, file = "wkly.RData")
  }
  
  
  #### WEEKLY - CLEAN UP ####
  rm(nwkly,
     all_age_ed_weekly, all_age_ed_byhosp_weekly, all_age_ed_uc_weekly, all_age_hosp_weekly,
     age_specific_ed_weekly, age_specific_ed_uc_weekly, age_specific_hosp_weekly)
  
}

message("Alert code completed")