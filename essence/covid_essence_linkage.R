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
               jsonlite, keyring, httr, kableExtra, knitr, rmarkdown)

httr::set_config(httr::config(ssl_verifypeer = 0L))

### Set up folder to work in
shared_path <- "//phshare01/cdi_share/Analytics and Informatics Team/Data Requests/2020/COVID RHINO match"


#### FUNCTIONS ####
# Call in from Github repo
source("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/essence/essence_query_functions.R")


#### BRING IN CASES TO MATCH ####
covid_orig <- read.xlsx(file.path(shared_path, "rhino_match.xlsx"))
covid_orig <- covid_orig %>% distinct() %>% # ~100 duplicates
  rename(case_id = CASE_ID, rhino_id = COL1) %>%
  # Note how many RHINO events per case
  group_by(case_id) %>%
  mutate(encounters = n()) %>%
  ungroup()



#### RETRIEVE CLINICAL DATA ####
case_data <- bind_rows(event_query_bulk(bulk_id = covid_orig$rhino_id))


#### RECODE DATA ####
# Bring in recode files
recodes <- read_csv(file.path(shared_path, "essence_recodes.csv"))

# Race/ethnicity
# Height
# Weight
# BMI
# Smoking
# Pregnancy
# Age group

# Setting/facility type
# Intubated
# Discharge status (died)
# Time to discharge


case_data_recodes <- left_join(covid_orig, case_data, by = "rhino_id") %>%
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
         )



#### SET UP REPORTS ####
# See how many matched overall
matched <- case_data_recodes %>% summarise(encounters = n(), people = n_distinct(case_id))

### Set up the first and most recent clinical encounter per person
first <- case_data_recodes %>%
  arrange(case_id, Date) %>%
  group_by(case_id) %>%
  slice(1) %>%
  ungroup()

last <- case_data_recodes %>%
  arrange(case_id, desc(Date)) %>%
  group_by(case_id) %>%
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


# Combine into one data frame
demogs_tot <- bind_rows(age, race, ethn, sex, obese, obese_severe, smoker_current) %>%
  mutate(Number = paste0(format(count, big.mark = ','), " (", pct, "%)")) %>%
  select(Demographic, Group, Number)


#### Clinical features ####
stay_length <- case_data_recodes %>% filter(!is.na(time_discharge_cat)) %>% 
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
