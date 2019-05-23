###########################################
# Code to analyze the WA infant mortality data
# for use in Best Starts for Kids
#
# Alastair Matheson
# Public Health Seattle & King County
#
# December 2016
###########################################

# Aim is to produce the following:
#  1) UnivaProportion breakdown of the indicator by age, race, place, SES
#  2) Time trends by race and region (need to incorporate Joinpoint output still)
#  3) Bivariate breakdown of the indicator (e.g., age by race)

#### Areas for future development/improvement ####
# Try other libraries to make code more efficient:
#   - purrr to apply functions over lists of variables
#   - TTR for rolling averages
# Use apply functions to avoid outer loops (may still need inner loops for small group crosstabs)
# Incorporate a call to JoinPoint into R rather than running it later on the generated file


#### Call in libraries ####
# Use install.packages("Package name") if you do not already have these libraries
library(RODBC) # Used to connect to the SQL database
library(lazyeval) # Makes functions work as expected
library(car) # used to recode variables
library(dplyr) # Used to manipulate data
library(data.table) # more data manipulation
library(dtplyr) # lets dplyr and data.table play nicely together
library(RcppRoll) # calculates rolling averages
library(openxlsx) # Writes to Excel


#### Set up ODBC connection to SQL data ####
db.apde50 <- odbcConnect("PH_APDEStore")


#### Bring in each year's data and combine ####
im03 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2003.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -97, 1, -407, 2, -53, 4, -24, 2, -2, 2, -63, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", "educ_mom",
                               "hisp_mom", "moracsum", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im04 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2004.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -97, 1, -407, 2, -53, 4, -24, 2, -2, 2, -63, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", "educ_mom",
                               "hisp_mom", "moracsum", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im05 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2005.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -97, 1, -407, 2, -53, 4, -24, 2, -2, 2, -63, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", "educ_mom",
                               "hisp_mom", "moracsum", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im06 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2006.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -97, 1, -407, 2, -53, 4, -24, 2, -2, 2, -63, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", "educ_mom",
                               "hisp_mom", "moracsum", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im07 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2007.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -97, 1, -407, 2, -53, 4, -24, 2, -2, 2, -63, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", "educ_mom",
                               "hisp_mom", "moracsum", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im08 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2008.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -97, 1, -407, 2, -53, 4, -24, 2, -2, 2, -63, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", "educ_mom",
                               "hisp_mom", "moracsum", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im09 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2009.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -97, 1, -407, 2, -53, 4, -24, 2, -2, 2, -63, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", "educ_mom",
                               "hisp_mom", "moracsum", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im10 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2010.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -97, 1, -407, 2, -53, 4, -24, 2, -2, 2, -63, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", "educ_mom",
                               "hisp_mom", "moracsum", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im11 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2011.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -97, 1, -407, 2, -53, 4, -24, 2, -2, 2, -63, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", "educ_mom",
                               "hisp_mom", "moracsum", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im12 <- read.csv(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/Infant2012.csv",
                   header = TRUE)
im12 <- select(im12, certno_e, dob_yr, age_mom, cnty_res, race_mom, educ_mom, hisp_mom, moracsum, dth_yr, dbstate, dcntyres, dst_res)
im13 <- read.csv(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/InfantDeathF2013.csv",
                 header = TRUE)
im13 <- select(im13, certno_e, dob_yr, age_mom, cnty_res, race_mom, educ_mom, hisp_mom, moracsum, dth_yr, dbstate, dcntyres, dst_res)
im14 <- read.csv(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/InfantDeathF2014.csv",
                 header = TRUE)
im14 <- select(im14, certno_e, dob_yr, age_mom, cnty_res, race_mom, educ_mom, hisp_mom, moracsum, dth_yr, dbstate, dcntyres, dst_res)
im15 <- read.csv(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/InfantDeathF2015.csv",
                 header = TRUE)
im15 <- select(im15, certno_e, dob_yr, age_mom, cnty_res, race_mom, educ_mom, hisp_mom, moracsum, dth_yr, dbstate, dcntyres, dst_res)


im03_15 <- bind_rows(im03, im04, im05, im06, im07, im08, im09, im10, im11, im12, im13, im14, im15)
rm(im03, im04, im05, im06, im07, im08, im09, im10, im11, im12, im13, im14, im15)


#### Bring in births_denom data for denominator ####
births_denom <-
  sqlQuery(
    db.apde50,
    "SELECT certno_e, dob_yr, age_mom, cnty_res, race_mom, educ_mom, hisp_mom, moracsum
    FROM dbo.wabir2003_2015
    ",
    stringsAsFactors = FALSE
  )


#### Bring in geo data and merge ####
birth_geo <-
  sqlQuery(
    db.apde50,
    "SELECT *
    FROM zztest.geobir1990_2015 g1
    LEFT JOIN dbo.geocomp_blk10 g2
    ON g1.geo_id_blk10 = g2.geo_id_blk10
    ",
    stringsAsFactors = FALSE
  )

im03_15 <- left_join(im03_15, birth_geo, by = "certno_e")

births_denom <- left_join(births_denom, birth_geo, by = "certno_e")


#### Backup data to avoid rereading ####
im03_15.bk <- im03_15
births_denom.bk <- births_denom


#### Restrict dataset to King County ####
im03_15 <- filter(im03_15, dcntyres == 17)
births_denom <- filter(births_denom, cnty_res == 17)


#### Make new variables and do recodes ####
# Clean up missing data or older coding
im03_15$educ_mom <- as.character(car::recode(im03_15$educ_mom, "11 = 2; 12 = 3; 15 = 4; 16 = 6; 99 = NA"))

# Mother's age groupings
im03_15$age_mom_grp <- as.character(car::recode(im03_15$age_mom, "10:17 = 1; 18:24 = 2; 25:34 = 3;
                            35:44 = 4; 45:hi = 5; else = NA"))

births_denom$age_mom_grp <- as.character(car::recode(births_denom$age_mom, "10:17 = 1; 18:24 = 2; 25:34 = 3;
                            35:44 = 4; 45:hi = 5; else = NA"))

# Mother's race/ethnicity
im03_15 <- im03_15 %>%
  mutate(moracsum = as.character(ifelse(!is.na(moracsum) & moracsum >= 20 & moracsum < 99, 20, 
                           ifelse(moracsum %in% c(0, 99), NA, moracsum))),
         hisp_mom = as.character(ifelse(!is.na(hisp_mom) & hisp_mom %in% c(1, 2, 3, 4, 5), 1, 
                           ifelse(hisp_mom == 9, NA, hisp_mom)))
  )

births_denom <- births_denom %>%
  mutate(moracsum = as.character(ifelse(!is.na(moracsum) & moracsum >= 20 & moracsum < 99, 20, 
                           ifelse(moracsum %in% c(0, 99), NA, moracsum))),
         hisp_mom = as.character(ifelse(!is.na(hisp_mom) & hisp_mom %in% c(1, 2, 3, 4, 5), 1, 
                           ifelse(hisp_mom == 9, NA, hisp_mom)))
  )

# Mother's grouped race/ethnicity
im03_15$race_mom_grp <- as.character(car::recode(im03_15$race_mom, "'1' = 1; '2' = 2; 
                                   '3' = 3; c('4', '5', '7', 'D', 'E', 'G') = 4;
                                   c('A', 'F', 'H') = 5; 'C' = 6;
                                   'B' = 7; '6' = 8; c('8', '9') = 9"))

births_denom$race_mom_grp <- as.character(car::recode(births_denom$race_mom, "'1' = 1; '2' = 2; 
                                   '3' = 3; c('4', '5', '7', 'D', 'E', 'G') = 4;
                                                 c('A', 'F', 'H') = 5; 'C' = 6;
                                                 'B' = 7; '6' = 8; c('8', '9') = 9"))

# Removes whitespace from HRA and region names (also make character for merging later)
im03_15$hra_id <- trimws(im03_15$hra_id)
im03_15$rgn_name <- trimws(im03_15$rgn_name)
im03_15$rgn_id <- as.character(im03_15$rgn_id)

births_denom$hra_id <- trimws(births_denom$hra_id)
births_denom$rgn_name <- trimws(births_denom$rgn_name)
births_denom$rgn_id <- as.character(births_denom$rgn_id)

im03_15$educ_mom <- as.character(im03_15$educ_mom)
births_denom$educ_mom <- as.character(births_denom$educ_mom)

#### Set up other parameters ####
minyr_im = min(im03_15$dth_yr) # pulls out oldest year for which data are present
maxyr_im = max(im03_15$dth_yr) # pulls out most recent year for which data are present

# Folder to write output to
filepath = "S:/WORK/Best Start for Kids/Dashboard/Interim dashboard materials/Tableau docs/"


#### Set up labels for each variable ####
# Make a data frame that can merge with created tables to label values
labels <- rbind.data.frame(
  cbind(Category1 = "King County", Group = "King County", Label = "King County"),
  cbind(Category1 = "Overall", Group = "Overall", Label = "Overall"),
  cbind(Category1 = c(rep("Mother's race/ethnicity (summary)", times = 8)),
        Group = c("0", "1", "10", "11", "12", "13", "14", "20"),
        Label = c("Non-Hispanic", "Hispanic", "White", "Black", "American Indian/Alaskan Native", 
                  "Asian", "Native Hawaiian/Pacific Islander", "Multiple")),
  cbind(Category1 = c(rep("Mother's race/ethnicity", times = 17)),
                Group = c("1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H"),
                  Label = c("White", "Black", "American Indian/Alaskan Native", "Chinese", "Japanese", "Other Non-White", 
                    "Filipino", "Refused to State", "Unknown/Not Stated", "Hawaiian", 
                    "Other Asian/Pacific Islander", "Hispanic", "Asian Indian",
                    "Korean", "Samoan", "Vietnamese", "Guamanian")),
  cbind(Category1 = c(rep("Mother's race/ethnicity (grouped)", times = 9)),
        Group = c("1", "2", "3", "4", "5", "6", "7", "8", "9"),
        Label = c("White", "Black", "American Indian/Alaskan Native", "Asian", "Pacific Islander",
                  "Hispanic", "Other Asian/Pacific Islander",
                  "Other Non-White", "Unknown/refused to state")),
  cbind(Category1 = c(rep("Mother's education", times = 9)),
        Group = c("0", "1", "2", "3", "4", "5", "6", "7", "8"),
        Label = c("Unknown", "<= 8th grade", "9th-12th grade, no diploma",
                  "High school graduate/GED", "Some college, no degree",
                  "Associate degree", "Bachelor’s degree", "Master’s degree",
                  "Doctorate/professional degree")),
  cbind(Category1 = c(rep("Mother's age", times = 5)),
        Group = c("1", "2", "3", "4", "5"),
        Label = c("10–17", "18–24", "25–34", "35–44", "45+")),
  cbind(Category1 = c(rep("HRA", times = 48)),
        Group = c("1000", "1100", "2000", "2100", "2200", "2300", "2400", "2500", "2550", 
                  "2600", "2700", "2800", "2900", "2950", "2960", "3000", "4000", "4100", 
                  "4200", "4300", "5000", "6000", "7000", "8000", "9000", "10000", "10100", 
                  "10200", "11000", "12000", "13000", "14000", "14100", "14200", "15000", 
                  "15100", "16000", "17000", "18000", "19000", "20000", "20100", "20200",
                  "21000", "22000", "23000", "24000", "25000"),
        Label = c("Auburn-North", "Auburn-South", "Ballard", "Beacon/Gtown/S.Park", "Capitol Hill/E.lake", 
                  "Central Seattle", "Delridge", "Downtown", "Fremont/Greenlake", "NE Seattle", "North Seattle", 
                  "NW Seattle", "QA/Magnolia", "SE Seattle", "West Seattle", "Bear Creek/Carnation/Duvall",
                  "Bellevue-Central", "Bellevue-NE", "Bellevue-South", "Bellevue-West", 
                  "Black Diamond/Enumclaw/SE County", "Bothell/Woodinville", "Burien", "Covington/Maple Valley",
                  "Des Moines/Normandy Park", "East Federal Way", "Fed Way-Central/Military Rd",
                  "Fed Way-Dash Point/Woodmont", "Fairwood", "Issaquah", "Kenmore/LFP", "Kent-East", "Kent-SE",
                  "Kent-West", "Kirkland", "Kirkland North", "Mercer Isle/Pt Cities", "Newcastle/Four Creeks",
                  "North Highline", "Redmond", "Renton-East", "Renton-North", "Renton-South", "Sammamish", 
                  "SeaTac/Tukwila", "Shoreline", "Snoqualmie/North Bend/Skykomish", "Vashon Island")),
  cbind(Category1 = c(rep("Region", times = 4)),
        Group = c("11153", "21151", "31152", "41154"),
        Label = c("East", "North", "Seattle", "South"))
)
# Change to character to avoid warning message about factors
labels <- mutate(labels,
                 Category1 = as.character(Category1),
                 Group = as.character(Group),
                 Label = as.character(Label))


#### Summarize birth data for denominators ####
f_denom_kc <-
  function(data, yrstart = minyr_im, yrend = maxyr_im, yrcombine = 5, trend = FALSE) {
    # need to add check that # years combined <= years between min and max years selected
    
    if (trend == TRUE) {
      data %>%
        filter_(~dob_yr >= yrstart) %>%
        filter_(~dob_yr <= yrend) %>%
        group_by(dob_yr) %>%
        summarise(`Sample Size` = n()) %>%
        mutate(
          Category1 = "King County",
          Group = "King County",
          Category2 = "King County",
          Subgroup = "King County"
        ) %>%
        rename(Year = dob_yr) %>%
        select(Year, Category1, Group, Category2, Subgroup, `Sample Size`) -> denom_kc
      return(denom_kc)
    }
    
    else {
      # Make range of years to look over (currently ignores min year)
      years <- seq(yrend - yrcombine + 1, yrend)
      
      data %>%
        filter(dob_yr %in% years) %>%
        summarise(`Sample Size` = n()) %>%
        mutate(
          Year = paste0(min(years), "–", max(years)),
          Category1 = "King County",
          Group = "King County",
          Category2 = "King County",
          Subgroup = "King County"
        ) %>%
        select(Year, Category1, Group, Category2, Subgroup, `Sample Size`) -> denom_kc
      return(denom_kc)
    }
  }


f_denom_grp <-
  function(data, yrstart = minyr_im, yrend = maxyr_im, yrcombine = 5, trend = FALSE) {
    # Make empty list to add data to
    denomlist = list()
    
    # List out subgroups to examine over
    groups <- c("moracsum", "race_mom", "hisp_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
    # List out group labels
    grplabels <- c("Mother's race/ethnicity (summary)", "Mother's race/ethnicity (expanded)", "Mother's race/ethnicity (summary)", 
                   "Mother's age", "Mother's education", "HRA", "Region")
    
    if (trend == TRUE) {
      for (i in 1:length(groups)) {
        data %>%
          filter_(~dob_yr >= yrstart) %>%
          filter_(~dob_yr <= yrend) %>%
          group_by_(~dob_yr, groups[i]) %>%
          summarise(`Sample Size` = n()) %>%
          mutate(
            Category1 = grplabels[i], 
            Category2 = "",
            Subgroup = "") %>%
          rename_(Year = ~dob_yr, Group = groups[i]) %>%
          select(Year, Category1, Group, Category2, Subgroup, `Sample Size`
          ) -> denomlist[[i]]
      }
      denom_grp <- rbindlist(denomlist)
      return(denom_grp)
    }
    
    else {
    # Make range of years to look over
    years <- seq(yrend - yrcombine + 1, yrend)
    
    for (i in 1:length(groups)) {
        data %>%
          filter(dob_yr %in% years) %>%
          group_by_(groups[i]) %>%
          summarise(`Sample Size` = n()) %>%
          mutate(
            Year = paste0(min(years), "–", max(years)),
            Category1 = grplabels[i], 
            Category2 = "",
            Subgroup = "") %>%
          rename_(Group = groups[i]) %>%
          select(Year, Category1, Group, Category2, Subgroup, `Sample Size`
                 ) -> denomlist[[i]]
    }
    denom_grp <- rbindlist(denomlist)
    return(denom_grp)
    }
  }


# No need for single-year trend data in subgroups at this point
f_denom_subgrp <-
  function(data, yrend = maxyr_im, yrcombine = 5) {
    # Make range of years to look over
    years <- seq(yrend - yrcombine + 1, yrend)
    
    # List out subgroups to examine over
    groups <- c("moracsum", "race_mom", "hisp_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
    # List out group labels
    grplabels <- c("Mother's race/ethnicity (summary)", "Mother's race/ethnicity (expanded)", "Mother's race/ethnicity (summary)", 
                   "Mother's age", "Mother's education", "HRA", "Region")
    
    # Make empty list to add data to
    denomlist = list()
    
    for (i in 1:length(groups)) {
      # Set up subgroups to cycle through
      subgroups <- groups[-i]
      # Set up subgroup labels
      subgrplabels <- grplabels[-i]

      # Make empty sublist to add data to
      sublist = list()
            
      for (j in 1:length(subgroups)) {
        data %>%
          filter(dob_yr %in% years) %>%
          group_by_(groups[i], subgroups[j]) %>%
          summarise(`Sample Size` = n()) %>%
          mutate(
            Year = paste0(min(years), "–", max(years)),
            Category1 = grplabels[i], 
            Category2 = subgrplabels[j]) %>%
          rename_(Group = groups[i], Subgroup = subgroups[j]) %>%
          select(Year, Category1, Group, Category2, Subgroup, `Sample Size`
                 ) -> sublist[[j]]
      }
      denomlist[[i]] <- rbindlist(sublist)
    }
    denom_subgrp <- rbindlist(denomlist)
    return(denom_subgrp)
  }


denom <- bind_rows(f_denom_kc(births_denom), f_denom_grp(births_denom), f_denom_subgrp(births_denom))


#### Define functions ####
# Gives overall KC average
infmort_kc <- function(imdata, birdata, yrend = maxyr_im, yrcombine = 5) {
  data2 <- birdata
  yrend2 <- yrend
  yrcombine2 <- yrcombine
  
  denom <- f_denom_kc(data = data2, yrend = yrend2, yrcombine = yrcombine2)
  
  # Make range of years to look over
  years <- seq(yrend - yrcombine + 1, yrend)
  
  imdata %>% 
    filter(dth_yr %in% years) %>%
    summarise(Numerator = n()) %>%
    mutate(
      Tab = "_King County",
      Year = paste0(min(years), "–", max(years)),
      Category1 = "King County",
      Group = "King County",
      Category1_grp = "King County",
      Group_grp = "King County",
      Category2 = "King County",
      Subgroup = "King County") %>%
    left_join(., denom, by = c("Category1", "Group", "Category2",
                               "Subgroup", "Year")) %>%
    mutate(Proportion = Numerator/`Sample Size` * 1000,
           `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size` * 1000,
           `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size` * 1000,
           se = sqrt(Numerator / (`Sample Size`^2)) * 1000,
           rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
           Suppress = ifelse(`Sample Size` < 50 |
                               Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
           ) %>%
    select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Proportion, 
           `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress
      ) -> imdata
    return(imdata)
}


# Looks at each value within a group (e.g., race)
infmort_subgroup <-
  function(imdata, birdata, yrend = maxyr_im, yrcombine = 5, group = 'all', grprace = FALSE, smlgrp = FALSE) {
    data2 <- birdata
    yrend2 <- yrend
    yrcombine2 <- yrcombine
    
    # Make range of years to look over
    years <- seq(yrend - yrcombine + 1, yrend)
    
    denom <- f_denom_grp(data = data2, yrend = yrend2, yrcombine = yrcombine2)  
    
    if (grprace == TRUE) {
      # List out subgroups to examine over
      groups <- c("moracsum", "hisp_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
      # List out group labels
      grplabels <- c("Mother's race/ethnicity (summary)", "Mother's race/ethnicity (summary)", 
                     "Mother's age", "Mother's education", "HRA", "Region")
    } else {
      # List out subgroups to examine over
      groups <- c("moracsum", "race_mom", "hisp_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
      # List out group labels
      grplabels <- c("Mother's race/ethnicity (summary)", "Mother's race/ethnicity (expanded)", "Mother's race/ethnicity (summary)", 
                     "Mother's age", "Mother's education", "HRA", "Region")
    }

    
    if (group == "all") {
      # Make empty list to add data to
      subgrplist = list()
      
      for (i in 1:length(groups)) {
        print(paste0("working on ", grplabels[i]))
        
        imdata %>% 
          filter(dth_yr %in% years) %>%
          group_by_(groups[i]) %>%
          summarise(Numerator = n()) %>%
          mutate(
            Tab = "Subgroups",
            Year = paste0(min(years), "–", max(years)),
            Category1 = grplabels[i], 
            Category1_grp = "",
            Group_grp = "",
            Category2 = "",
            Subgroup = "") %>%
          rename_(Group = groups[i]) %>%
          left_join(., denom, by = c("Year", "Category1", "Group", "Category2", "Subgroup")) %>%
          mutate(se = sqrt(Numerator / (`Sample Size`^2)) * 1000,
            rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
            Suppress = ifelse(`Sample Size` < 50 |
                                Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
          ) %>%
          # need to use rowwise for poisson.test to work
          rowwise() %>%
          mutate(Proportion = Numerator/`Sample Size` * 1000,
                 `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size` * 1000,
                 `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size` * 1000) %>%
          ungroup() %>%
          select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Proportion, 
                 `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress
          ) -> subgrplist[[i]]
      }
      subgroups <- rbindlist(subgrplist)
      
      if (grprace == TRUE) {
        subgroups <- subgroups %>%
          mutate(Category1_grp = Category1,
                 Group_grp = Group,
                 Category1 = "",
                 Group = "")
      }
      
      if (smlgrp == TRUE) {
        subgroups <- subgroups %>%
          mutate(Tab = "Smallgroups",
                 Category2 = as.character("Overall"),
                 Subgroup = as.character("Overall"))
      }
      
      return(subgroups)
    }
    
    else{
      imdata %>% 
        filter(dth_yr %in% years) %>%
        group_by_(group) %>%
        summarise(Numerator = n()) %>%
        mutate(
          Tab = "Subgroups",
          Year = paste0(min(years), "–", max(years)),
          Category1 = grplabels[which(groups == group)],
          Category1_grp = "",
          Group_grp = "",
          Category2 = "",
          Subgroup = "") %>%
        rename_(Group = group) %>%
        left_join(., denom, by = c("Year", "Category1", "Group", "Category2", "Subgroup")) %>%
        mutate(se = sqrt(Numerator / (`Sample Size`^2)) * 1000,
               rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
               Suppress = ifelse(`Sample Size` < 50 |
                                   Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
        ) %>%
        # need to use rowwise for poisson.test to work
        rowwise() %>%
        mutate(Proportion = Numerator/`Sample Size` * 1000,
               `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size` * 1000,
               `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size` * 1000) %>%
        ungroup() %>%
        select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Proportion, 
               `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress
        ) -> subgroups
      
      if (grprace == TRUE) {
        subgroups <- subgroups %>%
          mutate(Category1_grp = Category1,
                 Group_grp = Group,
                 Category1 = "",
                 Group = "")
      }
    
      if (smlgrp == TRUE) {
        subgroups <- subgroups %>%
          mutate(Tab = "Smallgroups",
                 Category2 = as.character("Overall"),
                 Subgroup = as.character("Overall"))
      }
      return(subgroups)
    }
    }


# Creates crosstabs for a group and subgroup (e.g., race by region)
infmort_smallgroup <-
  function(imdata, birdata, yrend = maxyr_im, yrcombine = 5, group = 'all', grprace = FALSE) {
    data2 <- birdata
    yrend2 <- yrend
    yrcombine2 <- yrcombine
    
    # Make range of years to look over
    years <- seq(yrend - yrcombine + 1, yrend)
    
    denom <- f_denom_subgrp(data = data2, yrend = yrend2, yrcombine = yrcombine2)  
    
    if (grprace == TRUE) {
      # List out subgroups to examine over
      groups <- c("moracsum", "hisp_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
      # List out group labels
      grplabels <- c("Mother's race/ethnicity (summary)", "Mother's race/ethnicity (summary)", 
                     "Mother's age", "Mother's education", "HRA", "Region")
    } else {
      # List out subgroups to examine over
      groups <- c("moracsum", "race_mom", "hisp_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
      # List out group labels
      grplabels <- c("Mother's race/ethnicity (summary)", "Mother's race/ethnicity (expanded)", "Mother's race/ethnicity (summary)", 
                     "Mother's age", "Mother's education", "HRA", "Region")
    }
    
    if (group == "all") {
      # Make empty list to add data to
      smlgrplist = list()
    
      for (i in 1:length(groups)) {
        print(paste0("working on ", grplabels[i]))
      
        # Set up subgroups to cycle through
        subgroups <- groups[-i]
        # Set up subgroup labels
        subgrplabels <- grplabels[-i]
      
        # Make empty sublist to add data to
        sublist = list()
      
        for (j in 1:length(subgroups)) {
          print(paste0("working on group: ", grplabels[i],  ", subgroup: ", subgrplabels[j]))
        
          imdata %>% 
            filter(dth_yr %in% years) %>%
            group_by_(groups[i], subgroups[j]) %>%
            summarise(Numerator = n()) %>%
            mutate(
              Tab = "Smallgroups",
              Year = paste0(min(years), "–", max(years)),
              Category1 = grplabels[i], 
              Category1_grp = "",
              Group_grp = "",
              Category2 = subgrplabels[j]
              ) %>%
            rename_(Group = groups[i], Subgroup = subgroups[j]) %>%
            left_join(., denom, by = c("Year", "Category1", "Group", "Category2", "Subgroup")) %>%
            mutate(se = sqrt(Numerator / (`Sample Size`^2)) * 1000,
                   rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
                   Suppress = ifelse(`Sample Size` < 50 |
                                       Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
            ) %>%
            # need to use rowwise for poisson.test to work
            rowwise() %>%
            mutate(Proportion = Numerator/`Sample Size` * 1000,
                   `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size` * 1000,
                  `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size` * 1000) %>%
            ungroup() %>%
            select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Proportion, 
                   `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress
            ) -> sublist[[j]]
        }
        smlgrplist[[i]] <- rbindlist(sublist)
      }
      smallgroups <- rbindlist(smlgrplist)
    
      if (grprace == TRUE) {
       smallgroups <- smallgroups %>%
          mutate(Category1_grp = Category1,
                Group_grp = Group,
                 Category1 = "",
                 Group = "")
      }
    return(smallgroups)
    }
    
    else {
      # Set up subgroups to cycle through
      subgroups <- groups[-which(groups == group)]
      # Set up subgroup labels
      subgrplabels <- grplabels[-which(groups == group)]
      
      # Make empty list to add data to
      smlgrplist = list()
      
      for (j in 1:length(subgroups)) {
        print(paste0("working on group: ", group, ": ", subgrplabels[j]))
        
        imdata %>% 
          filter(dth_yr %in% years) %>%
          group_by_(group, subgroups[j]) %>%
          summarise(Numerator = n()) %>%
          mutate(
            Tab = "Smallgroups",
            Year = paste0(min(years), "–", max(years)),
            Category1 = grplabels[which(groups == group)],
            Category1_grp = "",
            Group_grp = "",
            Category2 = subgrplabels[j]
          ) %>%
          rename_(Group = group, Subgroup = subgroups[j]) %>%
          left_join(., denom, by = c("Year", "Category1", "Group", "Category2", "Subgroup")) %>%
          mutate(se = sqrt(Numerator / (`Sample Size`^2)) * 1000,
                 rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
                 Suppress = ifelse(`Sample Size` < 50 |
                                     Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
          ) %>%
          # need to use rowwise for poisson.test to work
          rowwise() %>%
          mutate(Proportion = Numerator/`Sample Size` * 1000,
                 `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size` * 1000,
                 `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size` * 1000) %>%
          ungroup() %>%
          select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Proportion, 
                 `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress
          ) -> smlgrplist[[j]]
      }
        smallgroups <- rbindlist(smlgrplist)
        
        if (grprace == TRUE) {
          smallgroups <- smallgroups %>%
            mutate(Category1_grp = Category1,
                   Group_grp = Group,
                   Category1 = "",
                   Group = "")
        }
        return(smallgroups)
    }
  }


#### Look over time for JoinPoint ####
# Calculates yearly averages for KC overall
infmort_time_kc <- function(imdata, birdata, yrstart = minyr_im, yrend = maxyr_im) {
  
  data2 <- birdata
  yrstart2 <- yrstart
  yrend2 <- yrend

  denom <- f_denom_kc(data = data2, yrstart = yrstart2, yrend = yrend2, trend = TRUE)
  
  # Need to make a frame containing all possible years so that rolling averages combine properly
  yearframe <- as.data.frame(cbind(Year = seq(yrstart, yrend)))
  
  imdata %>% 
    filter(dth_yr >= yrstart & dth_yr <= yrend) %>%
    group_by(dth_yr) %>%
    summarise(Numerator = n()) %>%
    rename(Year = dth_yr) %>%
    right_join(., yearframe, by = c("Year")) %>%
    mutate(
      Tab = "Trends_JP",
      Category1 = "King County",
      Group = "King County",
      Category1_grp = "King County",
      Group_grp = "King County",
      Category2 = "King County",
      Subgroup = "King County") %>%
    left_join(., denom, by = c("Category1", "Group", "Category2",
                               "Subgroup", "Year")) %>%
    mutate(se = sqrt(Numerator / (`Sample Size`^2)) * 1000,
           rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
           Suppress = ifelse(`Sample Size` < 50 |
                               Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
    ) %>%
    # Need to temporarily set any missing numerator data to 0 for poisson.test to work
    mutate(naflag = ifelse(is.na(Numerator), 1, 0),
           Numerator = as.numeric(ifelse(is.na(Numerator), 0, Numerator))) %>%
    rowwise() %>%
    mutate(Proportion = Numerator/`Sample Size` * 1000,
           `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size` * 1000,
           `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size` * 1000) %>%
    ungroup() %>%
    # Restore missing data back to missing
    mutate(Numerator = ifelse(naflag == 1, NA, Numerator)) %>%
    select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Proportion, 
           `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress
    ) -> imdata
  return(imdata)
}


# Calculates yearly averages for each value within a group (e.g., race)
infmort_time_subgroup <-
  function(imdata, birdata, yrstart = minyr_im, yrend = maxyr_im, group = "all", grprace = FALSE) {
    data2 <- birdata
    yrstart2 <- yrstart
    yrend2 <- yrend
    
    denom <- f_denom_grp(data = data2, yrend = yrend2, trend = TRUE)
    

    if (grprace == TRUE) {
      # List out subgroups to examine over
      groups <-  c("moracsum", "hisp_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
      # List out group labels
      grplabels <- c("Mother's race/ethnicity (summary)", "Mother's race/ethnicity (summary)", 
                     "Mother's age", "Mother's education", "HRA", "Region")
    } else {
      # List out subgroups to examine over
      groups <- c("moracsum", "race_mom", "hisp_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
      # List out group labels
      grplabels <- c("Mother's race/ethnicity (summary)", "Mother's race/ethnicity (expanded)", "Mother's race/ethnicity (summary)", 
                     "Mother's age", "Mother's education", "HRA", "Region")
    }
    
    if (group == "all") {
      # Make empty list to add data to
      subgrplist = list()
    
      for (i in 1:length(groups)) {
        print(paste0("working on ", grplabels[i]))
      
        # Need to make a frame containing all possible years for each group 
        # so that rolling averages combine properly
        yearframe <- data.frame("Year" = rep(seq(yrstart, yrend), each = length(unique(imdata[,groups[i]]))),
                                "Group" = rep(unique(imdata[,groups[i]]), yrend - yrstart + 1),
                                stringsAsFactors = FALSE)
        
        imdata %>%
          filter(dth_yr >= yrstart & dth_yr <= yrend) %>%
          group_by_(~dth_yr, groups[i]) %>%
          summarise(Numerator = n()) %>%
          rename_(Year = ~dth_yr, Group = groups[i]) %>%
          right_join(., yearframe, by = c("Year", "Group")) %>%
          mutate(
            Tab = "Trends_JP",
            Category1 = grplabels[i], 
            Category1_grp = "",
            Group_grp = "",
            Category2 = "",
            Subgroup = ""
            ) %>%
          left_join(., denom, by = c("Category1", "Group", "Category2", "Subgroup", "Year")) %>%
          mutate(
            se = sqrt(Numerator / (`Sample Size` ^ 2)) * 1000,
            rse = ifelse(Numerator > 0, 1 / sqrt(Numerator), NA),
            Suppress = ifelse(`Sample Size` < 50 | Numerator < 5, 
                              "Y", ifelse(rse >= 30, "N*", "N")
            )
          ) %>%
          # Need to temporarily set any missing numerator data to 0 for poisson.test to work
          # Missing data occurs because there were no events that year so 0 is appropriate
          mutate(naflag = ifelse(is.na(Numerator), 1, 0),
                 Numerator = as.numeric(ifelse(is.na(Numerator), 0, Numerator))) %>%
          # need to use rowwise for poisson.test to work
          rowwise() %>%
          mutate(
            Proportion = Numerator / `Sample Size` * 1000,
            `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size` * 1000,
            `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size` * 1000
          ) %>%
          ungroup() %>%
          select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Proportion,
            `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress
          ) -> subgrplist[[i]]
      }
     subgroups <- rbindlist(subgrplist)
    
      if (grprace == TRUE) {
        subgroups <- subgroups %>%
          mutate(
            Category1_grp = Category1,
            Group_grp = Group,
            Category1 = "",
            Group = ""
          )
      }
    return(subgroups)
    }
    
    else {
      # Need to make a frame containing all possible years for each group 
      # so that rolling averages combine properly
      yearframe <- data.frame("Year" = rep(seq(yrstart, yrend), each = length(unique(imdata[,group]))),
                              "Group" = rep(unique(imdata[,group]), yrend - yrstart + 1),
                              stringsAsFactors = FALSE)
      
      imdata %>%
        filter(dth_yr >= yrstart & dth_yr <= yrend) %>%
        group_by_(~dth_yr, group) %>%
        summarise(Numerator = n()) %>%
        rename_(Year = ~dth_yr, Group = group) %>%
        right_join(., yearframe, by = c("Year", "Group")) %>%
        mutate(
          Tab = "Trends_JP",
          Category1 = grplabels[which(groups == group)],
          Category1_grp = "",
          Group_grp = "",
          Category2 = "",
          Subgroup = ""
        ) %>%
        left_join(., denom, by = c("Category1", "Group", "Category2", "Subgroup", "Year")) %>%
        mutate(
          se = sqrt(Numerator / (`Sample Size` ^ 2)) * 1000,
          rse = ifelse(Numerator > 0, 1 / sqrt(Numerator), NA),
          Suppress = ifelse(`Sample Size` < 50 | Numerator < 5, 
                            "Y", ifelse(rse >= 30, "N*", "N")
          )
        ) %>%
        # Need to set any missing numerator data to 0 for poisson.test to work
        # Missing data occurs because there were no events that year so 0 is appropriate
        mutate(naflag = ifelse(is.na(Numerator), 1, 0),
               Numerator = as.numeric(ifelse(is.na(Numerator), 0, Numerator))) %>%
        # need to use rowwise for poisson.test to work
        rowwise() %>%
        mutate(
          Proportion = Numerator / `Sample Size` * 1000,
          `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size` * 1000,
          `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size` * 1000
        ) %>%
        ungroup() %>%
        select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Proportion,
               `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress
        ) -> imdata
      
      if (grprace == TRUE) {
        imdata <- imdata %>%
          mutate(
            Category1_grp = Category1,
            Group_grp = Group,
            Category1 = "",
            Group = ""
          )
      }
      return(imdata)
      
    }
  }


#### Look over time with rolling averages ####
# Calculates rolling averages for KC overall
infmort_rollyr_kc <- function(imdata, birdata, yrstart = minyr_im, yrend = maxyr_im, yrcombine = 5) {
  
  # Need to add in checks about yrend >= yrstart and the maximum number of years that can be combined
  
  imdata2 <- imdata
  birdata2 <- birdata
  maxyr_im2 <- yrend
  
  indyr <- infmort_time_kc(imdata = imdata2, birdata = birdata2, yrend = maxyr_im2) %>%
    filter(Year >= yrstart & Year <= yrend) %>%
    select(Category1:Subgroup, Numerator, `Sample Size`)
  
  
  rollyr <- cbind.data.frame(Year = paste(seq(yrstart, yrstart + (yrend - yrstart - yrcombine + 1)), 
                                          seq(yrstart + yrcombine - 1, yrend), sep = "–"),
                             Numerator = as.numeric(roll_sum(indyr$Numerator, n = yrcombine)),
                             `Sample Size` = roll_sum(indyr$`Sample Size`, n = yrcombine))
  
  final <- indyr %>%
    group_by(Category1, Group, Category1_grp, Group_grp,
             Category2, Subgroup) %>%
    slice(1:length(rollyr$Year)) %>%
    select(Category1:Subgroup) %>%
    cbind.data.frame(., rollyr) %>%
    mutate(
      Tab = "Trends",
      Proportion = Numerator / `Sample Size` * 1000,
      se = sqrt(Numerator / (`Sample Size`^2)) * 1000,
      rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
      Suppress = ifelse(`Sample Size` < 50 |
                          Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
    ) %>%
    rowwise() %>%
    mutate(
      `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size` * 1000,
      `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size` * 1000) %>%
    ungroup() %>%
    select(
      Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Proportion, 
      `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress
    ) -> data
  return(data)
}


# Calculates rolling averages for each value within a group (e.g., race)
infmort_rollyr_subgroup <- function(imdata, birdata, yrstart = minyr_im, yrend = maxyr_im, 
                                   yrcombine = 5, group = "all", grprace = FALSE) {

  # Need to add in checks about yrend >= yrstart and the maximum number of years that can be combined
  imdata2 <- imdata
  birdata2 <- birdata
  maxyr_im2 <- yrend
  group2 <- group
  grprace2 <- grprace
  
  # Get each single year's data ready for combining
  indyr <-
    infmort_time_subgroup(imdata = imdata2, birdata = birdata2, yrend = maxyr_im2, 
                          group = group2, grprace = grprace2) %>%
    filter(Year >= yrstart & Year <= yrend) %>%
    select(Year, Category1:Subgroup, Numerator, `Sample Size`) %>%
    arrange(Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Year)
  
  
  # Define how many years to combine in each direction (cannot define in lead function below due to dplyr bug)
  buffera <- ifelse(yrcombine %% 2 == 0, floor((yrcombine - 1) / 2), ceiling((yrcombine - 1) /2))
  bufferb <- ceiling((yrcombine - 1) /2)
  
  # Combine data over rolling window (ignores groups, which are addressed with the drop var)
  final <- indyr %>%
    mutate(Year2 = as.character(paste(lag(Year, buffera), lead(Year, bufferb), sep = '–')),
           drop = ifelse(is.na(lag(Year, buffera)) | is.na(lead(Year, bufferb)) |
                           Year < lag(Year, buffera) |
                           Year > lead(Year, bufferb), 1, 0),
           Numerator2 = c(rep(NA, buffera), 
                          roll_sum(indyr$Numerator, n = yrcombine),
                          rep(NA, bufferb)),
           `Sample Size2` = c(rep(NA, buffera), 
                              roll_sum(indyr$`Sample Size`, n = yrcombine),
                              rep(NA, bufferb))) %>%
    filter(drop == 0) %>%
    select(Year2, Numerator2, `Sample Size2`, Category1:Subgroup) %>%
    rename(Year = Year2, Numerator = Numerator2, `Sample Size` = `Sample Size2`) %>%
    mutate(
      Tab = "Trends",
      Proportion = Numerator / `Sample Size` * 1000,
      se = sqrt(Numerator / (`Sample Size`^2)) * 1000,
      rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
      Suppress = ifelse(`Sample Size` < 50 |
                          Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
    ) %>%
    rowwise() %>%
    mutate(
      `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size` * 1000,
      `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size` * 1000) %>%
    ungroup() %>%
    select(
      Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Proportion, 
      `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress
    ) -> data
  
  return(data)
}



#### H1: Infant mortality ####
infmort<- rbindlist(list(
  # KC overall
  infmort_kc(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, yrcombine = 5),
  # By each subgroup
  infmort_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, yrcombine = 5, group = "all", grprace = FALSE, smlgrp = FALSE),
  infmort_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, yrcombine = 5, group = "all", grprace = TRUE, smlgrp = FALSE),
  # Crosstabs for each subgroup (including overall for when group = subgroup)
  infmort_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, yrcombine = 9, group = "all", grprace = FALSE, smlgrp = TRUE),
  infmort_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, yrcombine = 9, group = "all", grprace = TRUE, smlgrp = TRUE),
  infmort_smallgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, yrcombine = 9, group = "all", grprace = FALSE),
  infmort_smallgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, yrcombine = 9, group = "all", grprace = TRUE),
  # Time trend for KC overall
  infmort_rollyr_kc(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, yrcombine = 5),
  # Time trend for each subgroup
  infmort_rollyr_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, yrcombine = 5, group = "rgn_id", grprace = FALSE),
  infmort_rollyr_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, yrcombine = 5, group = "moracsum", grprace = FALSE),
  infmort_rollyr_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, yrcombine = 5, group = "hisp_mom", grprace = FALSE),
  infmort_rollyr_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, yrcombine = 5, group = "rgn_id", grprace = TRUE),
  infmort_rollyr_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, yrcombine = 5, group = "moracsum", grprace = TRUE),
  infmort_rollyr_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, yrcombine = 5, group = "hisp_mom", grprace = TRUE),
  # Time trend for KC overall (JoinPoint data)
  infmort_time_kc(imdata = im03_15, birdata = births_denom, yrend = maxyr_im),
  # Time trend for each subgroup (JoinPoint data)
  infmort_time_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, group = "rgn_id", grprace = FALSE),
  infmort_time_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, group = "moracsum", grprace = FALSE),
  infmort_time_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, group = "hisp_mom", grprace = FALSE),
  infmort_time_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, group = "rgn_id", grprace = TRUE),
  infmort_time_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, group = "moracsum", grprace = TRUE),
  infmort_time_subgroup(imdata = im03_15, birdata = births_denom, yrend = maxyr_im, group = "hisp_mom", grprace = TRUE)
))


### Convert to a df
infmort <- as.data.frame(infmort)

### Apply labels
infmort <- left_join(infmort, labels, by = c("Category1", "Group")) %>%
  rename(Group.id = Group, Group = Label)
infmort <- left_join(infmort, labels, by = c("Category1_grp" = "Category1", "Group_grp" = "Group")) %>%
  rename(Group_grp.id = Group_grp, Group_grp = Label)
infmort <- left_join(infmort, labels, by = c("Category2" = "Category1", "Subgroup" = "Group")) %>%
  rename(Subgroup.id = Subgroup, Subgroup = Label)

# Reorder and sort columns
infmort <- infmort %>% select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, 
                              Proportion, `Lower Bound`, `Upper Bound`, se, rse, 
                              `Sample Size`, Numerator, Suppress, Group.id, Subgroup.id) %>%
  arrange(Tab, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Year)

### Clean up nulls and blanks
infmort <- infmort %>%
  filter(!((Category1 != "" & is.na(Group)) | 
             (Category1_grp != "" & is.na(Group_grp)) |
             (Category2 != "" & is.na(Subgroup))))


### Add in comparisons with KC
# Pull out KC values
kclb <- as.numeric(infmort %>%
                     filter(Tab == "_King County") %>%
                     select(`Lower Bound`))
kcub <- as.numeric(infmort %>%
                     filter(Tab == "_King County") %>%
                     select(`Upper Bound`))
kctrend <- infmort %>%
  filter(Tab == "Trends" & Category1 == "King County") %>%
  select(Year, `Lower Bound`, `Upper Bound`)

# Do comparison for most recent year (i.e., not trends)
comparison <- infmort %>%
  filter(Tab != "Trends") %>%
  mutate(`Comparison with KC` = ifelse(`Upper Bound` < kclb, "lower",
                                       ifelse(`Lower Bound` > kcub, "higher", "no different"))) %>%
  select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, `Comparison with KC`)

# Do comparison for trends (each year compared to KC that year, not that group's trend over time)
comparison_trend <- infmort %>%
  filter(Tab == "Trends") %>%
  left_join(., kctrend, by = "Year") %>%
  mutate(`Comparison with KC` = ifelse(`Upper Bound.x` < `Lower Bound.y`, "lower",
                                       ifelse(`Lower Bound.x` > `Upper Bound.y`, "higher", "no different"))) %>%
  select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, `Comparison with KC`)

# Bring both comparisons together
comparison <- rbind(comparison, comparison_trend)

# Merge with main data and add column for time trends
infmort <- left_join(infmort, comparison, by = c("Tab", "Year", "Category1", "Group", "Category1_grp", 
                                                 "Group_grp", "Category2", "Subgroup")) %>%
  mutate(`Time Trends` = "")
rm(comparison, comparison_trend, kclb, kcub, kctrend)


# Reorder (something messes the order up and puts 2011–2015 first followed by the correct order)
# However, this command does not fix the issue
infmort <- arrange(infmort, Tab, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Year)

### Export to an Excel file
infmort %>% filter(Tab != "Trends_JP" & Category1 != "Mother's race/ethnicity (grouped)" &
                     Category1_grp != "Mother's race/ethnicity (grouped)" &
                     Category2 != "Mother's race/ethnicity (grouped)") %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - infant mortality.xlsx"), sheetName = "infmort")


### Apply suppression rules and export
infmort %>% mutate_at(vars(Proportion:rse, Numerator, `Comparison with KC`),
                      funs(ifelse(Suppress == "Y", NA, .))) %>%
  mutate_at(vars(Proportion:rse), funs(round(., digits = 1))) %>%
  filter(Tab != "Trends_JP" & Category1 != "Mother's race/ethnicity (grouped)" &
           Category1_grp != "Mother's race/ethnicity (grouped)" &
           Category2 != "Mother's race/ethnicity (grouped)") %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - infant mortality - suppressed.xlsx"), sheetName = "infmort")


### Create data sets for Joinpoint
infmort %>%
  filter(Tab == "Trends_JP" & Category1 != "" &  Category1 != "Mother's race/ethnicity (grouped)" &
           Category1_grp != "Mother's race/ethnicity (grouped)" &
           Category2 != "Mother's race/ethnicity (grouped)") %>%
  select(Tab, Category1, Group, Year, Numerator, `Sample Size`, Proportion, se) %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - infant mortality - JP.xlsx"), sheetName = "infmort")

infmort %>%
  filter(Tab == "Trends_JP" & Category1 != "" &  Category1 != "Mother's race/ethnicity (grouped)" &
           Category1_grp != "Mother's race/ethnicity (grouped)" &
           Category2 != "Mother's race/ethnicity (grouped)") %>%
  select(Tab, Category1_grp, Group_grp, Numerator, `Sample Size`, Year, Proportion, se) %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - infant mortality grouped race - JP.xlsx"), sheetName = "infmort")

