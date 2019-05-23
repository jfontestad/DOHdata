###########################################
# Code to analyze the WA birth data
# for use in Best Starts for Kids
#
# Alastair Matheson
# Public Health Seattle & King County
#
# November 2016
###########################################

# Aim is to produce the following:
#  1) Univarate breakdown of the indicator by age, race, place, SES
#  2) Time trends by race and region (need to incorporate Joinpoint output still)
#  3) Bivariate breakdown of the indicator (e.g., age by race)



#### Call in libraries ####
# Use install.packages("Package name") if you do not already have these libraries
library(RODBC) # Used to connect to the SQL database
library(lazyeval) # Makes functions work as expected
library(car) # used to recode variables
library(dplyr) # Used to manipulate data
library(data.table) # more data manipulation
library(dtplyr) # lets dplyr and data.table play nicely together
library(purrr) # applies functions over lists of variables
library(TTR) # calculates rolling averages
library(RcppRoll) # calculates rolling averages
library(openxlsx) # Writes to Excel


#### Connect to SQL data ####
db.apde50 <- odbcConnect("PH_APDEStore")

# Bring in births data and join with geocoding tables
births <-
  sqlQuery(
    db.apde50,
    "SELECT certno_e, dob_yr, age_mom, cnty_res, race_mom, educ_mom,
      pnatalmo, gestcalc, breastfd, pnatalvs, cigs_bef, 
      cigs_1st, cigs_2nd, cigs_3rd
    FROM 
    dbo.wabir2003_2015
    ",
    stringsAsFactors = FALSE
  )


# Bring in geo data
birth_geo <-
  sqlQuery(
    db.apde50,
    "SELECT *
    FROM dbo.geobir1990_2015 g1
    LEFT JOIN dbo.geocomp_blk10 g2
    ON g1.geo_id_blk10 = g2.geo_id_blk10
    ",
    stringsAsFactors = FALSE
  )


#### Bring in each year's IM data and combine ####
im03 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2003.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -560, 4, -24, 2, -2, 2, -65, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", 
                               "educ_mom", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im04 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2004.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -560, 4, -24, 2, -2, 2, -65, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", 
                               "educ_mom", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im05 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2005.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -560, 4, -24, 2, -2, 2, -65, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", 
                               "educ_mom", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im06 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2006.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -560, 4, -24, 2, -2, 2, -65, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", 
                               "educ_mom", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im07 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2007.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -560, 4, -24, 2, -2, 2, -65, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", 
                               "educ_mom", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im08 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2008.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -560, 4, -24, 2, -2, 2, -65, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", 
                               "educ_mom", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im09 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2009.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -560, 4, -24, 2, -2, 2, -65, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", 
                               "educ_mom", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im10 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2010.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -560, 4, -24, 2, -2, 2, -65, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", 
                               "educ_mom", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im11 <- read.fwf(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/inf_2011.asc", 
                 width = c(10, -1, 4, -15, 2, -2, 2, -9, 1, -3, 2, -560, 4, -24, 2, -2, 2, -65, 2, -312),
                 col.names = c("certno_e", "dob_yr", "age_mom", "cnty_res", "race_mom", 
                               "educ_mom", "dth_yr", "dbstate", "dcntyres", "dst_res"))
im12 <- read.csv(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/Infant2012.csv",
                 header = TRUE)
im12 <- select(im12, certno_e, dob_yr, age_mom, cnty_res, race_mom, educ_mom, dth_yr, dbstate, dcntyres, dst_res)
im13 <- read.csv(file = "//phdata01/DROF_DATA/DOH DATA/InfDeath/Data/InfantDeathF2013.csv",
                 header = TRUE)
im13 <- select(im13, certno_e, dob_yr, age_mom, cnty_res, race_mom, educ_mom, dth_yr, dbstate, dcntyres, dst_res)


im03_13 <- bind_rows(im03, im04, im05, im06, im07, im08, im09, im10, im11, im12, im13)
rm(im03, im04, im05, im06, im07, im08, im09, im10, im11, im12, im13)


#### Merge data and geo data ####
births <- left_join(births, birth_geo, by = "certno_e")
im03_13 <- left_join(im03_13, birth_geo, by = "certno_e")



# Backup data to avoid rereading
births.bk <- births
im03_13.bk <- im03_13





#### Restrict dataset to King County ####
births <- filter(births, cnty_res == 17)
im03_13 <- filter(im03_13, dcntyres == 17)




#### Make new variables and do recodes ####
# Clean up missing data
births$pnatalmo[births$pnatalmo == 999] <- NA
births$gestcalc[births$gestcalc == 99] <- NA

# Mother's age groupings
births$age_mom_grp <- car::recode(births$age_mom, "10:17 = 1; 18:24 = 2; 25:34 = 3;
                            35:44 = 4; 45:hi = 5; else = NA")
im03_13$age_mom_grp <- as.character(car::recode(im03_13$age_mom, "10:17 = 1; 18:24 = 2; 25:34 = 3;
                            35:44 = 4; 45:hi = 5; else = NA"))

# Mother's grouped race/ethnicity
births$race_mom_grp <- as.character(car::recode(births$race_mom, "'1' = 1; '2' = 2; 
                                   '3' = 3; c('4', '5', '7', 'D', 'E', 'G') = 4;
                                   c('A', 'F', 'H') = 5; 'C' = 6;
                                   'B' = 7; '6' = 8; c('8', '9') = 9"))
im03_13$race_mom_grp <- as.character(car::recode(im03_13$race_mom, "'1' = 1; '2' = 2; 
                                   '3' = 3; c('4', '5', '7', 'D', 'E', 'G') = 4;
                                   c('A', 'F', 'H') = 5; 'C' = 6;
                                   'B' = 7; '6' = 8; c('8', '9') = 9"))

# Expected number of prenatal visits
# Make Kotelchuck matrix
kotelchuck <- matrix(c(rep(1, 4), rep(2, 4), rep(3, 4), rep(4, 4), rep(5, 4), rep(6, 4),
                       rep(7, 2), rep(8, 2), rep(9, 2), 10, 11, seq(12, 24),
                     rep(NA, 4), rep(1, 4), rep(2, 4), rep(3, 4), rep(4, 4), rep(5, 4), rep(6, 2),
                       rep(7, 2), rep(8, 2), 9, 10, seq(11, 23),
                     rep(NA, 8), rep(1, 4), rep(2, 4), rep(3, 4), rep(4, 4), rep(5, 2), rep(6, 2),
                       rep(7, 2), 8, 9, seq(10, 22),
                     rep(NA, 12), rep(1, 4), rep(2, 4), rep(3, 4), rep(4, 2), rep(5, 2), 
                       rep(6, 2), 7, 8, seq(9, 21)),
                     nrow = 45, ncol = 4, byrow = FALSE,
                     dimnames = list(seq(from = 6, to = 50), seq(from = 1, to = 4)))

births$kotelchuck <- kotelchuck[cbind(match(births$gestcalc, rownames(kotelchuck)), match(births$pnatalmo, colnames(kotelchuck)))]


# Removes whitespace from HRA and region names (also make character for merging later)
births$hra_id <- trimws(births$hra_id)
births$rgn_name <- trimws(births$rgn_name)
births$rgn_id <- trimws(births$rgn_id)

im03_13$hra_id <- trimws(im03_13$hra_id)
im03_13$rgn_name <- trimws(im03_13$rgn_name)
im03_13$rgn_id <- as.character(im03_13$rgn_id)

births$educ_mom <- as.character(births$educ_mom)
im03_13$educ_mom <- as.character(im03_13$educ_mom)



#### Set up other parameters ####
minyr_b = min(births$dob_yr) # pulls out oldest recent year for which data are present
maxyr_b = max(births$dob_yr) # pulls out most recent year for which data are present

minyr_im = min(im03_13$dth_yr) # pulls out oldest year for which data are present
maxyr_im = max(im03_13$dth_yr) # pulls out most recent year for which data are present

z95 = qnorm(0.975) # sets up for calculating the SE (should no longer be needed since Poisson SEs)

# Folder to write output to
filepath = "S:/WORK/Best Start for Kids/Dashboard/Interim dashboard materials/Tableau docs/"


#### Set up labels for each variable ####
# Make a data frame that can merge with created tables to label values
labels <- rbind.data.frame(
  cbind(Category1 = "King County", Group = "King County", Label = "King County"),
  cbind(Category1 = c(rep("Mother's race/ethnicity", times = 17)),
                Group = c("1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H"),
                  Label = c("White", "Black", "Native American", "Chinese", "Japanese", "Other Non-White", 
                    "Filipino", "Refused to State", "Unknown/Not Stated", "Hawaiian", 
                    "Other Asian/Pacific Islander", "Mexican/Chicano/Hispanic", "Asian Indian",
                    "Korean", "Samoan", "Vietnamese", "Guamanian")),
  cbind(Category1 = c(rep("Mother's race/ethnicity (grouped)", times = 9)),
        Group = c("1", "2", "3", "4", "5", "6", "7", "8", "9"),
        Label = c("White", "Black", "Native American", "Asian", "Pacific Islander",
                  "Mexican/Chicano/Hispanic", "Other Asian/Pacific Islander",
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
labels <- mutate(labels,
                 Category1 = as.character(Category1),
                 Group = as.character(Group),
                 Label = as.character(Label))


#### Set up denominator birth data for infant mortality ####
births_denom <- select(births, certno_e, dob_yr, age_mom, cnty_res, race_mom, educ_mom)

f_denom_kc <-
  function(data) {
    data %>%
      group_by(dob_yr) %>%
      summarise(`Sample Size` = n()) %>%
      mutate(
        Category1 = "King County",
        Group = "King County",
        Category2 = "King County",
        Subgroup = "King County"
      ) %>%
      rename(Year = dob_yr) %>%
      select(Category1, Group, Category2, Subgroup, Year, `Sample Size`) -> data
    return(data)
  }

f_denom_grp <-
  function(data) {
    # Make empty list to add data to
    denomlist = list()
    
    # List out groups to examine over
    groups <-
      c("race_mom", "race_mom_grp", "age_mom_grp", "educ_mom",
        "hra_id", "rgn_id")
    
    grplabels <-
      c(
        "Mother's race/ethnicity", "Mother's race/ethnicity (grouped)",
        "Mother's age", "Mother's education", "HRA", "Region"
      )
    
    for (i in 1:length(groups)) {
      data %>%
        group_by_(groups[i], ~ dob_yr) %>%
        summarise(`Sample Size` = n()) %>%
        mutate(Category1 = grplabels[i], 
               Category2 = "",
               Subgroup = "") %>%
        rename_(Group = groups[i], Year = ~ dob_yr) %>%
        select(Category1, Group, Category2, Subgroup, Year, `Sample Size`
        ) -> denomlist[[i]]
    }
    denom <- rbindlist(denomlist)
    return(denom)
  }


f_denom_subgrp <-
  function(data) {
    # Make empty list to add data to
    denomlist = list()
    
    # List out groups to examine over
    groups <-
      c("race_mom", "race_mom_grp", "age_mom_grp", "educ_mom",
        "hra_id", "rgn_id")
    
    grplabels <-
      c(
        "Mother's race/ethnicity", "Mother's race/ethnicity (grouped)",
        "Mother's age", "Mother's education", "HRA", "Region"
      )
    
    for (i in 1:length(groups)) {
      # Set up subgroups to cycle through
      subgroups <- groups[-i]
      # Set up subgroup labels
      subgrplabels <- grplabels[-i]
      
      for (j in 1:length(subgroups)) {
        # Make empty sublist to add data to
        sublist = list()
        data %>%
          group_by_(groups[i], subgroups[j], ~ dob_yr) %>%
          summarise(`Sample Size` = n()) %>%
          mutate(Category1 = grplabels[i], 
                 Category2 = subgrplabels[j]) %>%
          rename_(Group = groups[i], Subgroup = subgroups[j], Year = ~ dob_yr) %>%
          select(Category1, Group, Category2, Subgroup, Year, `Sample Size`) -> sublist
      }
      denomlist[[i]] <- sublist
    }
    smallgroups <- rbindlist(denomlist)
    return(smallgroups)
  }

denom <- bind_rows(f_denom_kc(births_denom), f_denom_grp(births_denom), f_denom_subgrp(births_denom))


#### Set up outcome variables ####
# Headline indicators:
# H1) Preterm birth
# H2) Infant mortality (uses different data file)

# Secondary/intermediate indicators
# S1) Breatfeeding initiation
# S2) Early and adequate prenatal care
# S3) Adolescent birth rate (15-17)

# Extra analyses
# E1) Smoking during pregnancy

births <- births %>%
  mutate(
    #H1: preterm birth
    preterm = ifelse(!is.na(gestcalc) & gestcalc < 37, 1, ifelse(!is.na(gestcalc) & gestcalc >= 37, 0, NA)),
    # S1: Breastfeeding initiation
    breastfed = as.numeric(car::recode(breastfd, "'1' = 1; 'Y' = 1; 'N' = 0; else = NA")),
    # S2: Early and adequate prenatal care
    prenatal = ifelse(is.na(pnatalmo), NA, 
                      ifelse(pnatalmo %in% (0:4) & pnatalvs >= 0.8 * kotelchuck, 1, 0)),
    # S3: Adolescent birth rate
    teenbir = ifelse(is.na(age_mom_grp), NA, ifelse(age_mom_grp == 1, 1, 0)),
    # E1: smoked during pregnancy
    smoked = rowSums(.[, c("cigs_bef", "cigs_1st", "cigs_2nd", "cigs_3rd")], na.rm = FALSE),
         smoked = car::recode(smoked, "1:hi = 1; 0 = 0; else = NA"))



#### Define functions for recent years ####

# Gives overall KC average
inddata_kc <- function(data, indicator, year = maxyr_b, yrcombine = 5, infmort = FALSE) {
  # Make range of years to look over
  years <- seq(year - yrcombine + 1, year)
  
  data %>% 
    filter(dob_yr %in% years) %>%
    filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
    summarise_(
      Percent = interp( ~ mean(var, na.rm = TRUE), var = as.name(indicator)),
      Numerator = interp( ~ sum(var, na.rm = TRUE), var = as.name(indicator)),
      `Sample Size` = ~ n(),
      sd = ~ sqrt(Percent)
    ) %>%
    mutate(
      Tab = "_King County",
      Year = paste0(min(years), "–", max(years)),
      Category1 = "King County",
      Group = "King County",
      Category1_grp = "King County",
      Group_grp = "King County",
      Category2 = "King County",
      Subgroup = "King County",
      se = sd / `Sample Size`,
      rse = ifelse(Percent > 0, se / Percent * 100, NA),
      `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size`,
      `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size`,
      Suppress = ifelse(`Sample Size` < 50 |
                          Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
    ) %>%
    select(
      Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
      `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress
    ) -> data
    return(data)
}


# Looks at each value within a group (e.g., race)
inddata_subgroup <-
  function(data, indicator, year = maxyr_b, yrcombine = 5, group = "all") {
    # Make range of years to look over
    years <- seq(year - yrcombine + 1, year)
    
    # List out subgroups to examine over
    groups <-
      c("race_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
    # List out group labels
    grplabels <-
      c("Mother's race/ethnicity", "Mother's age", "Mother's education", "HRA", "Region")

    if (group == "all") {
      # Make empty list to add data to
      subgrplist = list()
      
      for (i in 1:length(groups)) {
        print(paste0("working on ", grplabels[i]))
        
        data %>% 
          filter(dob_yr %in% years) %>%
          filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
          group_by_(groups[i]) %>%
          summarise_(
            Percent = interp( ~ mean(var, na.rm = TRUE), var = as.name(indicator)),
            Numerator = interp( ~ sum(var, na.rm = TRUE), var = as.name(indicator)),
            `Sample Size` = ~ n(),
            sd = ~ sqrt(Percent)
          ) %>%
          mutate(
            Tab = "Subgroups",
            Year = paste0(min(years), "–", max(years)),
            Category1 = grplabels[i], 
            Category1_grp = "",
            Group_grp = "",
            Category2 = "",
            Subgroup = "",
            se = sd / `Sample Size`,
            rse = ifelse(Percent > 0, se / Percent * 100, NA),
            `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size`,
            `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size`,
            Suppress = ifelse(
              `Sample Size` < 50 | Numerator < 5, "Y",
              ifelse(rse >= 30, "N*", "N")
            )
          ) %>%
          rename_(Group = groups[i]) %>%
          select(
            Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
            `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress) %>%
          ungroup() -> subgrplist[[i]]
      }
      subgroups <- rbindlist(subgrplist)
      return(subgroups)
    }
    
    data %>% 
      filter(dob_yr %in% years) %>%
      filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
      group_by_(interp(~var, var = as.name(group))) %>%
      summarise_(
        Percent = interp( ~ mean(var, na.rm = TRUE), var = as.name(indicator)),
        Numerator = interp( ~ sum(var, na.rm = TRUE), var = as.name(indicator)),
        `Sample Size` = ~ n(),
        sd = ~ sqrt(Percent)
      ) %>%
      mutate(
        Tab = "Subgroups",
        Year = paste0(min(years), "–", max(years)),
        Category1 = grplabels[which(groups == group)],
        Category1_grp = "",
        Group_grp = "",
        Category2 = "",
        Subgroup = "",
        se = sd / `Sample Size`,
        rse = ifelse(Percent > 0, se / Percent * 100, NA),
        `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size`,
        `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size`,
        Suppress = ifelse(`Sample Size` < 50 |
                            Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
      ) %>%
      rename_(Group = group) %>%
      select(
        Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
        `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress) %>%
      ungroup() -> data
    return(data)
    
  }


# Looks at each value within a group (e.g., race) using grouped race variable
inddata_subgroup_grprace <-
  function(data, indicator, year = maxyr_b, yrcombine = 5, group = "all") {
    # Make range of years to look over
    years <- seq(year - yrcombine + 1, year)
    
    # List out subgroups to examine over
    groups <-
      c("race_mom_grp", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
    # List out group labels
    grplabels <-
      c("Mother's race/ethnicity (grouped)", "Mother's age", "Mother's education", "HRA", "Region")
    
    if (group == "all") {
      # Make empty list to add data to
      subgrplist = list()
      
      for (i in 1:length(groups)) {
        print(paste0("working on ", grplabels[i]))
        
        data %>% 
              filter(dob_yr %in% years) %>%
          filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
          group_by_(groups[i]) %>%
          summarise_(
            Percent = interp(~ mean(var), var = as.name(indicator)),
            `Sample Size` = ~ n(),
            sd = interp(~ sd(var), var = as.name(indicator))
          ) %>%
          mutate(
            Tab = "Subgroups",
            Year = paste0(min(years), "–", max(years)),
            Category1 = "",
            Group = "",
            Category1_grp = grplabels[i],
            Category2 = "",
            Subgroup = "",
            se = sd / `Sample Size`,
            rse = ifelse(Percent > 0, se / Percent * 100, NA),
            Numerator = Percent * `Sample Size`,
            `Lower Bound` = Percent - z95 * se, `Upper Bound` = Percent + z95 * se,
            Suppress = ifelse(
              `Sample Size` < 50 | Numerator < 5, "Y",
              ifelse(rse >= 30, "N*", "N")
            )
          ) %>%
          rename_(Group_grp = groups[i]) %>%
          select(
            Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
            `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress
          ) -> subgrplist[[i]]
      }
      subgroups <- rbindlist(subgrplist)
      return(subgroups)
    }
    
    data %>% 
      filter(dob_yr %in% years) %>%
      filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
      group_by_(interp(~var, var = as.name(group))) %>%
      summarise_(
        Percent = interp( ~ mean(var, na.rm = TRUE), var = as.name(indicator)),
        `Sample Size` = ~ n(),
        sd = interp( ~ sd(var, na.rm = TRUE), var = as.name(indicator))
      ) %>%
      mutate(
        Tab = "Subgroups",
        Year = paste0(min(years), "–", max(years)),
        Category1 = "",
        Group = "",
        Category1_grp = grplabels[which(groups == group)],
        Category2 = "",
        Subgroup = "",
        se = sd / `Sample Size`,
        rse = ifelse(Percent > 0, se / Percent * 100, NA),
        Numerator = round(Percent * `Sample Size`, digits = 0),
        `Lower Bound` = Percent - z95 * se,
        `Upper Bound` = Percent + z95 * se,
        Suppress = ifelse(`Sample Size` < 50 |
                            Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
      ) %>%
      rename_(Group_grp = group) %>%
      select(
        Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
        `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress
      ) -> data
    return(data)
    
  }


# Creates crosstabs for a group and subgroup (e.g., race by region)
inddata_smallgroup <-
  function(data, indicator, year = maxyr_b, yrcombine = 5, group = "all") {
    # Make range of years to look over
    years <- seq(year - yrcombine + 1, year)
    
    # List out groups to examine over
    groups <-
      c("race_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
    # List out group labels
    grplabels <-
      c("Mother's race/ethnicity", "Mother's age", "Mother's education", "HRA", "Region")
    

    if (group == "all") {
      # Make empty list to add data to
      smlgrplist = list()
      
      for (i in 1:length(groups)) {
        print(paste0("working on group ", grplabels[i]))
        
        # Set up subgroups to cycle through
        subgroups <- groups[-i]
        # Set up subgroup labels
        subgrplabels <- grplabels[-i]
        
          for (j in 1:length(subgroups)) {
            # Make empty sublist to add data to
            sublist = list()
            
            print(paste0("working on group: ", grplabels[i], 
                         ", subgroup: ", subgrplabels[j]))
        
          data %>% 
          filter(dob_yr %in% years) %>%
          filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
          group_by_(groups[i], subgroups[j]) %>%
          summarise_(
            Percent = interp(~ mean(var), var = as.name(indicator)),
            `Sample Size` = ~ n(),
            sd = interp(~ sd(var), var = as.name(indicator))
          ) %>%
          mutate(
            Tab = "Smallgroups",
            Year = paste0(min(years), "–", max(years)), Category1 = grplabels[i],
            Category1_grp = "",
            Group_grp = "",
            Category2 = subgrplabels[j],
            se = sd / `Sample Size`,
            rse = ifelse(Percent > 0, se / Percent * 100, NA),
            Numerator = Percent * `Sample Size`,
            `Lower Bound` = Percent - z95 * se, `Upper Bound` = Percent + z95 * se,
            Suppress = ifelse(
              `Sample Size` < 50 | Numerator < 5, "Y",
              ifelse(rse >= 30, "N*", "N"))
          ) %>%
          rename_(Group = groups[i], Subgroup = subgroups[j]) %>%
          select(
            Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
            `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress
          ) -> sublist
          }
        smlgrplist[[i]] <- sublist
      }
      smallgroups <- rbindlist(smlgrplist)
      return(smallgroups)
    }

    if (group %in% groups) {
      
      # Set up subgroups to cycle through
      subgroups <- groups[-which(groups == group)]
      # Set up subgroup labels
      subgrplabels <- grplabels[-which(groups == group)]
      
      # Make empty list to add data to
      smlgrplist = list()
    
      for (j in 1:length(subgroups)) {
       print(paste0("working on group: ",
                    group,
                    ": ",
                    subgroups[j]))
      
        data %>% 
          filter(dob_yr %in% years) %>%
          filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
          group_by_(group, subgroups[j]) %>%
          summarise_(
          Percent = interp(~ mean(var), var = as.name(indicator)),
          `Sample Size` = ~ n(),
          sd = interp(~ sd(var), var = as.name(indicator))
        ) %>%
        mutate(
          Tab = "Smallgroups",
          Year = paste0(min(years), "–", max(years)),
          Category1 = grplabels[which(groups == group)],
          Category1_grp = "",
          Group_grp = "",
          Category2 = subgrplabels[j],
          se = sd / `Sample Size`,
          rse = ifelse(Percent > 0, se / Percent * 100, NA),
          Numerator = Percent * `Sample Size`,
          `Lower Bound` = Percent - z95 * se, `Upper Bound` = Percent + z95 * se,
          Suppress = ifelse(
            `Sample Size` < 50 | Numerator < 5, "Y",
            ifelse(rse >= 30, "N*", "N")
          )
        ) %>%
        rename_(Group = group, Subgroup = subgroups[j]) %>%
        select(
          Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
          `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress
        ) -> smlgrplist[[j]]
    }
      smallgroups <- rbindlist(smlgrplist)
    return(smallgroups)
  }
    
    print("Nothing else worked")
  }


# Creates crosstabs for a group and subgroup (e.g., race by region) using grouped race
inddata_smallgroup_grprace <-
  function(data, indicator, year = maxyr_b, yrcombine = 5, group = "all") {
    # Make range of years to look over
    years <- seq(year - yrcombine + 1, year)
    
    # List out groups to examine over
    groups <-
      c("race_mom_grp", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
    # List out group labels
    grplabels <-
      c("Mother's race/ethnicity (grouped)", "Mother's age", "Mother's education", "HRA", "Region")
    
    
    if (group == "all") {
      # Make empty list to add data to
      smlgrplist = list()
      
      for (i in 1:length(groups)) {
        print(paste0("working on group ", grplabels[i]))
        
        # Set up subgroups to cycle through
        subgroups <- groups[-i]
        # Set up subgroup labels
        subgrplabels <- grplabels[-i]
        
        for (j in 1:length(subgroups)) {
          # Make empty sublist to add data to
          sublist = list()
          
          print(paste0("working on group: ", grplabels[i], 
                       ", subgroup: ", subgrplabels[j]))
          
          data %>% 
            filter(dob_yr %in% years) %>%
            filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
            group_by_(groups[i], subgroups[j]) %>%
            summarise_(
              Percent = interp(~ mean(var), var = as.name(indicator)),
              `Sample Size` = ~ n(),
              sd = interp(~ sd(var), var = as.name(indicator))
            ) %>%
            mutate(
              Tab = "Smallgroups",
              Year = paste0(min(years), "–", max(years)),
              Category1 = "",
              Group = "",
              Category1_grp = grplabels[i],
              Category2 = subgrplabels[j],
              se = sd / `Sample Size`,
              rse = ifelse(Percent > 0, se / Percent * 100, NA),
              Numerator = Percent * `Sample Size`,
              `Lower Bound` = Percent - z95 * se, `Upper Bound` = Percent + z95 * se,
              Suppress = ifelse(
                `Sample Size` < 50 | Numerator < 5, "Y",
                ifelse(rse >= 30, "N*", "N"))
            ) %>%
            rename_(Group_grp = groups[i], Subgroup = subgroups[j]) %>%
            select(
              Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
              `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress
            ) -> sublist
        }
        smlgrplist[[i]] <- sublist
      }
      smallgroups <- rbindlist(smlgrplist)
      return(smallgroups)
    }
    
    if (group %in% groups) {
      
      # Set up subgroups to cycle through
      subgroups <- groups[-which(groups == group)]
      # Set up subgroup labels
      subgrplabels <- grplabels[-which(groups == group)]
      
      # Make empty list to add data to
      smlgrplist = list()
      
      for (j in 1:length(subgroups)) {
        print(paste0("working on group: ",
                     group,
                     ": ",
                     subgroups[j]))
        
        data %>% 
          filter(dob_yr %in% years) %>%
          filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
          group_by_(group, subgroups[j]) %>%
          summarise_(
            Percent = interp(~ mean(var), var = as.name(indicator)),
            `Sample Size` = ~ n(),
            sd = interp(~ sd(var), var = as.name(indicator))
          ) %>%
          mutate(
            Tab = "Smallgroups",
            Year = paste0(min(years), "–", max(years)),
            Category1 = "",
            Group = "",
            Category1_grp = grplabels[which(groups == group)],
            Category2 = subgrplabels[j],
            se = sd / `Sample Size`,
            rse = ifelse(Percent > 0, se / Percent * 100, NA),
            Numerator = Percent * `Sample Size`,
            `Lower Bound` = Percent - z95 * se, `Upper Bound` = Percent + z95 * se,
            Suppress = ifelse(
              `Sample Size` < 50 | Numerator < 5, "Y",
              ifelse(rse >= 30, "N*", "N")
            )
          ) %>%
          rename_(Group_grp = group, Subgroup = subgroups[j]) %>%
          select(
            Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
            `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress
          ) -> smlgrplist[[j]]
      }
      smallgroups <- rbindlist(smlgrplist)
      return(smallgroups)
    }
    
    print("Nothing else worked")
  }


#### Look over time for JoinPoint ####
# Calculates yearly averages for KC overall
inddata_time_kc <- function(data, indicator, yrend = maxyr_b) {
  data %>% 
    filter_(interp(~dob_yr <= var, var = as.numeric(yrend))) %>%
    filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
    group_by(dob_yr) %>%
    summarise_(
      Percent = interp( ~ mean(var, na.rm = TRUE), var = as.name(indicator)),
      `Sample Size` = ~ n(),
      sd = interp( ~ sd(var, na.rm = TRUE), var = as.name(indicator))
    ) %>%
    mutate(
      Tab = "Trends_JP",
      Category1 = "King County",
      Group = "King County",
      Category1_grp = "King County",
      Group_grp = "King County",
      Category2 = "",
      Subgroup = "",
      se = sd / `Sample Size`,
      rse = ifelse(Percent > 0, se / Percent * 100, NA),
      Numerator = round(Percent * `Sample Size`, digits = 0),
      `Lower Bound` = Percent - z95 * se,
      `Upper Bound` = Percent + z95 * se,
      Suppress = ifelse(`Sample Size` < 50 |
                          Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
    ) %>%
    rename(Year = dob_yr) %>%
    select(
      Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
      `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress
    ) -> data
  return(data)
}

# Calculates yearly averages for each value within a group (e.g., race)
inddata_time_group <-
  function(data, indicator, yrend = maxyr_b, group = "all") {
    # List out subgroups to examine over
    groups <-
      c("race_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
    # List out group labels
    grplabels <-
      c("Mother's race/ethnicity", "Mother's age", "Mother's education", "HRA", "Region")
    
    
    if (group == "all") {
      # Make empty list to add data to
      subgrplist = list()
      
      for (i in 1:length(groups)) {
        print(paste0("working on ", grplabels[i]))
        
        data %>%
          filter_(interp(~dob_yr <= var, var = as.numeric(yrend))) %>%
          filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
          group_by_(~dob_yr, groups[i]) %>%
          summarise_(
            Percent = interp(~ mean(var), var = as.name(indicator)),
            `Sample Size` = ~ n(),
            sd = interp(~ sd(var), var = as.name(indicator))
          ) %>%
          mutate(
            Tab = "Trends_JP",
            Category1 = grplabels[i], 
            Category1_grp = "",
            Group_grp = "",
            Category2 = "",
            Subgroup = "", se = sd / `Sample Size`,
            rse = ifelse(Percent > 0, se / Percent * 100, NA),
            Numerator = Percent * `Sample Size`,
            `Lower Bound` = Percent - z95 * se, `Upper Bound` = Percent + z95 * se,
            Suppress = ifelse(
              `Sample Size` < 50 | Numerator < 5, "Y",
              ifelse(rse >= 30, "N*", "N")
            )
          ) %>%
          rename_(Year = ~dob_yr, Group = groups[i]) %>%
          select(
            Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
            `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress) %>%
        ungroup() -> subgrplist[[i]]
      }
      subgroups <- rbindlist(subgrplist)
      return(subgroups)
    }

    data %>%
      filter_(interp(~dob_yr <= var, var = as.numeric(yrend))) %>%
      filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
      group_by_(~dob_yr, interp(~var, var = as.name(group))) %>%
      summarise_(
        Percent = interp( ~ mean(var, na.rm = TRUE), var = as.name(indicator)),
        `Sample Size` = ~ n(),
        sd = interp( ~ sd(var, na.rm = TRUE), var = as.name(indicator))
      ) %>%
      mutate(
        Tab = "Trends_JP",
        Category1 = grplabels[which(groups == group)],
        Category1_grp = "",
        Group_grp = "",
        Category2 = "",
        Subgroup = "",
        se = sd / `Sample Size`,
        rse = ifelse(Percent > 0, se / Percent * 100, NA),
        Numerator = round(Percent * `Sample Size`, digits = 0),
        `Lower Bound` = Percent - z95 * se,
        `Upper Bound` = Percent + z95 * se,
        Suppress = ifelse(`Sample Size` < 50 |
                            Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
      ) %>%
      rename_(Year = ~dob_yr, Group = group) %>%
      select(
        Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
        `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress) %>%
      ungroup() -> data
    return(data)

    
  }

# Calculates yearly averages for each value within a group (e.g., race) using grouped race
inddata_time_group_grprace <-
  function(data, indicator, yrend = maxyr_b, group = "all") {
    # List out subgroups to examine over
    groups <-
      c("race_mom_grp", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
    # List out group labels
    grplabels <-
      c("Mother's race/ethnicity (grouped)", "Mother's age", "Mother's education", "HRA", "Region")
    
    
    if (group == "all") {
      # Make empty list to add data to
      subgrplist = list()
      
      for (i in 1:length(groups)) {
        print(paste0("working on ", grplabels[i]))
        
        data %>%
          filter_(interp(~dob_yr <= var, var = as.numeric(yrend))) %>%
          filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
          group_by_(~dob_yr, groups[i]) %>%
          summarise_(
            Percent = interp(~ mean(var), var = as.name(indicator)),
            `Sample Size` = ~ n(),
            sd = interp(~ sd(var), var = as.name(indicator))
          ) %>%
          mutate(
            Tab = "Trends_JP",
            Category1 = "",
            Group = "",
            Category1_grp = grplabels[i],
            Category2 = "",
            Subgroup = "", se = sd / `Sample Size`,
            rse = ifelse(Percent > 0, se / Percent * 100, NA),
            Numerator = Percent * `Sample Size`,
            `Lower Bound` = Percent - z95 * se, `Upper Bound` = Percent + z95 * se,
            Suppress = ifelse(
              `Sample Size` < 50 | Numerator < 5, "Y",
              ifelse(rse >= 30, "N*", "N")
            )
          ) %>%
          rename_(Year = ~dob_yr, Group_grp = groups[i]) %>%
          select(
            Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
            `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress) %>%
          ungroup() -> subgrplist[[i]]
      }
      subgroups <- rbindlist(subgrplist)
      return(subgroups)
    }
    
    data %>%
      filter_(interp(~dob_yr <= var, var = as.numeric(yrend))) %>%
      filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
      group_by_(~dob_yr, interp(~var, var = as.name(group))) %>%
      summarise_(
        Percent = interp( ~ mean(var, na.rm = TRUE), var = as.name(indicator)),
        `Sample Size` = ~ n(),
        sd = interp( ~ sd(var, na.rm = TRUE), var = as.name(indicator))
      ) %>%
      mutate(
        Tab = "Trends_JP",
        Category1 = "",
        Group = "",
        Category1_grp = grplabels[which(groups == group)],
        Category2 = "",
        Subgroup = "",
        se = sd / `Sample Size`,
        rse = ifelse(Percent > 0, se / Percent * 100, NA),
        Numerator = round(Percent * `Sample Size`, digits = 0),
        `Lower Bound` = Percent - z95 * se,
        `Upper Bound` = Percent + z95 * se,
        Suppress = ifelse(`Sample Size` < 50 |
                            Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
      ) %>%
      rename_(Year = ~dob_yr, Group_grp = group) %>%
      select(
        Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
        `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress) %>%
      ungroup() -> data
    return(data)
    
    
  }


#### Look over time with rolling averages ####
# Calculates rolling averages for KC overall
inddata_rollyr_kc <- function(data, indicator, yrstart = minyr_b, yrend = maxyr_b, yrcombine = 3) {
  
  # Need to add in checks about yrend >= yrstart and the maximum number of years that can be combined
  
  data2 <- data
  indicator2 <- indicator
  maxyr_b2 <- yrend
  
  indyr <- inddata_time_kc(data = data2, indicator = indicator2, yrend = maxyr_b2) %>%
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
      Percent = Numerator / `Sample Size`,
      sd = sqrt(
        ((Numerator * ((1 - Percent)^2)) +
           ((`Sample Size` - Numerator) * ((0 - Percent) ^2))) /
          (`Sample Size` - 1)),
      se = sd / `Sample Size`,
      rse = ifelse(Percent > 0, se / Percent * 100, NA),
      `Lower Bound` = Percent - z95 * se,
      `Upper Bound` = Percent + z95 * se,
      Suppress = ifelse(`Sample Size` < 50 |
                          Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
    ) %>%
    select(
      Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
      `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress
    ) -> data
  return(data)
}

# Calculates rolling averages for each value within a group (e.g., race)
inddata_rollyr_group <- function(data, indicator, yrstart = minyr_b, yrend = maxyr_b, yrcombine = 3, group = "all") {
  
  # Need to add in checks about yrend >= yrstart and the maximum number of years that can be combined
  
  data2 <- data
  indicator2 <- indicator
  maxyr_b2 <- yrend
  group2 <- group
  
  # Get each single year's data ready for combining
  indyr <- inddata_time_group(data = data2, indicator = indicator2, yrend = maxyr_b2, group = group2) %>%
    filter(Year >= yrstart & Year <= yrend) %>%
    select(Year, Category1:Subgroup, Numerator, `Sample Size`) %>%
    arrange(Category1, Group, Category1_grp, Group_grp,
            Category2, Subgroup, Year)

  
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
      Percent = Numerator / `Sample Size`,
      sd = sqrt(
        ((Numerator * ((1 - Percent)^2)) +
           ((`Sample Size` - Numerator) * ((0 - Percent) ^2))) /
          (`Sample Size` - 1)),
      se = sd / `Sample Size`,
      rse = ifelse(Percent > 0, se / Percent * 100, NA),
      `Lower Bound` = Percent - z95 * se,
      `Upper Bound` = Percent + z95 * se,
      Suppress = ifelse(`Sample Size` < 50 |
                          Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
    ) %>%
    select(
      Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
      `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress
    ) -> data
  return(data)
}

# Calculates rolling averages for each value within a group (e.g., race)
inddata_rollyr_group_grprace <- function(data, indicator, yrstart = minyr_b, yrend = maxyr_b, yrcombine = 3, group = "all") {
  
  # Need to add in checks about yrend >= yrstart and the maximum number of years that can be combined
  
  data2 <- data
  indicator2 <- indicator
  maxyr_b2 <- yrend
  group2 <- group
  
  # Get each single year's data ready for combining
  indyr <- inddata_time_group_grprace(data = data2, indicator = indicator2, yrend = maxyr_b2, group = group2) %>%
    filter(Year >= yrstart & Year <= yrend) %>%
    select(Year, Category1:Subgroup, Numerator, `Sample Size`) %>%
    arrange(Category1, Group, Category1_grp, Group_grp,
            Category2, Subgroup, Year)
  
  
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
      Percent = Numerator / `Sample Size`,
      sd = sqrt(
        ((Numerator * ((1 - Percent)^2)) +
           ((`Sample Size` - Numerator) * ((0 - Percent) ^2))) /
          (`Sample Size` - 1)),
      se = sd / `Sample Size`,
      rse = ifelse(Percent > 0, se / Percent * 100, NA),
      `Lower Bound` = Percent - z95 * se,
      `Upper Bound` = Percent + z95 * se,
      Suppress = ifelse(`Sample Size` < 50 |
                          Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
    ) %>%
    select(
      Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
      `Lower Bound`, `Upper Bound`, sd, se, rse, `Sample Size`, Numerator, Suppress
    ) -> data
  return(data)
}



#### H1: Preterm birth ####
preterm <- rbindlist(list(
  # KC overall
  inddata_kc(data = births, indicator = "preterm", year = maxyr_b),
  # By each subgroup
  inddata_subgroup(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "all"),
  inddata_subgroup_grprace(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "all"),
  # Crosstabs for each subgroup
  inddata_smallgroup(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "race_mom"),
  inddata_smallgroup(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "age_mom_grp"),
  inddata_smallgroup(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "educ_mom"),
  inddata_smallgroup(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "hra_id"),
  inddata_smallgroup(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "rgn_id"),
  inddata_smallgroup_grprace(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "race_mom_grp"),
  inddata_smallgroup_grprace(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "age_mom_grp"),
  inddata_smallgroup_grprace(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "educ_mom"),
  inddata_smallgroup_grprace(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "hra_id"),
  inddata_smallgroup_grprace(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "rgn_id"),
  # Time trend for KC overall
  inddata_rollyr_kc(data = births, indicator = "preterm", yrend = maxyr_b, yrcombine = 3),
  # Time trend for each subgroup
  inddata_rollyr_group(data = births, indicator = "preterm", yrend = maxyr_b, yrcombine = 3, group = "rgn_id"),
  inddata_rollyr_group(data = births, indicator = "preterm", yrend = maxyr_b, yrcombine = 3, group = "race_mom"),
  inddata_rollyr_group_grprace(data = births, indicator = "preterm", yrend = maxyr_b, yrcombine = 3, group = "rgn_id"),
  inddata_rollyr_group_grprace(data = births, indicator = "preterm", yrend = maxyr_b, yrcombine = 3, group = "race_mom_grp"),
    # Time trend for KC overall (JoinPoint data)
  inddata_time_kc(data = births, indicator = "preterm", yrend = maxyr_b),
  # Time trend for each subgroup (JoinPoint data)
  inddata_time_group(data = births, indicator = "preterm", yrend = maxyr_b, group = "rgn_id"),
  inddata_time_group(data = births, indicator = "preterm", yrend = maxyr_b, group = "race_mom"),
  inddata_time_group_grprace(data = births, indicator = "preterm", yrend = maxyr_b, group = "rgn_id"),
  inddata_time_group_grprace(data = births, indicator = "preterm", yrend = maxyr_b, group = "race_mom_grp")
))


### Convert to a df
preterm <- as.data.frame(preterm)
  
### Apply labels
preterm <- left_join(preterm, labels, by = c("Category1", "Group")) %>%
  rename(Group.id = Group, Group = Label)
preterm <- left_join(preterm, labels, by = c("Category1_grp" = "Category1", "Group_grp" = "Group")) %>%
  rename(Group_grp.id = Group_grp, Group_grp = Label)
preterm <- left_join(preterm, labels, by = c("Category2" = "Category1", "Subgroup" = "Group")) %>%
  rename(Subgroup.id = Subgroup, Subgroup = Label)

# Reorder and sort columns
preterm <- preterm %>% select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, 
                              Percent, `Lower Bound`, `Upper Bound`, sd, se, rse, 
                              `Sample Size`, Numerator, Suppress, Group.id, Subgroup.id) %>%
  arrange(Tab, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Year)

### Clean up nulls and blanks
preterm <- preterm %>%
  filter(!((Category1 != "" & is.na(Group)) | 
           (Category1_grp != "" & is.na(Group_grp)) |
           (Category2 != "" & is.na(Subgroup))))


### Add in comparisons with KC
# Pull out KC values
kclb <- as.numeric(preterm %>%
                     filter(Tab == "_King County") %>%
                     select(`Lower Bound`))
kcub <- as.numeric(preterm %>%
                     filter(Tab == "_King County") %>%
                     select(`Upper Bound`))
kctrend <- preterm %>%
  filter(Tab == "Trends" & Category1 == "King County") %>%
  select(Year, `Lower Bound`, `Upper Bound`)

# Do comparison for most recent year (i.e., not trends)
comparison <- preterm %>%
  filter(Tab != "Trends") %>%
  mutate(`Comparison with KC` = ifelse(`Upper Bound` < kclb, "lower",
                                       ifelse(`Lower Bound` > kcub, "higher", "no different"))) %>%
    select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, `Comparison with KC`)

# Do comparison for trends (each year compared to KC that year, not that group's trend over time)
comparison_trend <- preterm %>%
  filter(Tab == "Trends") %>%
  left_join(., kctrend, by = "Year") %>%
  mutate(`Comparison with KC` = ifelse(`Upper Bound.x` < `Lower Bound.y`, "lower",
                                       ifelse(`Lower Bound.x` > `Upper Bound.y`, "higher", "no different"))) %>%
  select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, `Comparison with KC`)

# Bring both comparisons together
comparison <- rbind(comparison, comparison_trend)

# Merge with main data and add column for time trends
preterm <- left_join(preterm, comparison, by = c("Tab", "Year", "Category1", "Group", "Category1_grp", 
                                                 "Group_grp", "Category2", "Subgroup")) %>%
  mutate(`Time Trends` = "")
rm(comparison, comparison_trend, kclb, kcub, kctrend)


### Export to an Excel file
preterm %>% filter(Tab != "Trends_JP") %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - preterm.xlsx"))


### Apply suppression rules and export
preterm %>% mutate_at(vars(Percent:rse, Numerator, `Comparison with KC`),
                      funs(ifelse(Suppress == "Y", NA, .))) %>%
  filter(Tab != "Trends_JP") %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - preterm - suppressed.xlsx"))


### Create data sets for Joinpoint
preterm %>%
  filter(Tab == "Trends_JP" & Category1 != "") %>%
  select(Tab, Category1, Group, Year, Percent, se) %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - preterm - JP.xlsx"))

preterm %>%
  filter(Tab == "Trends_JP" & Category1_grp != "") %>%
  select(Tab, Category1_grp, Group_grp, Year, Percent, se) %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - preterm grouped race - JP.xlsx"))




#### S1: Breastfeeding initiation ####
breastfed <- rbindlist(list(
  # KC overall
  inddata_kc(data = births, indicator = "breastfed", year = maxyr_b),
  # By each subgroup
  inddata_subgroup(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "all"),
  inddata_subgroup_grprace(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "all"),
  # Crosstabs for each subgroup
  inddata_smallgroup(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "race_mom"),
  inddata_smallgroup(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "age_mom_grp"),
  inddata_smallgroup(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "educ_mom"),
  inddata_smallgroup(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "hra_id"),
  inddata_smallgroup(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "rgn_id"),
  inddata_smallgroup_grprace(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "race_mom_grp"),
  inddata_smallgroup_grprace(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "age_mom_grp"),
  inddata_smallgroup_grprace(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "educ_mom"),
  inddata_smallgroup_grprace(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "hra_id"),
  inddata_smallgroup_grprace(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "rgn_id"),
  # Time trend for KC overall
  inddata_rollyr_kc(data = births, indicator = "breastfed", yrend = maxyr_b, yrcombine = 3),
  # Time trend for each subgroup
  inddata_rollyr_group(data = births, indicator = "breastfed", yrend = maxyr_b, yrcombine = 3, group = "rgn_id"),
  inddata_rollyr_group(data = births, indicator = "breastfed", yrend = maxyr_b, yrcombine = 3, group = "race_mom"),
  inddata_rollyr_group_grprace(data = births, indicator = "breastfed", yrend = maxyr_b, yrcombine = 3, group = "rgn_id"),
  inddata_rollyr_group_grprace(data = births, indicator = "breastfed", yrend = maxyr_b, yrcombine = 3, group = "race_mom_grp"),
  # Time trend for KC overall (JoinPoint data)
  inddata_time_kc(data = births, indicator = "breastfed", yrend = maxyr_b),
  # Time trend for each subgroup (JoinPoint data)
  inddata_time_group(data = births, indicator = "breastfed", yrend = maxyr_b, group = "rgn_id"),
  inddata_time_group(data = births, indicator = "breastfed", yrend = maxyr_b, group = "race_mom"),
  inddata_time_group_grprace(data = births, indicator = "breastfed", yrend = maxyr_b, group = "rgn_id"),
  inddata_time_group_grprace(data = births, indicator = "breastfed", yrend = maxyr_b, group = "race_mom_grp")
))


### Convert to a df
breastfed <- as.data.frame(breastfed)

### Apply labels
breastfed <- left_join(breastfed, labels, by = c("Category1", "Group")) %>%
  rename(Group.id = Group, Group = Label)
breastfed <- left_join(breastfed, labels, by = c("Category1_grp" = "Category1", "Group_grp" = "Group")) %>%
  rename(Group_grp.id = Group_grp, Group_grp = Label)
breastfed <- left_join(breastfed, labels, by = c("Category2" = "Category1", "Subgroup" = "Group")) %>%
  rename(Subgroup.id = Subgroup, Subgroup = Label)

# Reorder and sort columns
breastfed <- breastfed %>% select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, 
                              Percent, `Lower Bound`, `Upper Bound`, sd, se, rse, 
                              `Sample Size`, Numerator, Suppress, Group.id, Subgroup.id) %>%
  arrange(Tab, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Year)

### Clean up nulls and blanks
breastfed <- breastfed %>%
  filter(!((Category1 != "" & is.na(Group)) | 
             (Category1_grp != "" & is.na(Group_grp)) |
             (Category2 != "" & is.na(Subgroup))))


### Add in comparisons with KC
# Pull out KC values
kclb <- as.numeric(breastfed %>%
                     filter(Tab == "_King County") %>%
                     select(`Lower Bound`))
kcub <- as.numeric(breastfed %>%
                     filter(Tab == "_King County") %>%
                     select(`Upper Bound`))
kctrend <- breastfed %>%
  filter(Tab == "Trends" & Category1 == "King County") %>%
  select(Year, `Lower Bound`, `Upper Bound`)

# Do comparison for most recent year (i.e., not trends)
comparison <- breastfed %>%
  filter(Tab != "Trends") %>%
  mutate(`Comparison with KC` = ifelse(`Upper Bound` < kclb, "lower",
                                       ifelse(`Lower Bound` > kcub, "higher", "no different"))) %>%
  select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, `Comparison with KC`)

# Do comparison for trends (each year compared to KC that year, not that group's trend over time)
comparison_trend <- breastfed %>%
  filter(Tab == "Trends") %>%
  left_join(., kctrend, by = "Year") %>%
  mutate(`Comparison with KC` = ifelse(`Upper Bound.x` < `Lower Bound.y`, "lower",
                                       ifelse(`Lower Bound.x` > `Upper Bound.y`, "higher", "no different"))) %>%
  select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, `Comparison with KC`)

# Bring both comparisons together
comparison <- rbind(comparison, comparison_trend)

# Merge with main data and add column for time trends
breastfed <- left_join(breastfed, comparison, by = c("Tab", "Year", "Category1", "Group", "Category1_grp", 
                                                 "Group_grp", "Category2", "Subgroup")) %>%
  mutate(`Time Trends` = "")
rm(comparison, comparison_trend, kclb, kcub, kctrend)


### Export to an Excel file
breastfed %>% filter(Tab != "Trends_JP") %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - breastfed.xlsx"))


### Apply suppression rules and export
breastfed %>% mutate_at(vars(Percent:rse, Numerator, `Comparison with KC`),
                      funs(ifelse(Suppress == "Y", NA, .))) %>%
  filter(Tab != "Trends_JP") %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - breastfed - suppressed.xlsx"))


### Create data sets for Joinpoint
breastfed %>%
  filter(Tab == "Trends_JP" & Category1 != "") %>%
  select(Tab, Category1, Group, Year, Percent, se) %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - breastfed - JP.xlsx"))

breastfed %>%
  filter(Tab == "Trends_JP" & Category1_grp != "") %>%
  select(Tab, Category1_grp, Group_grp, Year, Percent, se) %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - breastfed grouped race - JP.xlsx"))








#### E1: Smoked during pregnancy ####
smoked <- rbindlist(list(
  # KC overall
  inddata_kc(data = births, indicator = "smoked", year = maxyr_b),
  # By each subgroup
  inddata_subgroup(data = births, indicator = "smoked", year = maxyr_b, group = "all"),
  # Crosstabs for each subgroup
  inddata_smallgroup(data = births, indicator = "smoked", year = maxyr_b, group = "race_mom"),
  inddata_smallgroup(data = births, indicator = "smoked", year = maxyr_b, group = "age_mom_grp"),
  inddata_smallgroup(data = births, indicator = "smoked", year = maxyr_b, group = "educ_mom"),
  inddata_smallgroup(data = births, indicator = "smoked", year = maxyr_b, group = "hra_id"),
  inddata_smallgroup(data = births, indicator = "smoked", year = maxyr_b, group = "rgn_id"),
  # Time trend for KC overall
  inddata_time_kc(data = births, indicator = "smoked"),
  # Time trend for each subgroup
  inddata_time_group(data = births, indicator = "smoked")
))

# Covnert to a df
smoked <- as.data.frame(smoked)

# Apply labels
smoked <- left_join(smoked, labels, by = c("Category1", "Group")) %>%
  rename(Group.id = Group, Group = Label)
smoked <- left_join(smoked, labels, by = c("Category2" = "Category1", "Subgroup" = "Group")) %>%
  rename(Subgroup.id = Subgroup, Subgroup = Label)

# Reorder columns
smoked <- smoked %>% select(Tab, Year, Category1, Group, Category2, Subgroup, Percent, `Lower Bound`, `Upper Bound`,
                              sd, se, rse, `Sample Size`, Numerator, Suppress, Group.id, Subgroup.id)

smoked %>% filter(Tab == "Trends" & (Category1 == "King County" | Group == "Native American")) %>% select(Year, Category1, Group, Percent:Numerator)
smoked %>% filter(Tab == "Trends" & (Category1 == "King County" | Category1 == "Mother's race/ethnicity")) %>% select(Year, Category1, Group, Percent:Numerator)





############### TESTING AREA #######################
# List out groups to examine over
groups <- c("race_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
groupsl <- list(c("race_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id"))
# List out group labels
grplabels <-c("Race/ethnicity", "Mother's age", "Mother's education", "HRA", "Region")



# try using map from purrr package
subgroup_test <- function(data, indicator, year, group = "all") {
  print(paste0("working on ", groups))
  
  births %>% 
    filter(dob_yr == maxyr_b & !is.na(indicator)) %>%
     group_by(groups) %>%
    summarise_(
      Percent = map( mean(indicator)),
      sd = map(sd(indicator))

}

# standard to test against
births %>% group_by(race_mom) %>% summarise(pct = mean(preterm, na.rm = TRUE))
births %>% group_by(age_mom_grp) %>% summarise(pct = mean(preterm, na.rm = TRUE))


births %>% map_df()


test_sum <- function(df, indicator) {
  df %>%
  filter_(!is.na(indicator)) %>%
  summarise(
    Percent = mean(indicator, na.rm = TRUE),
    `Sample Size` = n(),
    sd = sd(indicator)
  )
}

test_filter <- function(df, indicator) {
  df %>%
    filter_(!is.na(indicator)) -> df
  return(df)
}


# try to get lapply working on the subgroups
subgroup_test <- function(data, indicator, year, group = "all") {
      print(paste0("working on ", groups))
        
        data %>% 
          filter(dob_yr %in% years) %>%
          filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
          group_by_(groups) %>%
          summarise_(
            Percent = interp(~ mean(var), var = as.name(indicator)),
            `Sample Size` = ~ n(),
            sd = interp(~ sd(var), var = as.name(indicator))
          ) %>%
          mutate(
            Tab = "Subgroups", Year = paste0(min(years), "–", max(years)), Category1 = groups, Category2 = "",
            Subgroup = "", se = sd / `Sample Size`,
            rse = ifelse(Percent > 0, se / Percent * 100, NA),
            Numerator = Percent * `Sample Size`,
            `Lower Bound` = Percent - z95 * se, `Upper Bound` = Percent + z95 * se,
            Suppress = ifelse(
              `Sample Size` < 50 | Numerator < 5, "Y",
              ifelse(rse >= 30, "N*", "N")
            )
          ) %>%
          rename_(Group = groups) %>%
          select(Tab, Year, Category1, Group, Category2, Subgroup, Percent, `Lower Bound`, `Upper Bound`,
                 sd, se, rse, `Sample Size`, Numerator, Suppress)
    }

lapply(groups, subgroup_test, data = births, indicator = "preterm", year = maxyr_b)




# try to use lapply in place of the nested loop
inddata_smallgroup <-
  function(data, indicator, year, group = "all") {
    # List out groups to examine over
    groups <-
      c("race_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
    # List out group labels
    grplabels <-
      c("Race/ethnicity", "Mother's age", "Mother's education", "HRA", "Region")
    
    if (group == "all") {
      # Make empty list to add data to
      smlgrplist = list()
      
      for (i in 1:length(groups)) {
        print(paste0("working on group ", grplabels[i]))
        
        # Set up subgroups to cycle through
        subgroups <- groups[-i]
        # Set up subgroup labels
        subgrplabels <- grplabels[-i]
        
        for (j in 1:length(subgroups)) {
          # Make empty sublist to add data to
          sublist = list()
          
          print(paste0("working on group: ", grplabels[i], 
                       ", subgroup: ", subgrplabels[j]))
          
          data %>% 
                filter(dob_yr %in% years) %>%
            filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
            group_by_(groups[i], subgroups[j]) %>%
            summarise_(
              Percent = interp(~ mean(var), var = as.name(indicator)),
              `Sample Size` = ~ n(),
              sd = interp(~ sd(var), var = as.name(indicator))
            ) %>%
            mutate(
              Tab = "Smallgroups",
              Year = paste0(min(years), "–", max(years)),
              Category1 = grplabels[i], 
              Category2 = subgrplabels[j],
              se = sd / `Sample Size`,
              rse = ifelse(Percent > 0, se / Percent * 100, NA),
              Numerator = Percent * `Sample Size`,
              #`Lower Bound` = Percent - z95 * se, `Upper Bound` = Percent + z95 * se,
              Suppress = ifelse(
                `Sample Size` < 50 | Numerator < 5, "Y",
                ifelse(rse >= 30, "N*", "N")),
              source = paste0(i,"+",j)
            ) %>%
            rename_(Group = groups[i], Subgroup = subgroups[j]) %>%
            select(Tab, Year, Category1, Group, Category2, Subgroup, Percent, #`Lower Bound`, `Upper Bound`,
                   sd, se, rse, `Sample Size`, Numerator, Suppress, source
            ) -> sublist[[j]]
          print(sublist)
          
        }
        smlgrplist[[i]] <- rbindlist(sublist)
        print("Printing smlgrplist as it currently stands")
        print(head(smlgrplist))
      }
      smallgroups <- rbindlist(smlgrplist)
      return(smallgroups)
    }
  }








inddata_smallgroup <-
  function(data, indicator, year, group = "all") {
    # List out groups to examine over
    groups <-
      c("race_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
    # List out group labels
    grplabels <-
      c("Race/ethnicity", "Mother's age", "Mother's education", "HRA", "Region")
    
    if (group == "all") {
      # Make empty list to add data to
      smlgrplist = list()
      
      for (i in 1:length(groups)) {
        print(paste0("working on group ", grplabels[i]))
        
        # Set up subgroups to cycle through
        subgroups <- groups[-i]
        # Set up subgroup labels
        subgrplabels <- grplabels[-i]
        
        for (j in 1:length(subgroups)) {
          # Make empty sublist to add data to
          sublist = list()
          
          print(paste0("working on group: ", grplabels[i], 
                       ", subgroup: ", subgrplabels[j]))
          
          data %>% 
                filter(dob_yr %in% years) %>%
            filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
            group_by_(groups[i], subgroups[j]) %>%
            summarise_(
              Percent = interp(~ mean(var), var = as.name(indicator)),
              `Sample Size` = ~ n(),
              sd = interp(~ sd(var), var = as.name(indicator))
            ) %>%
            mutate(
              Tab = "Smallgroups",
              Year = paste0(min(years), "–", max(years)),
              Category1 = grplabels[i], 
              Category2 = subgrplabels[j],
              se = sd / `Sample Size`,
              rse = ifelse(Percent > 0, se / Percent * 100, NA),
              Numerator = Percent * `Sample Size`,
              #`Lower Bound` = Percent - z95 * se, `Upper Bound` = Percent + z95 * se,
              Suppress = ifelse(
                `Sample Size` < 50 | Numerator < 5, "Y",
                ifelse(rse >= 30, "N*", "N")),
              source = paste0(i,"+",j)
            ) %>%
            rename_(Group = groups[i], Subgroup = subgroups[j]) %>%
            select(Tab, Year, Category1, Group, Category2, Subgroup, Percent, #`Lower Bound`, `Upper Bound`,
                   sd, se, rse, `Sample Size`, Numerator, Suppress, source
            ) -> sublist[[j]]
          print(sublist)

        }
        smlgrplist[[i]] <- rbindlist(sublist)
        print("Printing smlgrplist as it currently stands")
        print(head(smlgrplist))
      }
      smallgroups <- rbindlist(smlgrplist)
      return(smallgroups)
    }
  }











  births %>% filter(dob_yr == maxyr_b) %>% 
    group_by(age_mom_grp) %>%
    summarise(Percent = as.numeric(mean(preterm)), `Sample Size` = as.numeric(n()),
              sd = as.numeric(sd(preterm))) %>%
    mutate(Tab = "Subgroups", Year = maxyr_b, Category1 = "Race/Ethnicity",  
           Category2 = "", Subgroup = "", se = sd/`Sample Size`,
           rse = ifelse(Percent > 0, se/Percent*100, NA), Numerator = Percent * `Sample Size`,
           `Lower Bound` = Percent - z95 * se, `Upper Bound` = Percent + z95 * se,
           Suppress = ifelse(`Sample Size` < 50 | Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))) %>%
    rename(Group = age_mom_grp) %>%
    select(Tab, Year, Category1, Group, Subgroup, Percent, `Lower Bound`, `Upper Bound`, sd, se, rse,
           `Sample Size`, Numerator, Suppress)

  
  births %>% 
   # filter(dob_yr == maxyr_b) %>% 
    filter(!is.na(preterm)) %>%
    group_by(rgn_id) %>%
    summarise(Percent = as.numeric(mean(preterm, na.rm = TRUE)), `Sample Size` = as.numeric(n()),
              sd = as.numeric(sd(preterm))) %>%
    mutate(Tab = "Subgroups", Year = maxyr_b, Category1 = "Race/Ethnicity",  
           Category2 = "", Subgroup = "", se = sd/`Sample Size`,
           rse = ifelse(Percent > 0, se/Percent*100, NA), Numerator = Percent * `Sample Size`,
           `Lower Bound` = Percent - z95 * se, `Upper Bound` = Percent + z95 * se,
           Suppress = ifelse(`Sample Size` < 50 | Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))) %>%
    rename(Group = rgn_id) %>%
    select(Tab, Year, Category1, Group, Subgroup, Percent, `Lower Bound`, `Upper Bound`, sd, se, rse,
           `Sample Size`, Numerator, Suppress)
  

births %>% 
   filter(dob_yr == 2014) %>% 
  filter(!is.na(rgn_id)) %>%
    filter(!is.na(preterm)) %>%
    group_by(rgn_id) %>%
    summarise(Percent = as.numeric(mean(preterm, na.rm = TRUE)))
  
  
# generate code to test error/bug
set.seed(753)
testdf <- as.data.frame(cbind(region = rep(1:4, each = 100),
                              region2 = head(births$rgn_id, n = 400),
                             year = rep(rep(2014:2015, each = 50), times = 4),
                             preterm = rbinom(400, 1, 0.0853)))

testdf %>% group_by(region) %>% summarise(Percent = mean(preterm))
testdf %>% filter(year == maxyr_b) %>% group_by(region2) %>% summarise(percent = mean(preterm))


############### END TESTING AREA #######################


