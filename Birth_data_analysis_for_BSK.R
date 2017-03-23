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


#### Connect to SQL data ####
db.apde50 <- odbcConnect("PH_APDEStore")

# Bring in births data and join with geocoding tables
births <-
  sqlQuery(
    db.apde50,
    "SELECT b1.certno_e, dob_yr, age_mom, cnty_res, race_mom, educ_mom,
      pnatalmo, gestcalc, breastfd, pnatalvs, cigs_bef, 
      cigs_1st, cigs_2nd, cigs_3rd, g1.year, g1.geo_id_blk10, g1.cnty_res2,
      g1.match_score, g2.hra_id, g2.hra_id, g2.rgn_id, g2.rgn_name
    FROM 
    dbo.wabir2003_2015 b1
    LEFT JOIN zztest.geobir1990_2015 g1
    ON b1.certno_e = g1.certno_e
    LEFT JOIN dbo.geocomp_blk10 g2
    ON g1.geo_id_blk10 = g2.geo_id_blk10
    ",
    stringsAsFactors = FALSE
  )


# Backup data to avoid rereading
births.bk <- births


#### Restrict dataset to King County ####
births <- filter(births, cnty_res == 17)


#### Make new variables and do recodes ####
# Clean up missing data
births$pnatalmo[births$pnatalmo == 999] <- NA
births$gestcalc[births$gestcalc == 99] <- NA

# Mother's age groupings
births$age_mom_grp <- car::recode(births$age_mom, "10:17 = 1; 18:24 = 2; 25:34 = 3;
                            35:44 = 4; 45:hi = 5; else = NA")

# Mother's grouped race/ethnicity
births$race_mom_grp <- as.character(car::recode(births$race_mom, "'1' = 1; '2' = 2; 
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


# Removes whitespace from HRA and region names
births$hra_id <- trimws(births$hra_id)
births$rgn_name <- trimws(births$rgn_name)


#### Set up other parameters ####
minyr_b = min(births$dob_yr) # pulls out oldest recent year for which data are present
maxyr_b = max(births$dob_yr) # pulls out most recent year for which data are present

# Folder to write output to
filepath = "S:/WORK/Best Start for Kids/Dashboard/Interim dashboard materials/Tableau docs/"


#### Set up labels for each variable ####
# Make a data frame that can merge with created tables to label values
# Make a data frame that can merge with created tables to label values
labels <- rbind.data.frame(
  cbind(Category1 = "King County", Group = "King County", Label = "King County"),
  cbind(Category1 = "Overall", Group = "Overall", Label = "Overall"),
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



#### Set up outcome variables ####
# Headline indicators:
# H1) Preterm birth
# H2) Infant mortality (uses different data file)

# Secondary/intermediate indicators
# S1) Breatfeeding initiation
# S2) Early and adequate prenatal care
# S3) Adolescent birth rate (15-17) (needs pop data for denominator)


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
    teenbir = ifelse(is.na(age_mom), NA, ifelse(age_mom >= 15 & age_mom <= 17, 1, 0)),
    # E1: smoked during pregnancy
    smoked = rowSums(.[, c("cigs_bef", "cigs_1st", "cigs_2nd", "cigs_3rd")], na.rm = FALSE),
         smoked = car::recode(smoked, "1:hi = 1; 0 = 0; else = NA"))



#### Define functions for recent years ####

# Gives overall KC average
indicator_kc <- function(data, indicator, year = maxyr_b, yrcombine = 5) {
  # Make range of years to look over
  years <- seq(year - yrcombine + 1, year)
  
  data %>% 
    filter(dob_yr %in% years) %>%
    filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
    summarise_(
      Percent = interp( ~ mean(var, na.rm = TRUE), var = as.name(indicator)),
      Numerator = interp( ~ sum(var, na.rm = TRUE), var = as.name(indicator)),
      `Sample Size` = ~ n()
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
      se = sqrt(Numerator / (`Sample Size`^2)),
      rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
      `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size`,
      `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size`,
      Suppress = ifelse(`Sample Size` < 50 |
                          Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
    ) %>%
    select(
      Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
      `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress
    ) -> data
    return(data)
}

# Looks at each value within a group (e.g., race)
indicator_subgroup <-
  function(data, indicator, year = maxyr_b, yrcombine = 5, group = "all", grprace = FALSE, smlgrp = FALSE) {
    # Make range of years to look over
    years <- seq(year - yrcombine + 1, year)
    
    if (grprace == TRUE) {
    # List out subgroups to examine over
    groups <-
      c("race_mom_grp", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
    # List out group labels
    grplabels <-
      c("Mother's race/ethnicity (grouped)", "Mother's age", "Mother's education", "HRA", "Region")
    } else {
    # List out subgroups to examine over
    groups <-
      c("race_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
    # List out group labels
    grplabels <-
      c("Mother's race/ethnicity", "Mother's age", "Mother's education", "HRA", "Region")
    }

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
            `Sample Size` = ~ n()
          ) %>%
          mutate(
            Tab = "Subgroups",
            Year = paste0(min(years), "–", max(years)),
            Category1 = grplabels[i],
            Category1_grp = "",
            Group_grp = "",
            Category2 = "",
            Subgroup = "",
            se = sqrt(Numerator / (`Sample Size`^2)),
            rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
            Suppress = ifelse(`Sample Size` < 50 |
                                Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
          ) %>%
          # need to use rowwise for poisson.test to work
          rowwise() %>%
          mutate(
            `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size`,
            `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size`) %>%
          ungroup() %>%
          rename_(Group = groups[i]) %>%
          select(
            Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
            `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress) -> subgrplist[[i]]
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
      # NB. Need to figure out why the df is not printed if grprace == TRUE but R will write to a df ok
    }
    
    data %>% 
      filter(dob_yr %in% years) %>%
      filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
      group_by_(interp(~var, var = as.name(group))) %>%
      summarise_(
        Percent = interp( ~ mean(var, na.rm = TRUE), var = as.name(indicator)),
        Numerator = interp( ~ sum(var, na.rm = TRUE), var = as.name(indicator)),
        `Sample Size` = ~ n()
      ) %>%
      mutate(
        Tab = "Subgroups",
        Year = paste0(min(years), "–", max(years)),
        Category1 = grplabels[which(groups == group)],
        Category1_grp = "",
        Group_grp = "",
        Category2 = "",
        Subgroup = "",
        se = sqrt(Numerator / (`Sample Size`^2)),
        rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
        Suppress = ifelse(`Sample Size` < 50 |
                            Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
      ) %>%
      # need to use rowwise for poisson.test to work
      rowwise() %>%
      mutate(
        `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size`,
        `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size`) %>%
      ungroup() %>%
      rename_(Group = group) %>%
      select(
        Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
        `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress) -> data
    
    if (grprace == TRUE) {
      data <- data %>%
        mutate(Category1_grp = Category1,
               Group_grp = Group,
               Category1 = "",
               Group = "")
    }
    
    if (smlgrp == TRUE) {
      data <- data %>%
        mutate(Tab = "Smallgroups",
               Category2 = as.character("Overall"),
               Subgroup = as.character("Overall"))
    }
    
    return(data)
  }


# Creates crosstabs for a group and subgroup (e.g., race by region)
indicator_smallgroup <-
  function(data, indicator, year = maxyr_b, yrcombine = 5, group = "all", grprace = FALSE) {
    # Make range of years to look over
    years <- seq(year - yrcombine + 1, year)
    
    if (grprace == TRUE) {
      # List out subgroups to examine over
      groups <-
        c("race_mom_grp", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
      # List out group labels
      grplabels <-
        c("Mother's race/ethnicity (grouped)", "Mother's age", "Mother's education", "HRA", "Region")
    } else {
      # List out subgroups to examine over
      groups <-
        c("race_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
      # List out group labels
      grplabels <-
        c("Mother's race/ethnicity", "Mother's age", "Mother's education", "HRA", "Region")
    }
    
    
    if (group == "all") {
      # Make empty list to add data to
      smlgrplist = list()
      
      for (i in 1:length(groups)) {
        print(paste0("working on group ", grplabels[i]))
        
        # Set up subgroups to cycle through
        subgroups <- groups[-i]
        # Set up subgroup labels
        subgrplabels <- grplabels[-i]
        
        # Make empty sublist to add data to
        sublist = list()
        
        for (j in 1:length(subgroups)) {
          print(paste0("working on group: ", grplabels[i],  ", subgroup: ", subgrplabels[j]))
          
          data %>% 
            filter(dob_yr %in% years) %>%
            filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
            group_by_(groups[i], subgroups[j]) %>%
            summarise_(
              Percent = interp( ~ mean(var, na.rm = TRUE), var = as.name(indicator)),
              Numerator = interp( ~ sum(var, na.rm = TRUE), var = as.name(indicator)),
              `Sample Size` = ~ n()
            ) %>%
            mutate(
              Tab = "Smallgroups",
              Year = paste0(min(years), "–", max(years)),
              Category1 = grplabels[i],
              Category1_grp = "",
              Group_grp = "",
              Category2 = subgrplabels[j],
              se = sqrt(Numerator / (`Sample Size`^2)),
              rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
              Suppress = ifelse(`Sample Size` < 50 | Numerator < 5, "Y",
                                ifelse(rse >= 30, "N*", "N"))
            ) %>%
            # need to use rowwise for poisson.test to work
            rowwise() %>%
            mutate(
              `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size`,
              `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size`) %>%
            ungroup() %>%
            rename_(Group = groups[i], Subgroup = subgroups[j]) %>%
            select(
              Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
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
    
    if (group %in% groups) {
      
      # Set up subgroups to cycle through
      subgroups <- groups[-which(groups == group)]
      # Set up subgroup labels
      subgrplabels <- grplabels[-which(groups == group)]
      
      # Make empty list to add data to
      smlgrplist = list()
      
      for (j in 1:length(subgroups)) {
        print(paste0("working on group: ", group, ": ", subgroups[j]))
        
        data %>% 
          filter(dob_yr %in% years) %>%
          filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
          group_by_(group, subgroups[j]) %>%
          summarise_(
            Percent = interp( ~ mean(var, na.rm = TRUE), var = as.name(indicator)),
            Numerator = interp( ~ sum(var, na.rm = TRUE), var = as.name(indicator)),
            `Sample Size` = ~ n()
          ) %>%
          mutate(
            Tab = "Smallgroups",
            Year = paste0(min(years), "–", max(years)),
            Category1 = grplabels[which(groups == group)],
            Category1_grp = "",
            Group_grp = "",
            Category2 = subgrplabels[j],
            se = sqrt(Numerator / (`Sample Size`^2)),
            rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
            Suppress = ifelse(`Sample Size` < 50 |
                                Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
          ) %>%
          # need to use rowwise for poisson.test to work
          rowwise() %>%
          mutate(
            `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size`,
            `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size`) %>%
          ungroup() %>%
          rename_(Group = group, Subgroup = subgroups[j]) %>%
          select(
            Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
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
indicator_time_kc <- function(data, indicator, yrstart = minyr_b, yrend = maxyr_b) {
  data %>% 
    filter_(interp(~dob_yr <= var, var = as.numeric(yrend))) %>%
    filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
    group_by(dob_yr) %>%
    summarise_(
      Percent = interp( ~ mean(var, na.rm = TRUE), var = as.name(indicator)),
      Numerator = interp( ~ sum(var, na.rm = TRUE), var = as.name(indicator)),
      `Sample Size` = ~ n()
    ) %>%
    mutate(
      Tab = "Trends_JP",
      Category1 = "King County",
      Group = "King County",
      Category1_grp = "King County",
      Group_grp = "King County",
      Category2 = "",
      Subgroup = "",
      se = sqrt(Numerator / (`Sample Size`^2)),
      rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
      Suppress = ifelse(`Sample Size` < 50 |
                          Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
    ) %>%
    # need to use rowwise for poisson.test to work
    rowwise() %>%
    mutate(
      `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size`,
      `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size`) %>%
    ungroup() %>%
    rename(Year = dob_yr) %>%
    select(
      Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
      `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress
    ) -> data
  return(data)
}


# Calculates yearly averages for each value within a group (e.g., race)
indicator_time_group <-
  function(data, indicator, yrstart = minyr_b, yrend = maxyr_b, group = "all", grprace = FALSE) {
    
    if (grprace == TRUE) {
      # List out subgroups to examine over
      groups <-
        c("race_mom_grp", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
      # List out group labels
      grplabels <-
        c("Mother's race/ethnicity (grouped)", "Mother's age", "Mother's education", "HRA", "Region")
    } else {
      # List out subgroups to examine over
      groups <-
        c("race_mom", "age_mom_grp", "educ_mom", "hra_id", "rgn_id")
      # List out group labels
      grplabels <-
        c("Mother's race/ethnicity", "Mother's age", "Mother's education", "HRA", "Region")
    }
    
    
    if (group == "all") {
      # Make empty list to add data to
      subgrplist = list()
      
      for (i in 1:length(groups)) {
        print(paste0("working on ", grplabels[i]))
        
        # Need to make a frame containing all possible years for each group 
        # so that rolling averages combine properly
        yearframe <- data.frame("Year" = rep(seq(yrstart, yrend), each = length(unique(data[,groups[i]]))),
                                "Group" = rep(unique(data[,groups[i]]), yrend - yrstart + 1),
                                stringsAsFactors = FALSE)
        
        data %>%
          filter_(interp(~dob_yr <= var, var = as.numeric(yrend))) %>%
          filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
          group_by_(~dob_yr, groups[i]) %>%
          summarise_(
            Percent = interp( ~ mean(var, na.rm = TRUE), var = as.name(indicator)),
            Numerator = interp( ~ sum(var, na.rm = TRUE), var = as.name(indicator)),
            `Sample Size` = ~ n()
          ) %>%
          rename_(Year = ~dob_yr, Group = groups[i]) %>%
          right_join(., yearframe, by = c("Year", "Group")) %>%
          mutate(
            Tab = "Trends_JP",
            Category1 = grplabels[i], 
            Category1_grp = "",
            Group_grp = "",
            Category2 = "",
            Subgroup = "",
            se = sqrt(Numerator / (`Sample Size`^2)),
            rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
            Suppress = ifelse(`Sample Size` < 50 |
                                Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
          ) %>%
          # Need to set any missing numerator data to 0 for poisson.test to work
          # Missing data occurs because there were no events that year so 0 is appropriate
          mutate(naflag = ifelse(is.na(Numerator), 1, 0),
                 Numerator = as.numeric(ifelse(is.na(Numerator), 0, Numerator))) %>%
          rowwise() %>%
          mutate(
            `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size`,
            `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size`) %>%
          ungroup() %>%
          select(
            Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
            `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress) -> subgrplist[[i]]
      }
      subgroups <- rbindlist(subgrplist)
      
      if (grprace == TRUE) {
        subgroups <- subgroups %>%
          mutate(Category1_grp = Category1,
                 Group_grp = Group,
                 Category1 = "",
                 Group = "")
      }
      
      return(subgroups)
    }

    if (group %in% groups) {
      # Need to make a frame containing all possible years for each group 
      # so that rolling averages combine properly
      yearframe <- data.frame("Year" = rep(seq(yrstart, yrend), each = length(unique(data[,group]))),
                              "Group" = rep(unique(data[,group]), yrend - yrstart + 1),
                              stringsAsFactors = FALSE)
      
      data %>%
        filter_(interp(~dob_yr <= var, var = as.numeric(yrend))) %>%
        filter_(interp(~!is.na(var), var = as.name(indicator))) %>%
        group_by_(~dob_yr, interp(~var, var = as.name(group))) %>%
        summarise_(
          Percent = interp( ~ mean(var, na.rm = TRUE), var = as.name(indicator)),
          Numerator = interp( ~ sum(var, na.rm = TRUE), var = as.name(indicator)),
          `Sample Size` = ~ n()
        ) %>%
        rename_(Year = ~dob_yr, Group = group) %>%
        right_join(., yearframe, by = c("Year", "Group")) %>%
        mutate(
          Tab = "Trends_JP",
          Category1 = grplabels[which(groups == group)],
          Category1_grp = "",
          Group_grp = "",
          Category2 = "",
          Subgroup = "",
          se = sqrt(Numerator / (`Sample Size`^2)),
          rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
         Suppress = ifelse(`Sample Size` < 50 |
                             Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
        ) %>%
        # Need to set any missing numerator data to 0 for poisson.test to work
        # Missing data occurs because there were no events that year so 0 is appropriate
        mutate(naflag = ifelse(is.na(Numerator), 1, 0),
               Numerator = as.numeric(ifelse(is.na(Numerator), 0, Numerator))) %>%
        # need to use rowwise for poisson.test to work
        rowwise() %>%
        mutate(
          `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size`,
          `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size`) %>%
        ungroup() %>%
        select(
          Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
          `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress) -> data
    
      if (grprace == TRUE) {
        data <- data %>%
          mutate(Category1_grp = Category1,
                 Group_grp = Group,
                 Category1 = "",
                 Group = "")
      }
    return(data)
    }
  }



#### Look over time with rolling averages ####
# Calculates rolling averages for KC overall
indicator_rollyr_kc <- function(data, indicator, yrstart = minyr_b, yrend = maxyr_b, yrcombine = 3) {

  # Need to add in checks about yrend >= yrstart and the maximum number of years that can be combined
  
  data2 <- data
  indicator2 <- indicator
  maxyr_b2 <- yrend
  
  indyr <- indicator_time_kc(data = data2, indicator = indicator2, yrend = maxyr_b2) %>%
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
      se = sqrt(Numerator / (`Sample Size`^2)),
      rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
      Suppress = ifelse(`Sample Size` < 50 |
                          Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
    ) %>%
    rowwise() %>%
    mutate(
      `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size`,
      `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size`) %>%
    ungroup() %>%
    select(
      Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
      `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress
    ) -> data
  return(data)
}


# Calculates rolling averages for each value within a group (e.g., race)
indicator_rollyr_group <- function(data, indicator, yrstart = minyr_b, yrend = maxyr_b, 
                                   yrcombine = 3, group = "all", grprace = FALSE) {
  
  # Need to add in checks about yrend >= yrstart and the maximum number of years that can be combined
  # Also need to add a check for non-consecutive years
  
  data2 <- data
  indicator2 <- indicator
  maxyr_b2 <- yrend
  group2 <- group
  grprace2 <- grprace
  
  # Get each single year's data ready for combining
  indyr <- indicator_time_group(data = data2, indicator = indicator2, yrend = maxyr_b2, group = group2, grprace = grprace2) %>%
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
      se = sqrt(Numerator / (`Sample Size`^2)),
      rse = ifelse(Numerator > 0, 1/sqrt(Numerator), NA),
      Suppress = ifelse(`Sample Size` < 50 |
                          Numerator < 5, "Y", ifelse(rse >= 30, "N*", "N"))
    ) %>%
    rowwise() %>%
    mutate(
      `Lower Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[1] / `Sample Size`,
      `Upper Bound` = poisson.test(Numerator, conf.level = 0.95)$conf.int[2] / `Sample Size`) %>%
    ungroup() %>%
    select(
      Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Percent, 
      `Lower Bound`, `Upper Bound`, se, rse, `Sample Size`, Numerator, Suppress
    ) -> data
  
  return(data)
}



#### H1: Preterm birth ####
preterm <- rbindlist(list(
  # KC overall
  indicator_kc(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5),
  # By each subgroup
  indicator_subgroup(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "all", grprace = FALSE, smlgrp = FALSE),
  indicator_subgroup(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "all", grprace = TRUE, smlgrp = FALSE),
  # Crosstabs for each subgroup (including overall for when group = subgroup)
  indicator_subgroup(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "all", grprace = FALSE, smlgrp = TRUE),
  indicator_subgroup(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "all", grprace = TRUE, smlgrp = TRUE),
  indicator_smallgroup(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "all", grprace = FALSE),
  indicator_smallgroup(data = births, indicator = "preterm", year = maxyr_b, yrcombine = 5, group = "all", grprace = TRUE),
  # Time trend for KC overall
  indicator_rollyr_kc(data = births, indicator = "preterm", yrstart = minyr_b, yrend = maxyr_b, yrcombine = 3),
  # Time trend for each subgroup
  indicator_rollyr_group(data = births, indicator = "preterm", yrstart = minyr_b, yrend = maxyr_b, yrcombine = 3, group = "rgn_id", grprace = FALSE),
  indicator_rollyr_group(data = births, indicator = "preterm", yrstart = minyr_b, yrend = maxyr_b, yrcombine = 3, group = "race_mom", grprace = FALSE),
  indicator_rollyr_group(data = births, indicator = "preterm", yrstart = minyr_b, yrend = maxyr_b, yrcombine = 3, group = "rgn_id", grprace = TRUE),
  indicator_rollyr_group(data = births, indicator = "preterm", yrstart = minyr_b, yrend = maxyr_b, yrcombine = 3, group = "race_mom_grp", grprace = TRUE),
    # Time trend for KC overall (JoinPoint data)
  indicator_time_kc(data = births, indicator = "preterm", yrstart = minyr_b, yrend = maxyr_b),
  # Time trend for each subgroup (JoinPoint data)
  indicator_time_group(data = births, indicator = "preterm", yrstart = minyr_b, yrend = maxyr_b, group = "rgn_id", grprace = FALSE),
  indicator_time_group(data = births, indicator = "preterm", yrstart = minyr_b, yrend = maxyr_b, group = "race_mom", grprace = FALSE),
  indicator_time_group(data = births, indicator = "preterm", yrstart = minyr_b, yrend = maxyr_b, group = "rgn_id", grprace = TRUE),
  indicator_time_group(data = births, indicator = "preterm", yrstart = minyr_b, yrend = maxyr_b, group = "race_mom_grp", grprace = TRUE)
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
                              Percent, `Lower Bound`, `Upper Bound`, se, rse, 
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
  write.xlsx(., file = paste0(filepath, "BSK dashboard - preterm.xlsx"), sheetName = "preterm")


### Apply suppression rules and export
preterm %>% mutate_at(vars(Percent:rse, Numerator, `Comparison with KC`),
                      funs(ifelse(Suppress == "Y", NA, .))) %>%
  mutate_at(vars(Proportion:rse), funs(round(., digits = 1))) %>%
  filter(Tab != "Trends_JP") %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - preterm - suppressed.xlsx"), sheetName = "preterm")


### Create data sets for Joinpoint
preterm %>%
  filter(Tab == "Trends_JP" & Category1 != "") %>%
  select(Tab, Category1, Group, Year, Numerator, Percent, se) %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - preterm - JP.xlsx"), sheetName = "preterm")

preterm %>%
  filter(Tab == "Trends_JP" & Category1_grp != "") %>%
  select(Tab, Category1_grp, Group_grp, Numerator, Year, Percent, se) %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - preterm grouped race - JP.xlsx"), sheetName = "preterm")




#### S1: Breastfeeding initiation ####
breastfed <- rbindlist(list(
  # KC overall
  indicator_kc(data = births, indicator = "breastfed", year = maxyr_b),
  # By each subgroup
  indicator_subgroup(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "all", grprace = FALSE, smlgrp = FALSE),
  indicator_subgroup(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "all", grprace = TRUE, smlgrp = FALSE),
  # Crosstabs for each subgroup (including overall for when group = subgroup)
  indicator_subgroup(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "all", grprace = FALSE, smlgrp = TRUE),
  indicator_subgroup(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "all", grprace = TRUE, smlgrp = TRUE),
  indicator_smallgroup(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "all", grprace = FALSE),
  indicator_smallgroup(data = births, indicator = "breastfed", year = maxyr_b, yrcombine = 5, group = "all", grprace = TRUE),
  # Time trend for KC overall
  indicator_rollyr_kc(data = births, indicator = "breastfed", yrend = maxyr_b, yrcombine = 3),
  # Time trend for each subgroup
  indicator_rollyr_group(data = births, indicator = "breastfed", yrend = maxyr_b, yrcombine = 3, group = "rgn_id", grprace = FALSE),
  indicator_rollyr_group(data = births, indicator = "breastfed", yrend = maxyr_b, yrcombine = 3, group = "race_mom", grprace = FALSE),
  indicator_rollyr_group(data = births, indicator = "breastfed", yrend = maxyr_b, yrcombine = 3, group = "rgn_id", grprace = TRUE),
  indicator_rollyr_group(data = births, indicator = "breastfed", yrend = maxyr_b, yrcombine = 3, group = "race_mom_grp", grprace = TRUE),
  # Time trend for KC overall (JoinPoint data)
  indicator_time_kc(data = births, indicator = "breastfed", yrend = maxyr_b),
  # Time trend for each subgroup (JoinPoint data)
  indicator_time_group(data = births, indicator = "breastfed", yrend = maxyr_b, group = "rgn_id", grprace = FALSE),
  indicator_time_group(data = births, indicator = "breastfed", yrend = maxyr_b, group = "race_mom", grprace = FALSE),
  indicator_time_group(data = births, indicator = "breastfed", yrend = maxyr_b, group = "rgn_id", grprace = TRUE),
  indicator_time_group(data = births, indicator = "breastfed", yrend = maxyr_b, group = "race_mom_grp", grprace = TRUE)
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
                              Percent, `Lower Bound`, `Upper Bound`, se, rse, 
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
  write.xlsx(., file = paste0(filepath, "BSK dashboard - breastfeeding initiation.xlsx"))


### Apply suppression rules and export
breastfed %>% mutate_at(vars(Percent:rse, Numerator, `Comparison with KC`),
                      funs(ifelse(Suppress == "Y", NA, .))) %>%
  mutate_at(vars(Proportion:rse), funs(round(., digits = 1))) %>%
  filter(Tab != "Trends_JP") %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - breastfeeding initiation - suppressed.xlsx"))


### Create data sets for Joinpoint
breastfed %>%
  filter(Tab == "Trends_JP" & Category1 != "") %>%
  select(Tab, Category1, Group, Year, Percent, se) %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - breastfeeding initiation - JP.xlsx"))

breastfed %>%
  filter(Tab == "Trends_JP" & Category1_grp != "") %>%
  select(Tab, Category1_grp, Group_grp, Year, Percent, se) %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - breastfeeding initiation grouped race - JP.xlsx"))



#### S2: Early and adequate prenatal care ####
prenatal <- rbindlist(list(
  # KC overall
  indicator_kc(data = births, indicator = "prenatal", year = maxyr_b),
  # By each subgroup
  indicator_subgroup(data = births, indicator = "prenatal", year = maxyr_b, yrcombine = 5, group = "all", grprace = FALSE, smlgrp = FALSE),
  indicator_subgroup(data = births, indicator = "prenatal", year = maxyr_b, yrcombine = 5, group = "all", grprace = TRUE, smlgrp = FALSE),
  # Crosstabs for each subgroup (including overall for when group = subgroup)
  indicator_subgroup(data = births, indicator = "prenatal", year = maxyr_b, yrcombine = 5, group = "all", grprace = FALSE, smlgrp = TRUE),
  indicator_subgroup(data = births, indicator = "prenatal", year = maxyr_b, yrcombine = 5, group = "all", grprace = TRUE, smlgrp = TRUE),
  indicator_smallgroup(data = births, indicator = "prenatal", year = maxyr_b, yrcombine = 5, group = "all", grprace = FALSE),
  indicator_smallgroup(data = births, indicator = "prenatal", year = maxyr_b, yrcombine = 5, group = "all", grprace = TRUE),
  # Time trend for KC overall
  indicator_rollyr_kc(data = births, indicator = "prenatal", yrend = maxyr_b, yrcombine = 3),
  # Time trend for each subgroup
  indicator_rollyr_group(data = births, indicator = "prenatal", yrend = maxyr_b, yrcombine = 3, group = "rgn_id", grprace = FALSE),
  indicator_rollyr_group(data = births, indicator = "prenatal", yrend = maxyr_b, yrcombine = 3, group = "race_mom", grprace = FALSE),
  indicator_rollyr_group(data = births, indicator = "prenatal", yrend = maxyr_b, yrcombine = 3, group = "rgn_id", grprace = TRUE),
  indicator_rollyr_group(data = births, indicator = "prenatal", yrend = maxyr_b, yrcombine = 3, group = "race_mom_grp", grprace = TRUE),
  # Time trend for KC overall (JoinPoint data)
  indicator_time_kc(data = births, indicator = "prenatal", yrend = maxyr_b),
  # Time trend for each subgroup (JoinPoint data)
  indicator_time_group(data = births, indicator = "prenatal", yrend = maxyr_b, group = "rgn_id", grprace = FALSE),
  indicator_time_group(data = births, indicator = "prenatal", yrend = maxyr_b, group = "race_mom", grprace = FALSE),
  indicator_time_group(data = births, indicator = "prenatal", yrend = maxyr_b, group = "rgn_id", grprace = TRUE),
  indicator_time_group(data = births, indicator = "prenatal", yrend = maxyr_b, group = "race_mom_grp", grprace = TRUE)
))


### Convert to a df
prenatal <- as.data.frame(prenatal)

### Apply labels
prenatal <- left_join(prenatal, labels, by = c("Category1", "Group")) %>%
  rename(Group.id = Group, Group = Label)
prenatal <- left_join(prenatal, labels, by = c("Category1_grp" = "Category1", "Group_grp" = "Group")) %>%
  rename(Group_grp.id = Group_grp, Group_grp = Label)
prenatal <- left_join(prenatal, labels, by = c("Category2" = "Category1", "Subgroup" = "Group")) %>%
  rename(Subgroup.id = Subgroup, Subgroup = Label)

# Reorder and sort columns
prenatal <- prenatal %>% select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, 
                                  Percent, `Lower Bound`, `Upper Bound`, se, rse, 
                                  `Sample Size`, Numerator, Suppress, Group.id, Subgroup.id) %>%
  arrange(Tab, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, Year)

### Clean up nulls and blanks
prenatal <- prenatal %>%
  filter(!((Category1 != "" & is.na(Group)) | 
             (Category1_grp != "" & is.na(Group_grp)) |
             (Category2 != "" & is.na(Subgroup))))


### Add in comparisons with KC
# Pull out KC values
kclb <- as.numeric(prenatal %>%
                     filter(Tab == "_King County") %>%
                     select(`Lower Bound`))
kcub <- as.numeric(prenatal %>%
                     filter(Tab == "_King County") %>%
                     select(`Upper Bound`))
kctrend <- prenatal %>%
  filter(Tab == "Trends" & Category1 == "King County") %>%
  select(Year, `Lower Bound`, `Upper Bound`)

# Do comparison for most recent year (i.e., not trends)
comparison <- prenatal %>%
  filter(Tab != "Trends") %>%
  mutate(`Comparison with KC` = ifelse(`Upper Bound` < kclb, "lower",
                                       ifelse(`Lower Bound` > kcub, "higher", "no different"))) %>%
  select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, `Comparison with KC`)

# Do comparison for trends (each year compared to KC that year, not that group's trend over time)
comparison_trend <- prenatal %>%
  filter(Tab == "Trends") %>%
  left_join(., kctrend, by = "Year") %>%
  mutate(`Comparison with KC` = ifelse(`Upper Bound.x` < `Lower Bound.y`, "lower",
                                       ifelse(`Lower Bound.x` > `Upper Bound.y`, "higher", "no different"))) %>%
  select(Tab, Year, Category1, Group, Category1_grp, Group_grp, Category2, Subgroup, `Comparison with KC`)

# Bring both comparisons together
comparison <- rbind(comparison, comparison_trend)

# Merge with main data and add column for time trends
prenatal <- left_join(prenatal, comparison, by = c("Tab", "Year", "Category1", "Group", "Category1_grp", 
                                                     "Group_grp", "Category2", "Subgroup")) %>%
  mutate(`Time Trends` = "")
rm(comparison, comparison_trend, kclb, kcub, kctrend)


### Export to an Excel file
prenatal %>% filter(Tab != "Trends_JP") %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - prenatal care.xlsx"))


### Apply suppression rules and export
prenatal %>% mutate_at(vars(Percent:rse, Numerator, `Comparison with KC`),
                        funs(ifelse(Suppress == "Y", NA, .))) %>%
  mutate_at(vars(Proportion:rse), funs(round(., digits = 1))) %>%
  filter(Tab != "Trends_JP") %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - prenatal care - suppressed.xlsx"))


### Create data sets for Joinpoint
prenatal %>%
  filter(Tab == "Trends_JP" & Category1 != "") %>%
  select(Tab, Category1, Group, Year, Percent, se) %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - prenatal care - JP.xlsx"))

prenatal %>%
  filter(Tab == "Trends_JP" & Category1_grp != "") %>%
  select(Tab, Category1_grp, Group_grp, Year, Percent, se) %>%
  write.xlsx(., file = paste0(filepath, "BSK dashboard - prenatal care grouped race - JP.xlsx"))



#### S3: Adolescent birth rate (15-17) ####

###### NB. Need to bring in pop data for denominator (adapt IM code to do this)


