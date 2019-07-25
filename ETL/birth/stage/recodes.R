## Header ####
    # Author: Danny Colombara
    # 
    # R version: 3.5.3
    #
    # Purpose: Recode data that was appended and standardized
    # 
    # Notes: Uses a function for staightforward recodes and binning. 
    #        Complex (custom) recoding is performed below, after usign the functions
    #        

## Update recoding functions created by Daniel Casey ----
    # devtools::install_local("C:/Users/dcolombara/code/apdeRecodes/", force=T)

## Set up environment ----
    pacman::p_load(apdeRecodes, odbc, data.table, RCurl, dplyr)
    rm(list=ls())
    source("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/danny/ETL/birth/stage/enact_recoding_function.R")

## Load reference data ----    
    recodes <- data.table::fread("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/danny/ETL/birth/ref/ref.bir_recodes_simple.csv")

    table_config_stage_bir_wa <- yaml::yaml.load(getURL("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/danny/ETL/birth/stage/create_stage.bir_wa.yaml"))

    table_config_final_bir_wa <- yaml::yaml.load(getURL("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/danny/ETL/birth/final/create_final.bir_wa.yaml"))

## Identify vars to be pulled in from SQL ----
    complete.varlist <- names(table_config_stage_bir_wa$vars) # all column names from YAML that made SQL table
    old.varlist <- setdiff(unique(recodes$old_var), "") # all column names that will be recoded with the code below
    
    query.varlist <- setdiff(old.varlist, setdiff(old.varlist, complete.varlist)) # column names for the SQL data that will be recoded with simple recode function
    
    query.varlist <- paste(
      c(query.varlist, # manually add vars needed for complex recodes 
        "non_vertex_presentation", "mother_bmi", 
        "prior_live_births_living", "prior_live_births_deceased", 
        "number_prenatal_visits"
      )
      , collapse=", ")
    
    query.string <- paste("SELECT", query.varlist, "FROM stage.bir_wa")

## Pull minimal staged data from SQL ----
  db_apde <- dbConnect(odbc(), "APDESQL50") ##Connect to SQL server

  bir_recodes.dt <- setDT(DBI::dbGetQuery(db_apde, query.string))

## Convert select character cols to numeric ----
  bir_recodes.dt[, mother_residence_county_wa_code := as.numeric(mother_residence_county_wa_code)]
  bir_recodes.dt[, birthplace_county_wa_code := as.numeric(birthplace_county_wa_code)]
  bir_recodes.dt[, mother_residence_zip := as.numeric(mother_residence_zip)]
  
## Prep simple recode instructions ----
    complex.vars <- recodes[recode_type=="complex"]$new_var # save list of vars made with complex recodes
    recodes <- recodes[recode_type != "complex"] # drop complex recodes from list of simple recodes
    recodes <- recodes[, .(old_var, new_var, old_value, new_value, new_label, start_year, end_year, var_label)] # keep columns needed for function

## Process simple recodes with package ----
    bir_recodes.dt <- enact_recoding(data = bir_recodes.dt, 
                   year = 2006, 
                   parse_recode_instructions(recodes, catch_NAs = T), 
                   ignore_case = T, 
                   hypothetical = F)  
    
## Custom code for complex recoding ----
    # Not in alphabetical order because some vars are dependant upon other vars
    # cigarettes_smoked_3_months_prior ----
      bir_recodes.dt[is.na(cigarettes_smoked_3_months_prior) & year >=2017, cigarettes_smoked_3_months_prior := 0]
      bir_recodes.dt[cigarettes_smoked_3_months_prior == 0, smokeprior := 0]
      
    # cigarettes_smoked_1st_tri ----
      bir_recodes.dt[is.na(cigarettes_smoked_1st_tri) & year >=2017, cigarettes_smoked_1st_tri := 0]
      bir_recodes.dt[cigarettes_smoked_1st_tri == 0, smoke1 := 0]
      
    # cigarettes_smoked_2nd_tri ----
      bir_recodes.dt[is.na(cigarettes_smoked_2nd_tri) & year >=2017, cigarettes_smoked_2nd_tri := 0]
      bir_recodes.dt[cigarettes_smoked_2nd_tri == 0, smoke2 := 0]
      
    # cigarettes_smoked_3rd_tri ----
      bir_recodes.dt[is.na(cigarettes_smoked_3rd_tri) & year >=2017, cigarettes_smoked_3rd_tri := 0]      
      bir_recodes.dt[cigarettes_smoked_3rd_tri == 0, smoke3 := 0]       

    # diab_no (Diabetes-No) ----
      bir_recodes.dt[diab_gest == 0 & diab_prepreg == 0, diab_no := 1] 
      bir_recodes.dt[diab_gest == 1 | diab_prepreg == 1, diab_no := 0]

    # htn_no (Hypertension-No) ----
      bir_recodes.dt[htn_gest == 0 & htn_prepreg == 0, htn_no := 1] 
      bir_recodes.dt[htn_gest == 1 | htn_prepreg == 1, htn_no := 0]

    # nullip (nulliparous) ----
      bir_recodes.dt[prior_live_births_living == 0 & prior_live_births_deceased == 0, nullip := 1]
      bir_recodes.dt[prior_live_births_living %in% c(1:98) | prior_live_births_deceased %in% c(1:98), nullip := 0]
      
    # vertex (vertex birth position) ----
      bir_recodes.dt[fetal_pres == 1, vertex := 0]
      bir_recodes.dt[fetal_pres %in% 2:3, vertex := 1]
      bir_recodes.dt[non_vertex_presentation=="N", vertex := 1]
      bir_recodes.dt[, vertex := factor(vertex, levels = c(0, 1), labels = c("Breech/Oth", "Vertex"))]
      
    # ntsv (nulliparous, term, singleton, vertex birth) ----
      bir_recodes.dt[, ntsv := 0] 
      bir_recodes.dt[(nullip == 1 & term == 1 & plurality == 1 & vertex == "Vertex"), ntsv := 1]

    # csec_lowrisk (Cesarean section among low risk deliveries) ----
      bir_recodes.dt[delivery_final %in% c(4:6), csec_lowrisk := 0] # any c-section == 0 
      bir_recodes.dt[delivery_final %in% c(4:6) & ntsv==1, csec_lowrisk:=1]    
      
    # pnc_lateno (Late or no prenatal care) ----
      bir_recodes.dt[month_prenatal_care_began %in% c(1:6), pnc_lateno := 0]
      bir_recodes.dt[month_prenatal_care_began %in% c(0, 7:10), pnc_lateno := 1]
    
    # smoking (Smoking-Yes (before &|or during pregnancy)) ----
      bir_recodes.dt[, smoking := NA_integer_]
      bir_recodes.dt[(smokeprior==0 & smoke1==0 & smoke2==0 & smoke3==0), smoking := 0]
      bir_recodes.dt[(smokeprior==1 & smoke1==1 & smoke2==1 & smoke3==1), smoking := 1]

    # smoking_dur (whether mother smoked at all during pregnancy) ----
      bir_recodes.dt[, smoking_dur := NA_integer_]
      bir_recodes.dt[(smoke1==0 & smoke2==0 & smoke3==0), smoking_dur := 0]
      bir_recodes.dt[(smoke1 == 1 | smoke2 == 1 | smoke3 == 1), smoking_dur := 1]

    # wtgain (CHAT categories of maternal weight gain) ----
      bir_recodes.dt[
               ((mother_bmi<18.5 & mother_weight_gain<28) | 
               (mother_bmi>=18.5 & mother_bmi<=24.99 & mother_weight_gain<25) | 
               (mother_bmi>=25.0 & mother_bmi<=29.99 & mother_weight_gain<15) | 
               (mother_bmi>=30.0 & mother_bmi<99.9 & mother_weight_gain<11)), 
             wtgain := 1]
      
      bir_recodes.dt[is.na(wtgain) & 
               ((mother_bmi<18.5 & mother_weight_gain>=28 & mother_weight_gain<=40) | 
               (mother_bmi>=18.5 & mother_bmi<=24.9 & mother_weight_gain>=25 & mother_weight_gain<=35) | 
               (mother_bmi>=25.0 & mother_bmi<=29.9 & mother_weight_gain>=15 & mother_weight_gain<=25) | 
               (mother_bmi>=30.0 & mother_bmi<99.9 & mother_weight_gain>=11 & mother_weight_gain<=20)), 
             wtgain := 2]
      
      bir_recodes.dt[is.na(wtgain) & 
               ((mother_bmi<18.5 & mother_weight_gain>40) | 
                  (mother_bmi>=18.5 & mother_bmi<=24.9 & mother_weight_gain>35) | 
                  (mother_bmi>=25.0 & mother_bmi<=29.9 & mother_weight_gain>25) | 
                  (mother_bmi>=30.0 & mother_bmi<99.9 & mother_weight_gain>20 & mother_weight_gain<999)), 
             wtgain := 3]       
      
      bir_recodes.dt[, wtgain := factor(wtgain, levels = c(1:3), labels = c("below recommended", "recommended", "above recommended"))]
    
    # wtgain_rec (WT Gain-Recommended) ----
      bir_recodes.dt[wtgain %in% c("below recommended", "above recommended"),  wtgain_rec := 0]
      bir_recodes.dt[wtgain=="recommended", wtgain_rec := 1]

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
          bir_recodes.dt$expected.pnc <- expected.pnc[cbind(
            match(bir_recodes.dt$calculated_gestation, rownames(expected.pnc)), 
            match(bir_recodes.dt$month_prenatal_care_began, colnames(expected.pnc))
            )]
      
      # Step 3: Compare actual to the expected to calculate Adequacy of PNC based on kotelchuck          
          # Determin adequacy of prenatal care
            bir_recodes.dt[month_prenatal_care_began %in% c(0, 5:14), apncu := 0]  # inadequate
            bir_recodes.dt[number_prenatal_visits < 0.8 * expected.pnc, apncu := 0] # less than 80% of expected is inadequate
            bir_recodes.dt[number_prenatal_visits >= 0.8 * expected.pnc, apncu := 1] # greater than or equal to 80% is adequate
          
          # Set to NA when underlying variables are missing or illogical
            bir_recodes.dt[is.na(month_prenatal_care_began) | is.na(calculated_gestation) | calculated_gestation == 0, apncu := NA]
            bir_recodes.dt[is.na(number_prenatal_visits) | number_prenatal_visits == 99, apncu := NA]
            bir_recodes.dt[(month_prenatal_care_began > (calculated_gestation/4)), apncu := NA]
            bir_recodes.dt[(month_prenatal_care_began == 0 & number_prenatal_visits >=1) | (number_prenatal_visits == 0 & month_prenatal_care_began >= 1), apncu := NA]
            bir_recodes.dt[is.na(expected.pnc), apncu := NA]

          bir_recodes.dt <- bir_recodes.dt %>% mutate(
            kotelchuck = case_when(
              # Set to missing when underlying variables are missing or illogical
                is.na(month_prenatal_care_began) | is.na(calculated_gestation) | calculated_gestation == 0 ~ NA_real_,
                is.na(number_prenatal_visits) | number_prenatal_visits == 99 ~ NA_real_,
                (month_prenatal_care_began > (calculated_gestation/4)) ~ NA_real_,
                (month_prenatal_care_began==0 & number_prenatal_visits >=1) | (number_prenatal_visits==0 & month_prenatal_care_began >= 1) ~ NA_real_,
              # Met/did not meet the threshold
                month_prenatal_care_began > 4 & month_prenatal_care_began < 15 ~ 0, # after 4th month automatically inadequate
                number_prenatal_visits < 0.8 * expected.pnc ~ 0, # less than 80% of expected is inadequate
                number_prenatal_visits >= 0.8 * expected.pnc ~ 1, # greater than or equal to 80% is adequate
                number_prenatal_visits == 0 ~ 0,
              # If the expected is missing, means it is off the charts (literally) and cannot be calculated
                is.na(expected.pnc) ~ NA_real_,
              TRUE ~ NA_real_
            ))

      setDT(bir_recodes.dt)
          
          View(bir_recodes.dt[(kotelchuck != apncu) | (is.na(kotelchuck) + is.na(apncu)==1), 
                              .(kotelchuck, apncu, month_prenatal_care_began, calculated_gestation, number_prenatal_visits, expected.pnc)])    
          
          
    # Check kotelchuck output vis a vis CHAT ----
          # CHAT 295/505
          bir_recodes.dt[mother_calculated_age==25 & date_of_birth_year == 2015 & mother_residence_county_wa_code==17, table(kotelchuck)] 
          bir_recodes.dt[mother_calculated_age == 25 & date_of_birth_year == 2015 & mother_residence_county_wa_code==17, table(apncu)]

          # CHAT 269/691
          bir_recodes.dt[mother_calculated_age==30 & date_of_birth_year == 2005 & mother_residence_county_wa_code==17, table(kotelchuck)] 
          bir_recodes.dt[mother_calculated_age == 30 & date_of_birth_year == 2005 & mother_residence_county_wa_code==17, table(apncu)]


## Consolidate staged & recoded data ----
    # Pull all staged data that is not recoded ----
        new.query.varlist <- paste(
          setdiff(complete.varlist, 
                  unlist(strsplit(query.varlist, ", "))), 
          collapse=", ")
          
        new.query.string <- paste("SELECT", query.varlist, "FROM stage.bir_wa")
        
        staged.dt <- setDT(DBI::dbGetQuery(db_apde, query.string))
        
    # Combine two datasets into one wider dataset ----
        staged.dt <- cbind(staged.dt, bir_recodes.dt)
        rm(bir_recodes.dt)
          
    # Order columns of final dataset to match SQL ----
        column.order <- names(table_config_final_bir_wa$vars)
        setcolorder(staged.dt, column.order)
        
#### LOAD TO SQL: Final complete dataset ####
    tbl_id_2003_20xx <- DBI::Id(schema = table_config_final_bir_wa$schema, 
                                table = table_config_final_bir_wa$table)
    
    dbWriteTable(db_apde, 
                 tbl_id_2003_20xx, 
                 value = as.data.frame(staged.dt),
                 overwrite = T, append = F, 
                 field.types = unlist(table_config_final_bir_wa$vars))
    

## The end! ----

          