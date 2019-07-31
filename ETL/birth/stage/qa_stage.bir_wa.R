## Header ####
    # Author: Danny Colombara
    # 
    # R version: 3.5.3
    #
    # Purpose: QA staged birth data to ensure not corrupted when pushed to SQL and recoding was executed properly
    # 
    # Notes: did not check all variables vis-à-vis CHAT, but rather a random assorment

## Set up environment ----
    rm(list=ls())
    pacman::p_load(data.table, odbc, lubridate, RCurl, glue)
    options("scipen"=10) # turn off scientific notation  
    options(warning.length = 8170) # get lengthy warnings, needed for SQL
    setwd("C:/temp")

## Load staged from SQL ----
    table_config_stage_bir_wa <- yaml::yaml.load(getURL(
      "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/stage/create_stage.bir_wa.yaml"))
    
    query.varlist <- paste(c(names(table_config_stage_bir_wa$vars), names(table_config_stage_bir_wa$recodes)), collapse = ", ")
    
    query.string <- glue_sql ("SELECT ", query.varlist, " FROM stage.bir_wa")
    
    db_apde <- dbConnect(odbc(), "PH_APDEStore50") ##Connect to SQL server
    
    staged.dt <- setDT(DBI::dbGetQuery(db_apde, query.string))
    
## Identify columns that are 100% missing -----
    all.miss <- lapply(staged.dt, function(x)all(is.na(x)))
    all.miss <- data.table(name = names(all.miss), all.missing = all.miss)
    print(all.miss[all.missing == TRUE])
    
## Identify numeric columns ----
    numerics <- data.table(name = names(table_config_stage_bir_wa$vars), type = as.character(table_config_stage_bir_wa$vars))
    numerics <- numerics[type %in% c("DATE", "INTEGER", "NUMERIC")]
    
## Check date range -----
    date.vars <- numerics[type=="DATE"]$name
    staged.dt[, (date.vars) :=lapply(.SD, ymd), .SDcols = date.vars]
    
    rbind(staged.dt[, lapply(.SD, min, na.rm = TRUE), .SDcols = date.vars], 
          staged.dt[, lapply(.SD, max, na.rm = TRUE), .SDcols = date.vars])
    
## COMPARE TO CHAT ----    
    # Births compared to CHAT ----
        births.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_total_births.csv", 
                            skip = 5, nrows = 15)[, .(year = V1, count.doh = V7)]
        births <- setorder(staged.dt[wa_res==1, .N, by = year], year)
        births <- merge(births, births.doh, by = "year"); rm(births.doh)
        births[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        # print(births)
        
    # Race compared to CHAT ----
        staged.dt[, race3 := factor(race3, levels = 1:6, labels=c("AIAN alone", "Asian alone", "Black alone", "Multiple race", "NHPI alone", "White alone"))]
        race <- setorder(as.data.table(staged.dt[wa_res==1, table(year, race3)], row.names = TRUE), year, race3)    
        
        race.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_race.csv", 
                          skip = 5, nrows = 84)[, .(year = as.character(V1), race3 = V4, count.doh = V7)]
        race.doh[race3 == "American Indian/Alaskan Native Only", race3 := "AIAN alone"]
        race.doh[race3 == "Asian Only", race3 := "Asian alone"]
        race.doh[race3 == "Black Only", race3 := "Black alone"]
        race.doh[race3 == "Multi Race", race3 := "Multiple race"]
        race.doh[race3 == "Pacific Islander Only", race3 := "NHPI alone"]
        race.doh[race3 == "White Only", race3 := "White alone"]
                            
        race <- merge(race, race.doh, by = c("year", "race3"), all.x = TRUE); rm(race.doh)
        race[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        # print(race)
        
    # Hispanic compared to CHAT ----
        hispanic <- setorder(as.data.table(staged.dt[wa_res==1, table(year, hispanic)], row.names = TRUE), year)[hispanic==1, -c("hispanic")]
        
        hispanic.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_hispanic.csv", 
                              skip = 5, nrows = 15)[, .(year = as.character(V1), count.doh = V7)]
        
        hispanic <- merge(hispanic, hispanic.doh, by = "year"); rm(hispanic.doh)
        hispanic[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        # print(hispanic)
        
    # Birthweight compared to CHAT ----
        bw <- rbind(
          setorder(as.data.table(staged.dt[wa_res==1, table(year, bw_vlow)], row.names = TRUE), year)[bw_vlow==1,][, bw_vlow := NULL][, bw := "bw_vlow"],
          setorder(as.data.table(staged.dt[wa_res==1, table(year, bw_modlow)], row.names = TRUE), year)[bw_modlow==1,][, bw_modlow := NULL][, bw := "bw_modlow"],
          setorder(as.data.table(staged.dt[wa_res==1, table(year, bw_norm)], row.names = TRUE), year)[bw_norm==1,][, bw_norm := NULL][, bw := "bw_norm"],
          setorder(as.data.table(staged.dt[wa_res==1, table(year, bw_high)], row.names = TRUE), year)[bw_high==1,][, bw_high := NULL][, bw := "bw_high"],
          setorder(as.data.table(staged.dt[wa_res==1, table(year, bw_vhigh)], row.names = TRUE), year)[bw_vhigh==1,][, bw_vhigh := NULL][, bw := "bw_vhigh"]
        )
        
        bw.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_birthweight.csv", 
                        skip = 5, nrows = 75)[, .(year = as.character(V1), bw = V6, count.doh = V7)]
        bw.doh[bw == "Birth WT-Very Low (227-1499 grams)", bw := "bw_vlow"]
        bw.doh[bw == "Birth WT-Moderately Low (1500-2499 grams)", bw := "bw_modlow"]
        bw.doh[bw == "Birth WT-Normal (2500-3999 grams)", bw := "bw_norm"]
        bw.doh[bw == "Birth WT-High (4000-4499 grams)", bw := "bw_high"]
        bw.doh[bw == "Birth WT-Very High (4500-8164 grams)", bw := "bw_vhigh"]
        
        bw <- merge(bw, bw.doh, by = c("year", "bw"), all.x = TRUE); rm(bw.doh)
        bw[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        # print(bw)
        
    # Premature compared to CHAT ----
        preterm <- setorder(as.data.table(staged.dt[wa_res==1, table(year, preterm)], row.names = TRUE), year)[preterm==1, -c("preterm")]
        
        preterm.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_premature.csv", 
                              skip = 5, nrows = 15)[, .(year = as.character(V1), count.doh = V7)]
        
        preterm <- merge(preterm, preterm.doh, by = "year"); rm(preterm.doh)
        preterm[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        # print(preterm)
    
    # Mom born in USA compared to CHAT ----
        mother_birthplace_usa <- setorder(as.data.table(staged.dt[wa_res==1, table(year, mother_birthplace_usa)], row.names = TRUE), year)[mother_birthplace_usa==1, -c("mother_birthplace_usa")]
        
        mother_birthplace_usa.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_usa.csv", 
                             skip = 5, nrows = 15)[, .(year = as.character(V1), count.doh = V7)]
        
        mother_birthplace_usa <- merge(mother_birthplace_usa, mother_birthplace_usa.doh, by = "year"); rm(mother_birthplace_usa.doh)
        mother_birthplace_usa[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        # print(mother_birthplace_usa)        
        
    # Prior pregnancy compared to CHAT ----
        ch_priorpreg <- setorder(as.data.table(staged.dt[wa_res==1, table(year, ch_priorpreg)], row.names = TRUE), year)[ch_priorpreg==1, -c("ch_priorpreg")]
        
        ch_priorpreg.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_priorpreg.csv", 
                                           skip = 5, nrows = 15)[, .(year = as.character(V1), count.doh = V7)]
        
        ch_priorpreg <- merge(ch_priorpreg, ch_priorpreg.doh, by = "year"); rm(ch_priorpreg.doh)
        ch_priorpreg[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        # print(ch_priorpreg)   
        
    # NCHS Prenatal Care compared to CHAT ----
        pnc <- setorder(as.data.table(staged.dt[wa_res==1, table(year, pnc_tri_nchs)], row.names = TRUE), year, pnc_tri_nchs)    
        
        pnc.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_pnc_nchs.csv", 
                         skip = 5, nrows = 45)[, .(year = as.character(V1), pnc.doh=V6, count.doh = V7)]
        pnc.doh[pnc.doh == "NCHS Prenatal Care Init.-1st Trimester", pnc.doh := "1"]
        pnc.doh[pnc.doh == "NCHS Prenatal Care Init.-2nd Trimester", pnc.doh := "2"]
        pnc.doh[pnc.doh == "NCHS Prenatal Care Init.-3rd Trimester", pnc.doh := "3"]
        pnc.doh[, pnc_tri_nchs := pnc.doh][, pnc.doh := NULL]
        
        pnc.nchs <- merge(pnc, pnc.doh, by = c("year", "pnc_tri_nchs"), all.x = TRUE); rm(pnc.doh)
        pnc.nchs[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        pnc.nchs <- pnc.nchs[pnc_tri_nchs != 0 & year >= 2011]
        # print(pnc.nchs)
        
    # Prenatal Care compared to CHAT ----
        pnc <- setorder(as.data.table(staged.dt[wa_res==1, table(year, pnc_tri)], row.names = TRUE), year, pnc_tri)    
        
        pnc.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_pnc.csv", 
                         skip = 5, nrows = 60)[, .(year = as.character(V1), pnc.doh=V6, count.doh = V7)]
        pnc.doh[pnc.doh == "Prenatal Care Init.-First Trimester", pnc.doh := "1"]
        pnc.doh[pnc.doh == "Prenatal Care Init.-Second Trimester", pnc.doh := "2"]
        pnc.doh[pnc.doh == "Prenatal Care Init.-Third Trimester", pnc.doh := "3"]
        pnc.doh[pnc.doh == "Prenatal Care Init.-No Prenatal Care", pnc.doh := "0"]
        pnc.doh[, pnc_tri := pnc.doh][, pnc.doh := NULL]
        
        pnc <- merge(pnc, pnc.doh, by = c("year", "pnc_tri"), all.x = TRUE); rm(pnc.doh)
        pnc[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        # print(pnc)
        
    # Hypertension - gestational compared to CHAT ----
        htn_gest <- setorder(as.data.table(staged.dt[wa_res==1, table(year, htn_gest)], row.names = TRUE), year)[htn_gest==1, -c("htn_gest")]
        
        htn_gest.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_htn_gest.csv", 
                                           skip = 5, nrows = 15)[, .(year = as.character(V1), count.doh = V7)]
        
        htn_gest <- merge(htn_gest, htn_gest.doh, by = "year"); rm(htn_gest.doh)
        htn_gest[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        # print(htn_gest)  
        
    # Hypertension - Prepregnancy compared to CHAT ----
        htn_prepreg <- setorder(as.data.table(staged.dt[wa_res==1, table(year, htn_prepreg)], row.names = TRUE), year)[htn_prepreg==1, -c("htn_prepreg")]
        
        htn_prepreg.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_htn_prepreg.csv", 
                              skip = 5, nrows = 15)[, .(year = as.character(V1), count.doh = V7)]
        
        htn_prepreg <- merge(htn_prepreg, htn_prepreg.doh, by = "year"); rm(htn_prepreg.doh)
        htn_prepreg[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        # print(htn_prepreg)      
        
    # Kotelchuck Index (adequate PNC) compared to CHAT ----
        kotelchuck <- setorder(as.data.table(staged.dt[wa_res==1, table(year, kotelchuck)], row.names = TRUE), year)[kotelchuck==1, -c("kotelchuck")]
        
        kotelchuck.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_kotelchuck_adequate.csv", 
                                 skip = 5, nrows = 15)[, .(year = as.character(V1), count.doh = V7)]
        
        kotelchuck <- merge(kotelchuck, kotelchuck.doh, by = "year"); rm(kotelchuck.doh)
        kotelchuck[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        # print(kotelchuck) 
        
    # Kotelchuck Index (inadequate PNC) compared to CHAT ----
        kotelchuck.inadequate <- setorder(as.data.table(staged.dt[wa_res==1, table(year, kotelchuck)], row.names = TRUE), year)[kotelchuck==0, -c("kotelchuck")]
        
        kotelchuck.inadequate.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_kotelchuck_inadequate.csv", 
                                skip = 5, nrows = 15)[, .(year = as.character(V1), count.doh = V7)]
        
        kotelchuck.inadequate <- merge(kotelchuck.inadequate, kotelchuck.inadequate.doh, by = "year"); rm(kotelchuck.inadequate.doh)
        kotelchuck.inadequate[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        # print(kotelchuck.inadequate)    
        
    # Births in KC compared to CHAT ----
        kc.births.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_total_births_kc.csv", 
                            skip = 5, nrows = 15)[, .(year = V1, count.doh = V7)]
        kc.births <- setorder(staged.dt[kc_res==1, .N, by = year], year)
        kc.births <- merge(kc.births, kc.births.doh, by = "year"); rm(kc.births.doh)
        kc.births[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        # print(kc.births)
        
## SAVE CHAT COMPARISONS & SIMPLE CROSS-TABS AS A TEXT FILE ----
        sink('bir_wa_2003-2017_qa.txt', append = FALSE)
          print("Summary of columns with 100% missing")  
            print(all.miss[all.missing == TRUE])
            print(" ")
          for(item in c("births", "race", "hispanic", "bw", "preterm", "mother_birthplace_usa", 
                        "ch_priorpreg", "pnc.nchs", "pnc", "htn_gest", "htn_prepreg", 
                        "kotelchuck", "kotelchuck.inadequate", "kc.births")){
            print(paste0(item, " compared with chat"))
            print(get(item))
            print(" ")
          }  
            
         #Check some cross-tabulations
            print("*****************************")
            print("**** Simple cross-tabs  *****")
            print("*****************************")
            staged.dt[, table(plurality, singleton, exclude = NULL)]
            print("*****************************")
            staged.dt[, table(plurality, multiple, exclude = NULL)]
            print("*****************************")
            staged.dt[, table(sex, gender, exclude = NULL)]
            print("*****************************")
            staged.dt[, table(breastfed, bf, exclude = NULL)]
            print("*****************************")
            staged.dt[, table(infant_death_flag, inf_death, exclude = NULL)]
            print("*****************************")
            staged.dt[, table(mother_residence_county_wa_code, kc_res, exclude = NULL)]
            print("*****************************")
            staged.dt[, table(mother_residence_state_nchs_code, wa_res, exclude = NULL)]
            print("*****************************")
            staged.dt[, table(source_of_payment, dlp_medicaid, exclude = NULL)]
            print("*****************************")
            staged.dt[, table(nchs_month_prenatal_care_began, pnc_tri_nchs, exclude = NULL)]
            print("*****************************")
            staged.dt[, table(month_prenatal_care_began, pnc_tri, exclude = NULL)]
            print("*****************************")
            staged.dt[, table(priorprg, ch_priorpreg, exclude = NULL)]
          sink()
          
## Compare row counts by year with raw data ----          
  rows.raw <- setDT(DBI::dbGetQuery(db_apde, "SELECT *  FROM metadata.qa_bir_values"))       
  rows.raw <- rows.raw[table_name %in% c("load_raw.bir_wa_2003_2016", "load_raw.bir_wa_2017_20xx")] # limit to tables of interest       
  rows.raw[, qa_date_max := max(qa_date), by = c("table_name", "date_max")]  # identify the most recent QA for each table/year combo
  rows.qa <- rows.raw[qa_date_max == qa_date][, year := year(date_min)][, raw := as.numeric(qa_value)] # keep as template for loading new QA to SQL
  rows.raw <- rows.qa[, .(year, raw)] # keep the most recent

  row.count <- setorder(as.data.table(staged.dt[, table(year)], row.names = TRUE), year)[, year := as.numeric(year)]
  
  row.count <- merge(row.count, rows.raw, by = "year"); rm(rows.raw)
  row.count[, diff := paste0(round(100* (N - raw) / raw, 2), "%")]
  
## Write QA row count results to SQL ----
  if(sum(row.count$N) == sum(row.count$raw)){ 
    ## WRITE QA RESULTS TO VALUES TABLE ----
        rows.total <- copy(rows.qa)[, date_min := "2003-01-01"][, date_max := "2017-12-31"][, raw := sum(row.count$N)][1,]
        rows.qa <- rbind(rows.qa, rows.total)
        rows.qa[, qa_value := as.character(raw)]
        rows.qa[, table_name := "stage.bir_wa"]
        rows.qa[, c("date_min", "date_max") := lapply(.SD, as.Date), .SDcols = c("date_min", "date_max")]
        rows.qa[, note := paste0("[", year(date_min), ", ", year(date_max), "]")]
        rows.qa <- rows.qa[, !c("qa_date_max", "year", "raw")]
        rows.qa[, qa_date := Sys.time()]
        setorder(rows.qa, date_max, -date_min)
    
        tbl_id_qa <- DBI::Id(schema = "metadata", 
                                    table = "qa_bir_values")
        dbWriteTable(db_apde, 
                     tbl_id_qa, 
                     value = as.data.frame(rows.qa),
                     overwrite = F, 
                     append = T )
        
    ## WRITE QA RESULTS TO SUMMARY TABLE ----
        qa_bir <- setDT(DBI::dbGetQuery(db_apde, "SELECT *  FROM metadata.qa_bir"))[1,] # pull in data as a template
        qa_bir[, etl_batch_id := NA]
        qa_bir[, last_run := Sys.Date()]
        qa_bir[, date_min := min(rows.qa$date_min)]
        qa_bir[, date_max := min(rows.qa$date_max)]
        qa_bir[, table_name := rows.qa[1, ]$table_name]
        qa_bir[, qa_item := "Number rows BY YEAR"]
        qa_bir[, qa_result := "PASS"]
        qa_bir[, qa_date := max(rows.qa$qa_date)]
        qa_bir[, note := paste0("# rows in stage == # rows in load_raw for each year in ", min(year(rows.qa$date_min)), ":", max(year(rows.qa$date_max)))]
        
        tbl_id_qa_bir <- DBI::Id(schema = "metadata", 
                             table = "qa_bir")
        dbWriteTable(db_apde, 
                     tbl_id_qa_bir, 
                     value = as.data.frame(qa_bir),
                     overwrite = F, 
                     append = T )
    
  }
  
## The end! ----

