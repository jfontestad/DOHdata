## Header ####
    # Author: Danny Colombara
    # 
    # R version: 3.5.3
    #
    # Purpose: QA staged birth data to ensure not corrupted when pushed to SQL and recoding was executed properly
    # 
    # Notes: did not check all variables vis-à-vis CHAT, but rather a random assorment

## Set up environment ----
    rm(list=setdiff(ls(), c("db_apde", "bir_combined")))
 
    #  db_apde <- dbConnect(odbc(), "PH_APDEStore50")

## Load staged from SQL ----
    table_config_stage_bir_wa <- yaml::yaml.load(getURL(
      "https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/stage/create_stage.bir_wa.yaml"))
    
    if(!exists("bir_combined")){ # only load data if needed
      query.varlist <- paste(c(names(table_config_stage_bir_wa$vars), names(table_config_stage_bir_wa$recodes)), collapse = ", ")
      query.string <- glue_sql ("SELECT ", query.varlist, " FROM stage.bir_wa")
      bir_combined <- setDT(DBI::dbGetQuery(db_apde, query.string))
    }
    
## Identify columns that are 100% missing -----
    all.miss <- lapply(bir_combined, function(x)all(is.na(x)))
    all.miss <- data.table(name = names(all.miss), all.missing = all.miss)
    print(all.miss[all.missing == TRUE])
    
## Identify numeric columns ----
    numerics <- data.table(name = names(table_config_stage_bir_wa$vars), type = as.character(table_config_stage_bir_wa$vars))
    numerics <- numerics[type %in% c("DATE", "INTEGER", "NUMERIC")]
    
## Check date range -----
    date.vars <- numerics[type=="DATE"]$name
    bir_combined[, (date.vars) :=lapply(.SD, lubridate::ymd), .SDcols = date.vars]
    
    print(rbind(suppressWarnings(bir_combined[, lapply(.SD, min, na.rm = TRUE), .SDcols = date.vars]), 
          suppressWarnings(bir_combined[, lapply(.SD, max, na.rm = TRUE), .SDcols = date.vars])))
    
## COMPARE TO CHAT ----    
    # Births compared to CHAT ----
        births.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_total_births.csv", 
                            skip = 5, nrows = 15)[, .(chi_year = V1, count.doh = V7)]
        births <- setorder(bir_combined[chi_geo_wastate=="Yes", .N, by = chi_year], chi_year)
        births <- merge(births, births.doh, by.x = "chi_year", by.y = "chi_year"); rm(births.doh)
        births[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        births[, indicator := "births"]
        print(births)
        
    # Race compared to CHAT ----
        race <- setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, chi_race_7)], row.names = TRUE), chi_year, chi_race_7)    
        
        race.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_race.csv", 
                          skip = 5, nrows = 84)[, .(chi_year = as.character(V1), race3 = V4, count.doh = V7)]
        race.doh[race3 == "American Indian/Alaskan Native Only", race3 := "AIAN"]
        race.doh[race3 == "Asian Only", race3 := "Asian"]
        race.doh[race3 == "Black Only", race3 := "Black"]
        race.doh[race3 == "Multi Race", race3 := "Multiple"]
        race.doh[race3 == "Pacific Islander Only", race3 := "NHPI"]
        race.doh[race3 == "White Only", race3 := "White"]
                            
        race <- merge(race, race.doh, by.x = c("chi_year", "chi_race_7"), by.y = c("chi_year", "race3"), all.x = TRUE); rm(race.doh)
        race[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        print(race)
        
    # Hispanic compared to CHAT ----
        hispanic <- setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, chi_race_hisp)], row.names = TRUE), chi_year)
        hispanic <- hispanic[chi_race_hisp=="Hispanic"]
        
        hispanic.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_hispanic.csv", 
                              skip = 5, nrows = 15)[, .(chi_year = as.character(V1), count.doh = V7)]
        
        hispanic <- merge(hispanic, hispanic.doh, by = "chi_year"); rm(hispanic.doh)
        hispanic[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        print(hispanic)
        
    # Birthweight compared to CHAT ----
        bw <- rbind(
          setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, bw_vlow)], row.names = TRUE), chi_year)[bw_vlow=="Yes",][, bw_vlow := NULL][, bw := "bw_vlow"],
          setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, bw_modlow)], row.names = TRUE), chi_year)[bw_modlow=="Yes",][, bw_modlow := NULL][, bw := "bw_modlow"],
          setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, bw_norm)], row.names = TRUE), chi_year)[bw_norm=="Yes",][, bw_norm := NULL][, bw := "bw_norm"],
          setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, bw_high)], row.names = TRUE), chi_year)[bw_high=="Yes",][, bw_high := NULL][, bw := "bw_high"],
          setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, bw_vhigh)], row.names = TRUE), chi_year)[bw_vhigh=="Yes",][, bw_vhigh := NULL][, bw := "bw_vhigh"]
        )
        
        bw.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_birthweight.csv", 
                        skip = 5, nrows = 75)[, .(chi_year = as.character(V1), bw = V6, count.doh = V7)]
        bw.doh[bw == "Birth WT-Very Low (227-1499 grams)", bw := "bw_vlow"]
        bw.doh[bw == "Birth WT-Moderately Low (1500-2499 grams)", bw := "bw_modlow"]
        bw.doh[bw == "Birth WT-Normal (2500-3999 grams)", bw := "bw_norm"]
        bw.doh[bw == "Birth WT-High (4000-4499 grams)", bw := "bw_high"]
        bw.doh[bw == "Birth WT-Very High (4500-8164 grams)", bw := "bw_vhigh"]
        
        bw <- merge(bw, bw.doh, by = c("chi_year", "bw"), all.x = TRUE); rm(bw.doh)
        bw[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        print(bw)
        
    # Premature compared to CHAT ----
        preterm <- setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, preterm)], row.names = TRUE), chi_year)[preterm=="Yes", -c("preterm")]
        
        preterm.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_premature.csv", 
                              skip = 5, nrows = 15)[, .(chi_year = as.character(V1), count.doh = V7)]
        
        preterm <- merge(preterm, preterm.doh, by = "chi_year"); rm(preterm.doh)
        preterm[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        print(preterm)
    
    # Mom born in USA compared to CHAT ----
        mother_birthplace_usa <- setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, mother_birthplace_usa)], row.names = TRUE), chi_year)[mother_birthplace_usa==1, -c("mother_birthplace_usa")]
        
        mother_birthplace_usa.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_usa.csv", 
                             skip = 5, nrows = 15)[, .(chi_year = as.character(V1), count.doh = V7)]
        
        mother_birthplace_usa <- merge(mother_birthplace_usa, mother_birthplace_usa.doh, by = "chi_year"); rm(mother_birthplace_usa.doh)
        mother_birthplace_usa[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        print(mother_birthplace_usa)        
        
    # Prior pregnancy compared to CHAT ----
        ch_priorpreg <- setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, ch_priorpreg)], row.names = TRUE), chi_year)[ch_priorpreg==1, -c("ch_priorpreg")]
        
        ch_priorpreg.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_priorpreg.csv", 
                                           skip = 5, nrows = 15)[, .(chi_year = as.character(V1), count.doh = V7)]
        
        ch_priorpreg <- merge(ch_priorpreg, ch_priorpreg.doh, by = "chi_year"); rm(ch_priorpreg.doh)
        ch_priorpreg[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        print(ch_priorpreg)   
        
    # NCHS Prenatal Care compared to CHAT (only availble >=2011) ----
        pnc <- setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, pnc_tri_nchs)], row.names = TRUE), chi_year, pnc_tri_nchs)    
        pnc <- pnc[chi_year>=2011]
        
        pnc.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_pnc_nchs.csv", 
                         skip = 5, nrows = 45)[, .(chi_year = as.character(V1), pnc.doh=V6, count.doh = V7)]
        pnc.doh[pnc.doh == "NCHS Prenatal Care Init.-1st Trimester", pnc.doh := "NCHS Trimester 1"]
        pnc.doh[pnc.doh == "NCHS Prenatal Care Init.-2nd Trimester", pnc.doh := "NCHS Trimester 2"]
        pnc.doh[pnc.doh == "NCHS Prenatal Care Init.-3rd Trimester", pnc.doh := "NCHS Trimester 3"]
        pnc.doh[, pnc_tri_nchs := pnc.doh][, pnc.doh := NULL]
        pnc.doh <- pnc.doh[chi_year>=2011]
        
        pnc.nchs <- merge(pnc, pnc.doh, by = c("chi_year", "pnc_tri_nchs"), all.x = TRUE); rm(pnc.doh)
        pnc.nchs[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        pnc.nchs <- pnc.nchs[pnc_tri_nchs != "NCHS No PNC" & chi_year >= 2011]
        print(pnc.nchs)
        
    # Prenatal Care compared to CHAT ----
        pnc <- setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, pnc_tri)], row.names = TRUE), chi_year, pnc_tri)    
        
        pnc.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_pnc.csv", 
                         skip = 5, nrows = 60)[, .(chi_year = as.character(V1), pnc.doh=V6, count.doh = V7)]
        pnc.doh[pnc.doh == "Prenatal Care Init.-First Trimester", pnc.doh := "Trimester 1"]
        pnc.doh[pnc.doh == "Prenatal Care Init.-Second Trimester", pnc.doh := "Trimester 2"]
        pnc.doh[pnc.doh == "Prenatal Care Init.-Third Trimester", pnc.doh := "Trimester 3"]
        pnc.doh[pnc.doh == "Prenatal Care Init.-No Prenatal Care", pnc.doh := "No PNC"]
        pnc.doh[, pnc_tri := pnc.doh][, pnc.doh := NULL]
        
        pnc <- merge(pnc, pnc.doh, by = c("chi_year", "pnc_tri"), all.x = TRUE); rm(pnc.doh)
        pnc[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        print(pnc)
        
    # Hypertension - gestational compared to CHAT ----
        htn_gest <- setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, htn_gest)], row.names = TRUE), chi_year)[htn_gest=="Yes", -c("htn_gest")]
        
        htn_gest.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_htn_gest.csv", 
                                           skip = 5, nrows = 15)[, .(chi_year = as.character(V1), count.doh = V7)]
        
        htn_gest <- merge(htn_gest, htn_gest.doh, by = "chi_year"); rm(htn_gest.doh)
        htn_gest[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        print(htn_gest)  
        
    # Hypertension - Prepregnancy compared to CHAT ----
        htn_prepreg <- setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, htn_prepreg)], row.names = TRUE), chi_year)[htn_prepreg=="Yes", -c("htn_prepreg")]
        
        htn_prepreg.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_htn_prepreg.csv", 
                              skip = 5, nrows = 15)[, .(chi_year = as.character(V1), count.doh = V7)]
        
        htn_prepreg <- merge(htn_prepreg, htn_prepreg.doh, by = "chi_year"); rm(htn_prepreg.doh)
        htn_prepreg[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        print(htn_prepreg)      
        
    # Kotelchuck Index (adequate PNC) compared to CHAT ----
        kotelchuck <- setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, kotelchuck)], row.names = TRUE), chi_year)[kotelchuck==1, -c("kotelchuck")]
        
        kotelchuck.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_kotelchuck_adequate.csv", 
                                 skip = 5, nrows = 15)[, .(chi_year = as.character(V1), count.doh = V7)]
        
        kotelchuck <- merge(kotelchuck, kotelchuck.doh, by = "chi_year"); rm(kotelchuck.doh)
        kotelchuck[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        print(kotelchuck) 
        
    # Kotelchuck Index (inadequate PNC) compared to CHAT ----
        kotelchuck.inadequate <- setorder(as.data.table(bir_combined[chi_geo_wastate=="Yes", table(chi_year, kotelchuck)], row.names = TRUE), chi_year)[kotelchuck==0, -c("kotelchuck")]
        
        kotelchuck.inadequate.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_kotelchuck_inadequate.csv", 
                                skip = 5, nrows = 15)[, .(chi_year = as.character(V1), count.doh = V7)]
        
        kotelchuck.inadequate <- merge(kotelchuck.inadequate, kotelchuck.inadequate.doh, by = "chi_year"); rm(kotelchuck.inadequate.doh)
        kotelchuck.inadequate[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        print(kotelchuck.inadequate)    
        
    # Births in KC compared to CHAT ----
        kc.births.doh <- fread("//phdata01/DROF_DATA/DOH DATA/Births/DATA/BIRTH/DOH_Birth_Tables_Summary_for_Comparison/2003_2017_total_births_kc.csv", 
                            skip = 5, nrows = 15)[, .(chi_year = V1, count.doh = V7)]
        kc.births <- setorder(bir_combined[chi_geo_kc==, .N, by = chi_year], chi_year)
        kc.births <- merge(kc.births, kc.births.doh, by = "chi_year"); rm(kc.births.doh)
        kc.births[, diff := paste0(round(100* (N - count.doh) / count.doh, 2), "%")]
        print(kc.births)
        
## Check some cross-tabulations ----
            print("*****************************")
            print("**** Simple cross-tabs  *****")
            print("*****************************")
            bir_combined[, table(plurality, singleton, exclude = NULL)]
            print("*****************************")
            bir_combined[, table(plurality, multiple, exclude = NULL)]
            print("*****************************")
            bir_combined[, table(breastfed, bf, exclude = NULL)]
            print("*****************************")
            bir_combined[, table(infant_death_flag, inf_death, exclude = NULL)]
            print("*****************************")
            bir_combined[, table(mother_residence_county_wa_code, chi_geo_kc, exclude = NULL)]
            print("*****************************")
            bir_combined[, table(mother_residence_state_nchs_code, chi_geo_wastate, exclude = NULL)]
            print("*****************************")
            bir_combined[, table(source_of_payment, dlp_medicaid, exclude = NULL)]
            print("*****************************")
            bir_combined[, table(nchs_month_prenatal_care_began, pnc_tri_nchs, exclude = NULL)]
            print("*****************************")
            bir_combined[, table(month_prenatal_care_began, pnc_tri, exclude = NULL)]
            print("*****************************")
            bir_combined[, table(priorprg, ch_priorpreg, exclude = NULL)]

## Compare row counts by year with raw data ----          
  rows.raw <- setDT(DBI::dbGetQuery(db_apde, "SELECT *  FROM metadata.qa_bir_values"))       
  rows.raw <- rows.raw[table_name %in% c("load_raw.bir_wa_2003_2016", "load_raw.bir_wa_2017_20xx")] # limit to tables of interest       
  rows.raw[, qa_date_max := max(qa_date), by = c("table_name", "date_max")]  # identify the most recent QA for each table/year combo
  rows.raw <- rows.raw[as.Date(date_max) - as.Date(date_min) <367]  # drop if time is > 1 year
  rows.qa <- rows.raw[qa_date_max == qa_date][, chi_year := year(date_min)][, raw := as.numeric(qa_value)] # keep as template for loading new QA to SQL
  rows.raw <- rows.qa[, .(chi_year, raw)] # keep the most recent

  row.count <- setorder(as.data.table(bir_combined[, table(chi_year)], row.names = TRUE), chi_year)[, chi_year := as.numeric(chi_year)]
  
  row.count <- merge(row.count, rows.raw, by = "chi_year"); rm(rows.raw)
  row.count[, diff := paste0(round(100* (N - raw) / raw, 2), "%")]
  row.count[, indicator := "number of rows in stage vs load_raw"]
  print(row.count)
  
## Write QA row count results to SQL ----
  if(sum(row.count$N) == sum(row.count$raw)){ 
    ## WRITE QA RESULTS TO VALUES TABLE ----
        rows.total <- copy(rows.qa)[, date_min := "2003-01-01"][, date_max := paste0(max(row.count$chi_year), "-12-31")][, raw := sum(row.count$N)][1,]
        rows.qa <- rbind(rows.qa, rows.total)
        rows.qa[, qa_value := as.character(raw)]
        rows.qa[, table_name := "stage.bir_wa"]
        rows.qa[, c("date_min", "date_max") := lapply(.SD, as.Date), .SDcols = c("date_min", "date_max")]
        rows.qa[, note := paste0("[", year(date_min), ", ", year(date_max), "]")]
        rows.qa <- rows.qa[, !c("qa_date_max", "chi_year", "raw")]
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

