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
    # devtools::install_local("C:/Users/dcolombara/code/hysdataprep/", force=T)

## Set up environment ----
    pacman::p_load(hysdataprep)
    source("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/danny/ETL/birth/stage/enact_recoding_function.R")
    
## Create year column that is needed for recoding----
    bir_combined[, year := date_of_birth_year]
    
## Load recode data ----
    recodes <- data.table::fread("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/danny/ETL/birth/ref/ref.bir_recodes_simple.csv")
    recodes <- recodes[recode_type != "complex"]
    recodes <- recodes[, .(old_var, new_var, old_value, new_value, new_label, start_year, end_year, var_label)]
    # recodes <- recodes[c(1:24), ]

## Process with recoding functions / package ----
    # Test a single recode
      # my.recode <- unlist(parse_recode_instructions(recodes, catch_NAs = T), recursive = F)
      # data = copy(sql.dt)
      # year = 2006
      # recode = my.recode
      # y <- apply_recode(data = sql.dt, year = 2006, recode = my.recode, jump_scope = F, return_vector = F) # one at a time
    
    # Test a list of recodes
      data = copy(bir_combined)
      year = 2006
      ignore_case = T
      hypothetical = F
      recodes.list <- parse_recode_instructions(recodes, catch_NAs = T)
      dots = recodes.list

      x <- enact_recoding(data = data, year = 2006, recodes.list, ignore_case = T, hypothetical = F)
    
      
      
      
## Custom code for complex recoding ----
    # Create copy of raw data ----
      custom <- copy(bir_combined)
    
    # vertex (vertex birth position) ----
      custom[year %in% c(2003:2015) & (fetal_presentation == "C"), vertex := 0]
      custom[year %in% c(2003:2015) & (fetal_presentation %in% c("B", "o")), vertex := 1]
      custom[year %in% c(2003:2015) & (labchar1==3 | labchar2==3 | labchar3==3 | labchar4==3 | labchar5==3 | labchar6==3 | labchar7==3), vertex := 1]
      custom[, vertex := factor(vertex, levels = c(0, 1), labels = c("Breech/Oth", "Vertex"))]

    # smoking (Smoking-Yes (before & during pregnancy)) ----
      custom[year %in% c(2003:2015), smoking := NA_integer_]
      custom[year %in% c(2003:2015) & (cigarettes_smoked_3_months_prior==0 & cigarettes_smoked_1st_tri==0 & cigarettes_smoked_2nd_tri==0 & cigarettes_smoked_3rd_tri==0), smoking := 0]
      custom[year %in% c(2003:2015) & (cigarettes_smoked_3_months_prior %in% c(1:98) | cigarettes_smoked_1st_tri %in% c(1:98) | cigarettes_smoked_2nd_tri %in% c(1:98) | cigarettes_smoked_3rd_tri %in% c(1:98)), smoking := 1]
      custom[, smoking := factor(smoking, levels = c(0, 1), labels = c("No", "Yes"))]

    # smoking_dur (whether mother smoked at all during pregnancy) ----
      custom[year %in% c(2000:2002) & smoking=="N", smoking_dur := 0]
      custom[year %in% c(2000:2002) & smoking=="Y", smoking_dur := 1]

      custom[year %in% c(2003:2015), smoking_dur := NA_integer_]
      custom[year %in% c(2003:2015) & (cigarettes_smoked_1st_tri==0 & cigarettes_smoked_2nd_tri==0 & cigarettes_smoked_3rd_tri==0), smoking_dur := 0]
      custom[year %in% c(2003:2015) & (cigarettes_smoked_1st_tri %in% c(1:98) | cigarettes_smoked_2nd_tri %in% c(1:98) | cigarettes_smoked_3rd_tri %in% c(1:98)), smoking_dur := 1]
      
      custom[, smoking_dur := factor(smoking_dur, levels = c(0, 1), labels = c("No", "Yes"))]

    # wtgain (CHAT categories of maternal weight gain) ----
            custom[year %in% c(2000:2015) &
                     ((mother_bmi<18.5 & mother_weight_gain<28) | 
                     (mother_bmi>=18.5 & mother_bmi<=24.99 & mother_weight_gain<25) | 
                     (mother_bmi>=25.0 & mother_bmi<=29.99 & mother_weight_gain<15) | 
                     (mother_bmi>=30.0 & mother_bmi<99.9 & mother_weight_gain<11)), 
                   wtgain := 1]
            
            custom[year %in% c(2000:2015) & is.na(wtgain) & 
                     ((mother_bmi<18.5 & mother_weight_gain>=28 & mother_weight_gain<=40) | 
                     (mother_bmi>=18.5 & mother_bmi<=24.9 & mother_weight_gain>=25 & mother_weight_gain<=35) | 
                     (mother_bmi>=25.0 & mother_bmi<=29.9 & mother_weight_gain>=15 & mother_weight_gain<=25) | 
                     (mother_bmi>=30.0 & mother_bmi<99.9 & mother_weight_gain>=11 & mother_weight_gain<=20)), 
                   wtgain := 2]
            
            custom[year %in% c(2000:2015) & is.na(wtgain) & 
                     ((mother_bmi<18.5 & mother_weight_gain>40) | 
                        (mother_bmi>=18.5 & mother_bmi<=24.9 & mother_weight_gain>35) | 
                        (mother_bmi>=25.0 & mother_bmi<=29.9 & mother_weight_gain>25) | 
                        (mother_bmi>=30.0 & mother_bmi<99.9 & mother_weight_gain>20 & mother_weight_gain<999)), 
                   wtgain := 3]       
            
            custom[, wtgain := factor(wtgain, levels = c(1:3), labels = c("below recommended", "recommended", "above recommended"))]
    
    # wtgain_rec (WT Gain-Recommended) ----
            custom[year %in% c(2000:2015) & (wtgain %in% c("below recommended", "above recommended")),  wtgain_rec := 0]
            custom[year %in% c(2000:2015) & (wtgain=="recommended"), wtgain_rec := 1]
            custom[, wtgain_rec := factor(wtgain_rec, levels = c(0, 1), labels = c("No", "Yes"))]

    # diab_no (Diabetes-No) ----
          custom[year %in% c(2000:2015) & (diabetes %in% c("E", "G", "U")), diab_no := 0]
          custom[year %in% c(2000:2015) & (is.na(diabetes)), diab_no := 1] # blank means no diabetes
          custom[, diab_no := factor(diab_no, levels = c(0, 1), labels = c("No", "Yes"))]

    # diab_prepreg (Diabetes-Pre Pregnancy) ----
          custom[year %in% c(2000:2015) & (is.na(diabetes) | diabetes == "G"), diab_prepreg := 0] # it is is unknown type, can't categorize it
          custom[year %in% c(2000:2015) & (diabetes == "E"), diab_prepreg := 1] # blank means no diabetes
          custom[, diab_prepreg := factor(diab_prepreg, levels = c(0, 1), labels = c("No", "Yes"))]

    # diab_gest (Diabetes-Gestational) ----
          custom[year %in% c(2000:2015) & (is.na(diabetes) | diabetes == "E"), diab_gest := 0] # it is is unknown type, can't categorize it
          custom[year %in% c(2000:2015) & (diabetes == "G"), diab_gest := 1] # blank means no diabetes
          custom[, diab_gest := factor(diab_gest, levels = c(0, 1), labels = c("No", "Yes"))]

    # htn_no (Hypertension-No) ----
          custom[year %in% c(2003:2015) & (hyperflg %in% c("E", "G", "U")), htn_no := 0]
          custom[year %in% c(2003:2015) & (is.na(hyperflg)), htn_no := 1] # blank means no diabetes
          custom[, htn_no := factor(htn_no, levels = c(0, 1), labels = c("No", "Yes"))]

    # htn_prepreg (Hypertension-Pre Pregnancy) ----
          custom[year %in% c(2003:2015) & (is.na(hyperflg) | hyperflg == "G"), htn_prepreg := 0] # it is is unknown type, can't categorize it
          custom[year %in% c(2003:2015) & (hyperflg == "E"), htn_prepreg := 1] # blank means no hypertension
          custom[, htn_prepreg := factor(htn_prepreg, levels = c(0, 1), labels = c("No", "Yes"))]

    # htn_gest (Hypertension-Gestational) ----
          custom[year %in% c(2003:2015) & (is.na(hyperflg) | hyperflg == "E"), htn_gest := 0] # it is is unknown type, can't categorize it
          custom[year %in% c(2003:2015) & (hyperflg == "G"), htn_gest := 1] # blank means no hypertension
          custom[, htn_gest := factor(htn_gest, levels = c(0, 1), labels = c("No", "Yes"))]

    # nullip (nulliparous) ----
      custom[year %in% c(1989:2015) & (prior_live_births_living == 0 & prior_live_births_deceased == 0), nullip := 1]
      custom[year %in% c(1989:2015) & (prior_live_births_living %in% c(1:98)), nullip := 0]
      custom[year %in% c(1989:2015) & (prior_live_births_deceased %in% c(1:98)), nullip := 0]
      custom[, nullip := factor(nullip, levels = c(0, 1), labels = c("No", "Yes"))]
      
    # ntsv (nulliparous, term, singleton, vertex birth) ----
      custom[year %in% c(2003:2015), ntsv := 0] 
      custom[year %in% c(2003:2015) & (nullip == "Yes" & preterm == "No" & plural == 1 & vertex == "Vertex"), ntsv := 1]
      custom[, ntsv := factor(ntsv, levels = c(0, 1), labels = c("No", "Yes"))]
          
      
    # pnc_lateno (Late or no prenatal care) ----
      custom[month_prenatal_care_began %in% c(1:6), pnc_lateno := 0]
      custom[month_prenatal_care_began %in% c(0, 7:10), pnc_lateno := 1]
      
    # kotel.am (Kotelchuck Index) Alastair Method ----
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
      
      custom$kotelchuck <- kotelchuck[cbind(match(custom$calculated_gestation, rownames(kotelchuck)), match(custom$month_prenatal_care_began, colnames(kotelchuck)))]
      
      custom <- custom %>% mutate(
        kotel.am = case_when(
          # Missing values needed to calculate
          is.na(month_prenatal_care_began) | month_prenatal_care_began == 999 ~ NA_real_,
          is.na(number_prenatal_visits) | number_prenatal_visits == 99 ~ NA_real_,
          calculated_gestation == 0 ~ NA_real_,
          # Met/did not meet the threshold
          month_prenatal_care_began > 4 & month_prenatal_care_began < 15 ~ 0,
          number_prenatal_visits < 0.8 * kotelchuck ~ 0,
          number_prenatal_visits >= 0.8 * kotelchuck ~ 1,
          number_prenatal_visits == 0 ~ 0,
          # Catch everything else as missing
          is.na(kotelchuck) ~ NA_real_,
          TRUE ~ NA_real_
        ))
      
      setDT(custom)
      
    # kotel.doh (Kotelchuck Index) DOH METHOD ----
      # standardize and clean starting birth certificate variables ----
          custom[, PrenatalBeganMonth := month_prenatal_care_began]
          custom[, PrenatalVisitsCount := number_prenatal_visits]
          custom[, CalculatedGestationalAge := calculated_gestation]
          
          custom[!(PrenatalBeganMonth %in% c(0:10)), PrenatalBeganMonth := NA]
          custom[(CalculatedGestationalAge < 60) & (PrenatalBeganMonth > CalculatedGestationalAge/4), PrenatalBeganMonth := NA]
          
          custom[!(CalculatedGestationalAge %in% c(18:50)), CalculatedGestationalAge := NA]
        
          custom[(PrenatalBeganMonth=0 & PrenatalVisitsCount >=1) | (PrenatalVisitsCount=0 & PrenatalBeganMonth >= 1), 
                 c("PrenatalBeganMonth", "PrenatalVisitsCount") := NA]
          
          custom[(PrenatalBeganMonth=0 & is.na(PrenatalVisitsCount)), PrenatalBeganMonth := NA]
      
          custom[(PrenatalVisitsCount=0 & is.na(PrenatalBeganMonth)), PrenatalVisitsCount := NA]
          
      # skip imputation ----
      # create month index ----
          custom[PrenatalBeganMonth ==  1,  MonthIndex := 4]
          custom[PrenatalBeganMonth ==  2,  MonthIndex := 4]
          custom[PrenatalBeganMonth ==  3,  MonthIndex := 3]
          custom[PrenatalBeganMonth ==  4,  MonthIndex := 3]
          custom[PrenatalBeganMonth ==  5,  MonthIndex := 2]
          custom[PrenatalBeganMonth ==  6,  MonthIndex := 2]
          custom[PrenatalBeganMonth >=  7,  MonthIndex := 1]      
          
          custom[is.na(PrenatalBeganMonth) | is.na(PrenatalVisitsCount), MonthIndex := NA]
          
      # create Expected visits #1 ----
          custom[(CalculatedGestationalAge %in% c( 0: 5)), ExpectedVisitsCalc1 :=  0]
          custom[(CalculatedGestationalAge %in% c( 6: 9)), ExpectedVisitsCalc1 :=  1]
          custom[(CalculatedGestationalAge %in% c(10:13)), ExpectedVisitsCalc1 :=  2]
          custom[(CalculatedGestationalAge %in% c(14:17)), ExpectedVisitsCalc1 :=  3]
          custom[(CalculatedGestationalAge %in% c(18:21)), ExpectedVisitsCalc1 :=  4]
          custom[(CalculatedGestationalAge %in% c(22:25)), ExpectedVisitsCalc1 :=  5]
          custom[(CalculatedGestationalAge %in% c(26:29)), ExpectedVisitsCalc1 :=  6]
          custom[(CalculatedGestationalAge %in% c(30:31)), ExpectedVisitsCalc1 :=  7]
          custom[(CalculatedGestationalAge %in% c(32:33)), ExpectedVisitsCalc1 :=  8]
          custom[(CalculatedGestationalAge %in% c(34:35)), ExpectedVisitsCalc1 :=  9]
          custom[(CalculatedGestationalAge %in% c(35:299)), ExpectedVisitsCalc1 :=  (CalculatedGestationalAge - 35L) + 9L]
          
      # create Expected visits #2 ----
          custom[PrenatalBeganMonth >= 9, ExpectedVisitsCalc2 := ExpectedVisitsCalc1 - 13]
          custom[PrenatalBeganMonth == 8, ExpectedVisitsCalc2 := ExpectedVisitsCalc1 - 9]
          custom[PrenatalBeganMonth == 7, ExpectedVisitsCalc2 := ExpectedVisitsCalc1 - 7]
          custom[PrenatalBeganMonth == 6, ExpectedVisitsCalc2 := ExpectedVisitsCalc1 - 6]
          custom[PrenatalBeganMonth == 5, ExpectedVisitsCalc2 := ExpectedVisitsCalc1 - 5]
          custom[PrenatalBeganMonth == 4, ExpectedVisitsCalc2 := ExpectedVisitsCalc1 - 3]	
          custom[PrenatalBeganMonth == 3, ExpectedVisitsCalc2 := ExpectedVisitsCalc1 - 2]	
          custom[PrenatalBeganMonth == 2, ExpectedVisitsCalc2 := ExpectedVisitsCalc1 - 1]
          custom[PrenatalBeganMonth == 1, ExpectedVisitsCalc2 := ExpectedVisitsCalc1]          
          
          custom[is.na(PrenatalBeganMonth), ExpectedVisitsCalc2 := NA]
          
      # create Expected visits #3 ----
          custom[, ExpectedVisitsCalc3 := ExpectedVisitsCalc2]
          custom[ExpectedVisitsCalc2 <= 0, ExpectedVisitsCalc3 := 1]
          
      # create Expected visits #4 ----
          custom[, ExpectedVisitsCalc4 := (PrenatalVisitsCount / ExpectedVisitsCalc3) * 100 ]
          custom[is.na(PrenatalBeganMonth) | is.na(PrenatalVisitsCount), ExpectedVisitsCalc4 := NA]
          
      # Create CHAT Kotelchuck Index ----
          custom[MonthIndex < 3, ChatKotelchuckIndex := 1]
          custom[ExpectedVisitsCalc4 >= 110, ChatKotelchuckIndex := 4]
          custom[(ExpectedVisitsCalc4 >= 80) & (ExpectedVisitsCalc4 < 110), ChatKotelchuckIndex := 3]
          custom[(ExpectedVisitsCalc4 >= 50) & (ExpectedVisitsCalc4 < 80), ChatKotelchuckIndex := 2]
          custom[ExpectedVisitsCalc4 < 50, ChatKotelchuckIndex := 1]
          
          custom[is.na(PrenatalBeganMonth) | is.na(PrenatalVisitsCount), ChatKotelchuckIndex := NA]
          
      # Create CHATE Binary Kotelchuck Index ----    
          custom[ChatKotelchuckIndex %in% c(3, 4), CHATkotel := 1] # kotelchuck adequate (>= 80% of expected visits)
          custom[ChatKotelchuckIndex %in% c(1, 2), CHATkotel := 0] # kotelchuck adequate (< 80% of expected visits)
          custom[, CHATkotel := factor(CHATkotel, levels = c(0, 1), labels = c("No", "Yes"))]
          
    # kotel(Kotelchuck Index) LEGIT METHOD ----
      #- Notes ----
          # based on SAS code (https://www.ncemch.org/databases/HSNRCPDFs/APNCU994_20SAS.pdf) ... 
          # named variables according to Milton Kotelchuck's code to ease code translation
          # order / process is the same as Milton Kotelchuck's, to facilitate comparison / troubleshooting
      
      #- Standardize & clean starting birth certificate variables ----
          # number of prenatal care visits from birth certificate
              custom[, npcvbc := number_prenatal_visits]
              custom[!(npcvbc %in% c(0:90)), npcvbc := NA] # cleaned according to SAS instructions
          
          # month prenatal care began, from birth certificate
              custom[, mpcbbc := month_prenatal_care_began] 
              custom[!(mpcbbc %in% c(0:10)), mpcbbc := NA] # cleaned according to SAS instructions
        
          # gestational age calculated from birth certificate    
              custom[, gagebc := calculated_gestation] 
              custom[!(gagebc %in% c(18:50)), gagebc := NA] # cleaned according to SAS instructions
              
              custom[(mpcbbc > (gagebc/4)), mpcbbc := NA]# cleaned according to SAS instructions (not grouped under mpcbbc code because needs gagebc to be created first)          
              
          # sex of infant, from birth certificate
              custom[, sexbc := sex]
              custom[sexbc %in% c("F", "U"), sexbc := "2"] # cleaned according to SAS instructions (yes, unknown is supposed to code to female)
              custom[sexbc %in% c("M"), sexbc := "1"] # cleaned according to SAS instructions
              custom[, sexbc := as.integer(sexbc)]
              
          # birth weight in grams
              custom[, bwgrams := birth_weight_grams] 
              custom[!(bwgrams %in% c(400:6000)), bwgrams := NA]
              
      #- Clean data to establish NO PNC ----
          custom[(npcvbc==0 & mpcbbc >= 1) | (mpcbbc=0 & npcvbc>=1), c("npcvbc", "mpcbbc") := NA]
          custom[npcvbc==0 & is.na(mpcbbc), mpcbbc := 0]
          custom[mpcbbc==0 & is.na(npcvbc), npcvbc := 0]
              
      #- Impute missing gestational age ----
          custom[, gestimp := NA_integer_]
          custom[gagebc %in% c(18:50), gestimp := 1] # gestimp == 1 >> not imputed
          custom[!(gagebc %in% c(18:50)), gestimp := 2] #  # gestimp == 2 >> imputed
          
          # Males
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(530:608), gagebc := 22]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(609:698), gagebc := 23]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(699:799), gagebc := 24]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(800:912), gagebc := 25]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(913:1040), gagebc := 26]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(1041:1183), gagebc := 27]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(1184:1342), gagebc := 28]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(1343:1536), gagebc := 29]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(1537:1751), gagebc := 30]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(1752:1978), gagebc := 31]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(1979:2219), gagebc := 32]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(2220:2458), gagebc := 33]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(2459:2693), gagebc := 34]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(2694:2909), gagebc := 35]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(2910:3111), gagebc := 36]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(3112:3291), gagebc := 37]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(3292:3433), gagebc := 38]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(3434:3533), gagebc := 39]
              custom[is.na(gagebc) & sexbc==1 & bwgrams %in% c(3534:6000), gagebc := 40]
          
          # Females
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(496:568), gagebc := 22]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(569:650), gagebc := 23]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(651:744), gagebc := 24]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(745:849), gagebc := 25]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(850:968), gagebc := 26]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(969:1101), gagebc := 27]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(1102:1251), gagebc := 28]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(1252:1429), gagebc := 29]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(1430:1636), gagebc := 30]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(1637:1860), gagebc := 31]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(1861:2089), gagebc := 32]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(2090:2328), gagebc := 33]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(2329:2561), gagebc := 34]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(2562:2787), gagebc := 35]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(2788:2991), gagebc := 36]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(2992:3160), gagebc := 37]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(3161:3293), gagebc := 38]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(3294:3388), gagebc := 39]
              custom[is.na(gagebc) & sexbc==2 & bwgrams %in% c(3389:6000), gagebc := 40]
          
          custom[is.na(gagebc), gestimp := NA]
                  
      #- Calculate Adequacy of Initiation of Prenatal Care ----        
          custom[, moindex4 := NA_integer_]
          custom[npcvbc>=0 & mpcbbc %in% c(1:2), moindex4 := 4]     # Adequate Plus
          custom[npcvbc>=0 & mpcbbc %in% c(3:4), moindex4 := 3]     # Adequate
          custom[npcvbc>=0 & mpcbbc %in% c(5:6), moindex4 := 2]     # Intermediate
          custom[npcvbc>=0 & !(mpcbbc %in% c(1:6)) , moindex4 := 1] # Inadequate
          custom[is.na(mpcbbc) | is.na(npcvbc), moindex4 := 0]      # Missing information
          
      #- Expected visits calculations & Received Prenatal Care Services Index ----
          # There are two steps: (1) calculate the EXPECTED visits & (2) calculate the visit ratio of OBSERVED/EXPECTED
          # The OBSERVED/EXPECTED ratio is directly converted to the Received Prenatal Care Services Index

          #-- Unexpected visits ----
              # Accounting solely for length of gestation
                custom[, uexpvis := NA_integer_]
                  custom[gagebc >= 35, uexpvis:=(as.integer(gagebc)-35L)+9L]
                  custom[gagebc==34 & is.na(uexpvis), uexpvis:=9]
                  custom[gagebc>=32 & is.na(uexpvis), uexpvis:=8]
                  custom[gagebc>=30 & is.na(uexpvis), uexpvis:=7]
                  custom[gagebc>=26 & is.na(uexpvis), uexpvis:=6]
                  custom[gagebc>=22 & is.na(uexpvis), uexpvis:=5]
                  custom[gagebc>=18 & is.na(uexpvis), uexpvis:=4]
                  custom[gagebc>=14 & is.na(uexpvis), uexpvis:=3]
                  custom[gagebc>=10 & is.na(uexpvis), uexpvis:=2]
                  custom[gagebc>=6 & is.na(uexpvis), uexpvis:=1]
                  custom[gagebc>=0 & is.na(uexpvis), uexpvis:=0]
              
          #-- Expected visits ----
              # Adjusts for month of PNC initiation
                custom[, expvis := NA_integer_]
                  custom[is.na(mpcbbc) | mpcbbc==0, expvis := uexpvis]
                  custom[mpcbbc==10, expvis := uexpvis-17L]
                  custom[mpcbbc==9, expvis := uexpvis-13L]
                  custom[mpcbbc==8, expvis := uexpvis-9L]
                  custom[mpcbbc==7, expvis := uexpvis-7L]
                  custom[mpcbbc==6, expvis := uexpvis-6L]
                  custom[mpcbbc==5, expvis := uexpvis-5L]
                  custom[mpcbbc==4, expvis := uexpvis-3L]
                  custom[mpcbbc==3, expvis := uexpvis-2L]
                  custom[mpcbbc==2, expvis := uexpvis-1L]
                  custom[mpcbbc==1, expvis := uexpvis]
                  custom[expvis<=0, expvis := 1]
          
          #-- Calculate expected visits ratio ----
              custom[, evratio := (npcvbc/expvis)*100]
                  
      #- Calc adequacy of received service (expected visits) index ----
          custom[, evindex := NA_integer_]
          custom[is.na(evratio), evindex := 0]
          custom[is.na(mpcbbc), evindex := 0]
          custom[evindex!=0 & evratio > 109.99, evindex := 4]                    # Adequate Plus
          custom[evindex!=0 & evratio > 79.99 & evratio <= 109.99, evindex := 3] # Adequate
          custom[evindex!=0 & evratio > 49.99 & evratio <= 79.99, evindex := 2]  # Indeterminate
          custom[evindex!=0 & evratio <= 49.99, evindex := 1]                    # Inadequate
      
      #- Calc Kotelchuck Index (a.k.a. Adequacy of PNC utilization (APNCU) index) ----
          ## combines Adequacy of Initiation of Prenatal Care Index (MOINDEX4) & Adequacy of Received Prenatal Care Services Index (EVINDEX)
          ## to create Kotlechuck Index (APNCU)
            custom[, indexsum := NA_integer_]
            custom[evindex==0 | moindex4==0, indexsum := 0]           # Missing information
            custom[evindex==1 | moindex4 %in% c(1, 2), indexsum := 1] # Inadequate
            custom[evindex==3 | moindex4 %in% c(3, 4), indexsum := 3] # Adequate
            custom[evindex==4 | moindex4 %in% c(3, 4), indexsum := 4] # Adequate Plus
            custom[is.na(indexsum), indexsum := 2]                    # Intermediate
            
      #- Create flag for NO PNC ----
          custom[npcvbc==0 & (mpcbbc==0 | is.na(mpcbbc)), nopnc := 1] # No PNC
          custom[mpcbbc==0 & (npcvbc==0 | is.na(npcvbc)), nopnc := 1] # No PNC
          custom[is.na(nopnc), nopnc := 0] # Some PNC
          custom[is.na(npcvbc) | is.na(mpcbbc), nopnc := NA]
          
      #- Create final binary Kotelchuck indicator ----
          custom[indexsum %in% c(3, 4), kotel := 1] # kotelchuck adequate (>= 80% of expected visits)
          custom[indexsum %in% c(1, 2), kotel := 0] # kotelchuck adequate (< 80% of expected visits)
          custom[, kotel := factor(kotel, levels = c(0, 1), labels = c("No", "Yes"))]
          
    # csec_lowrisk (Cesarean section among low risk deliveries) ----
          custom[delivery_method_final %in% c(4, 5), csec_lowrisk := 0] # any c-section == 0 
          custom[delivery_method_final %in% c(4, 5) & ntsv=="Yes", csec_lowrisk:=1]
          custom[, csec_lowrisk := factor(csec_lowrisk, levels = c(0, 1), labels = c("No", "Yes"))]
          
          
## Check output vis a vis CHAT ----
          custom[mother_calculated_age==25 & date_of_birth_year == 2015 & mother_residence_county_wa_code==17, table(kotel)] # 681 adequate / 173 inadequate in 2015 for age 25
          custom[mother_calculated_age == 25 & date_of_birth_year == 2015 & mother_residence_county_wa_code==17, table(CHATkotel)]
          custom[mother_calculated_age == 25 & date_of_birth_year == 2015 & mother_residence_county_wa_code==17, table(kotel.am)]
          
          # custom[mother_calculated_age==25 & date_of_birth_year == 2015 & mother_residence_county_wa_code==17, table(plural)] #okay
          # custom[mother_calculated_age==25 & date_of_birth_year == 2015 & mother_residence_county_wa_code==17, table(nopnc)]      
          
## Save output ----
    
## The end! ----

          