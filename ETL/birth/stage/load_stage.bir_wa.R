#### CODE TO COMBINE BIRTH DATA FROM THE BEDROCK (2003-2016) AND WHALES (2017-) SYSTEMS
# Alastair Matheson, PHSKC (APDE)
#
# 2019-06


# Recode groups of variables where <group_name>1-<group_nameX> has been replaced
#     by specific flags rather than codes
# Standardize missing to be NULL
# Standardize country/county/state/city codes (not needed as there is literal and code now)


load_stage.bir_wa_f <- function(table_config_create = NULL,
                                table_config_load = NULL,
                                conn = db_apde) {
  
  #### PULL IN BOTH DATA SETS ####
  tbl_id_2013_2016 <- DBI::Id(schema = "load_raw", table = "bir_wa_2003_2016")
  bir_2013_2016 <- DBI::dbReadTable(conn, tbl_id_2013_2016)
  
  tbl_id_2017_20xx <- DBI::Id(schema = "load_raw", table = "bir_wa_2017_20xx")
  bir_2017_20xx <- DBI::dbReadTable(conn, tbl_id_2017_20xx)
  
  
  #### REMOVE FIELDS IN BEDROCK NOT COLLECTED AFTER 2003 ####
  ### NB. Need to do this BEFORE renaming variables because otherwise two 
  ###   WIC variables are created
  ### Remove variables not collected after 2003
  bir_2013_2016 <- bir_2013_2016 %>%
    select(-priorprg, -fd_lt20, -fd_ge20, -apgar1, -ind_num, -malf_sam, -herpes, 
           -smokenum, -drinknum, -contains("amnio"), -contains("dmeth"), 
           -contains("complab"), -racecdes, -hispcdes, -carepay, -wic, -firsteps, 
           -afdc, -localhd, -contains("ubleed"), -smoking, -drinking, -lb_f_nl, 
           -lb_f84, -lb_p_nl, -lb_p84, -lb_p_nd, -lb_f_nd, -fd_2036, -fd_ge37, 
           -contains("malficd"), -contains("mrfnon"), -ind_mo, -ind_yr, -lfd_mo, 
           -lfd_yr, -loth_mo, -loth_yr, -lpp_mo, -lpp_yr, -trib_res)
  
  
  #### STANDARDIZE NAMES FROM BEDROCK TO WHALES ####
  ### Bring in reference table
  field_maps <- vroom::vroom("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/ETL/birth/ref/ref.bir_field_name_map.csv")
  
  data.table::setnames(bir_2013_2016, 
                       field_maps$field_name_apde[match(names(bir_2013_2016), 
                                                        field_maps$field_name_bedrock)])
  
  
  #### SPLIT OUT VARIABLES THAT ARE NOW INDIVIDUAL FLAGS ####
  #### Medical risk factors ####
  # Can rely on the fact that the code for diabetes (01) only ever went in mrf1
  # and code for hypertension (02) only ever went in mrf1 and mrf2
  # Also rely on the fact that if mrf1 = "09" or "99" then all other mrf fields are NA
    bir_2013_2016 <- bir_2013_2016 %>% 
    mutate(
      prepreg_diabetes = case_when(
        mrf1 == "01" & diabetes == "E" ~ "Y",
        mrf1 == "01" & diabetes == "U" ~ "U",
        mrf1 == "01" & diabetes == "G" ~ "N",
        mrf1 != "01" & mrf1 != "99" ~ "N",
        TRUE ~ "U"
      ),
      gestational_diabetes = case_when(
        mrf1 == "01" & diabetes == "G" ~ "Y",
        mrf1 == "01" & diabetes == "U" ~ "U",
        mrf1 == "01" & diabetes == "E" ~ "N",
        mrf1 != "01" & mrf1 != "99" ~ "N",
        TRUE ~ "U"
      ),
      prepreg_hypertension = case_when(
        (mrf1 == "02" | mrf2 == "02") & hyperflg == "E" ~ "Y",
        (mrf1 == "02" | mrf2 == "02") & hyperflg == "U" ~ "U",
        (mrf1 == "02" | mrf2 == "02") & hyperflg == "G" ~ "N",
        mrf1 != "02" & mrf1 != "99" & mrf2 != "02" ~ "N",
        TRUE ~ "U"
      ),
      gestational_hypertention = case_when(
        (mrf1 == "02" | mrf2 == "02") & hyperflg == "G" ~ "Y",
        (mrf1 == "02" | mrf2 == "02") & hyperflg == "U" ~ "U",
        (mrf1 == "02" | mrf2 == "02") & hyperflg == "E" ~ "N",
        mrf1 != "02" & mrf1 != "99" & mrf2 != "02" ~ "N",
        TRUE ~ "U"
      ),
      risk_factors_none = case_when(
        mrf1 == "09" & mrf2 == "09" & mrf3 == "09" & mrf4 == "09" &
          mrf5 == "09" & mrf6 == "09" ~ "Y",
        (mrf1 != "09" & mrf1 != "99") ~ "N",
        TRUE ~ "U"
      ),
      risk_factors_unknown = case_when(
        mrf1 == "99" ~ "Y",
        mrf1 != "99" ~ "N",
        TRUE ~ "U"
      )
    )
    
    # Other medical risk factors are more straight forward
    # First set up lists of new vars and their equivalent code in mfr
    mrf_name <- list("hypertension_eclampsia", "preterm_births", "poor_preg_outcomes",
                      "vaginal_bleeding", "fertility_treatment", "assisted_reproduction",
                      "previous_cesarean", "group_b_strep")
    mrf_num <- list("02", "03", "04", "05", "06", "06", "07", "08")
    
    # Write a function to recode new var
    mrf_f <- function(df, name, num) {
      df <- df %>%
        mutate(!!name := case_when(
          (mrf1 == num | mrf2 == num | mrf3 == num | mrf4 == num | mrf5 == num | mrf6 == num) ~ "Y",
          mrf1 == "09" ~ "N",
          mrf1 != num & mrf1 != "99" & 
            (mrf2 != num | is.na(mrf2)) & (mrf3 != num | is.na(mrf3)) &
            (mrf4 != num | is.na(mrf4)) & (mrf5 != num | is.na(mrf5)) & 
            (mrf6 != num | is.na(mrf6)) ~ "N",
          mrf1 == "09" ~ "N",
          TRUE ~ "U"
        )) %>%
        # Just keep the new column and the cert number to join on
        select(birth_cert_encrypt, !!name)
    }
    
    # Run function and collapse list of results into a single df
    mrf_out <- purrr::map2(mrf_name, mrf_num, 
                         ~ mrf_f(df = bir_2013_2016, name = .x, num = .y)) %>%
      purrr::reduce(left_join, by = "birth_cert_encrypt")
    
    # Bind new columns to existing data frame
    bir_2013_2016 <- left_join(bir_2013_2016, mrf_out, by = "birth_cert_encrypt")
    
    # Clean up mfr objects
    rm(mfr_name, mfr_num, mfr_f, mfr_out)
    
    
    #### Maternal infections ####
    
    
    
    
    #### Obstetric procedures ####
    bir_2013_2016 <- bir_2013_2016 %>% 
      mutate(
        
        
        prepreg_diabetes = case_when(
          mrf1 == "01" & diabetes == "E" ~ "Y",
          mrf1 == "01" & diabetes == "U" ~ "U",
          mrf1 == "01" & diabetes == "G" ~ "N",
          mrf1 != "01" & mrf1 != "99" ~ "N",
          TRUE ~ "U"
        ),
        gestational_diabetes = case_when(
          mrf1 == "01" & diabetes == "G" ~ "Y",
          mrf1 == "01" & diabetes == "U" ~ "U",
          mrf1 == "01" & diabetes == "E" ~ "N",
          mrf1 != "01" & mrf1 != "99" ~ "N",
          TRUE ~ "U"
        )
      )

  
  
  
  # birplmom and birpldad need to be converted to FIPS codes and renamed to mother/father_birthplace_state_fips
  

  # Look through codes that are split out now

  # - obproc1-4
  # - cephaflg
  # - abcond1-6
  # - malf1-7
  # - downsflg
  # - chromflg
  # - labchar1-7
  # - minfect1-7
  # - mmorbid1-5
  # - labons1-3
  
  
  # Rename some fields that don't exist in the new system
  # - nchsnew
  # - geozip
  
  
  
  
  
  
}