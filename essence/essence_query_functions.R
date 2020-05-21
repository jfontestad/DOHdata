## ---------------------------
##
## Script name: Useful functions for querying ESSENCE/RHINO
##
## Purpose of script: Act as a central repository of handy scripts for querying ESSENCE
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2020-04-13
## Email: alastair.matheson@kingcounty.gov
##
## ---------------------------
##
## Notes: Tries not to assume key packages are already loaded in the script that 
##        calls these functions:
##          - tidyverse, glue, httr, keyring
##
## ---------------------------


### Use this query to find demographics specific to this person
# Can also pull in DX codes to look for chronic conditions over the time period (dx = T)
#    (used to find people with chronic conditions)
# Look for one PID (bulk = F) or many (bulk = T)
# If using bulk, can set the size of the group to search at the same time (group_size)
person_query <- function(pid = NULL, sdate = "2019-01-01", edate = today() - 1,
                         dx = F, bulk = F, group_size = 100) {
  
  # Check how many IDs were provided
  if (length(pid) > 1 & bulk == F) {
    pid <- pid[1]
    warning("Multiple IDs were provided but only the first was used. \n
            Use bulk = T to search for multiple IDs")
  }
  
  # Ensure that group_size is an integer
  group_size <- round(group_size)
  
  # Format dates properly
  # Throw an error if input not in expected format
  if (is.na(as.Date(sdate, format = "%Y-%m-%d"))) {
    stop("sdate must be %Y-%m-%d format")
  }
  if (is.na(as.Date(edate, format = "%Y-%m-%d"))) {
    stop("sdate must be %Y-%m-%d format")
  }
  
  start_date <- format(as.Date(sdate, "%Y-%m-%d"), "%d%b%Y")
  end_date <- format(as.Date(edate, "%Y-%m-%d"), "%d%b%Y")
  
  
  if (dx == T) {
    dx_fields <- paste0("&field=dischargeDiagnosis&field=Diagnosis_Combo&field=CCDDCategory_flat", 
                        "&field=HasBeenE&field=HasBeenI")
  } else {
    dx_fields <- ""
  }
  
  # Set up fields common to both types of query
  fields <- paste0(# Add in dates and geographies
    "startDate=", start_date, "&endDate=", end_date,
    "&geography=wa&geographySystem=hospitalstate", 
    # Add in a few other fields including userID
    "&datasource=va_hosp&medicalGroupingSystem=essencesyndromes&userId=3544",
    "&aqtTarget=DataDetails", 
    # Add in percent param, frequency, and detector
    "&percentParam=noPercent&timeResolution=daily&detector=nodetectordetector",
    # Add in demographic fields
    "&field=PID&field=Date&field=Age&field=Birth_Date_Time&field=Sex&field=Zipcode",
    "&field=Race_flat&field=Ethnicity_flat&field=Height&field=Height_Units",
    "&field=Weight&field=Weight_Units&field=Body_Mass_Index&field=Smoking_Status_Code",
    # Add in DX fields if desired
    dx_fields)
  
  
  if (bulk == F) {
    url <- paste0("https://essence.syndromicsurveillance.org/nssp_essence/api/dataDetails?", 
                  fields,
                  # Add in patient ID
                  "&cBiosenseID=%5E", pid)
    
    data_load <- jsonlite::fromJSON(httr::content(
      httr::GET(url, httr::authenticate(keyring::key_list("essence")[1, 2], 
                                        keyring::key_get("essence", keyring::key_list("essence")[1, 2]))), as = "text"))
    
    df <- data_load$dataDetails
  } else if (bulk == T) {
    # Make sure IDs are unique
    pid <- unique(pid)
    
    # Split up queries into smaller batches to avoid overloading the system
    num_ids <- length(pid)
    input_split <- split(pid, rep(1:ceiling(num_ids/group_size), each = group_size, length.out = num_ids))
    
    # Run each group through the query
    df <- bind_rows(lapply(seq_along(input_split), function(x) {
      message(paste0("Working on group ", x, " of ", length(input_split)))
      ids <- paste0("%5E", input_split[[x]], collapse = ",")
      
      url <- paste0("https://essence.syndromicsurveillance.org/nssp_essence/api/dataDetails?", 
                    fields,
                    # Add in patient ID
                    "&cBiosenseID=", ids)
      
      data_load <- jsonlite::fromJSON(httr::content(
        httr::GET(url, httr::authenticate(keyring::key_list("essence")[1, 2], 
                                          keyring::key_get("essence", keyring::key_list("essence")[1, 2]))), as = "text"))
      
      output <- data_load$dataDetails
      output
    }))
  }
  
  df <- df %>% arrange(PID, Date)
  
  return(df)
}


### Use this query to look up events using C_Biosense_ID
event_query <- function(event_id = NULL, bulk = F, group_size = 5000) {
  
 
  # Check how many IDs were provided
  if (length(event_id) > 1 & bulk == F) {
    event_id <- event_id[1]
    warning("Multiple IDs were provided but only the first was used. \n
            Use bulk = T to search for multiple IDs")
  }
  
  # Set up fields common to both types of query
  fields <- paste0(# Add in rowFields
    "&field=C_BioSense_ID&field=PID&field=Date&field=Age&field=AgeGroup&field=Birth_Date_Time&field=Sex&field=Zipcode",
    "&field=Race_flat&field=Ethnicity_flat&field=Height&field=Height_Units",
    "&field=Weight&field=Weight_Units&field=Body_Mass_Index&field=Smoking_Status_Code",
    "&field=PregnancyStatus",
    # Add in clinical fields
    "&field=Facility&field=Facility_Type_Description",
    "&field=HasBeenE&field=HasBeenI&field=HasBeenO&field=AdmissionTypeCategory&field=C_Patient_Class&field=PatientClassList",
    "&field=TriageNotesParsed",
    "&field=Admit_Reason_Combo&field=Diagnosis_Combo&field=Procedure_Combo&field=Medication_Combo",
    "&field=CCDDCategory_flat",
    "&field=Onset_Date&field=Initial_Temp_Calc&field=HighestTemp_Calc&field=Initial_Pulse_Oximetry_Calc",
    "&field=Systolic_Blood_Pressure&field=Diastolic_Blood_Pressure&field=Systolic_Diastolic_Blood_Pressure",
    "&field=Admit_Date_Time&field=Discharge_Date_Time&field=DischargeDisposition&field=DispositionCategory&field=MinutesFromVisitToDischarge",
    "&field=C_Death&field=C_Death_Source&field=Death_Date_Time")
  
  # Set up a common URL and call
  url_call <- function(sdate_inner, edate_inner, id_inner) {
    url <- paste0("https://essence.syndromicsurveillance.org/nssp_essence/api/dataDetails?",
                  # Add in dates and geographies
                  "startDate=", sdate_inner, "&endDate=", edate_inner,
                  "&geography=wa&geographySystem=hospitalstate",
                  # Add in a few other fields including userID
                  "&datasource=va_hosp&medicalGroupingSystem=essencesyndromes&userId=3544",
                  "&aqtTarget=DataDetails",
                  # Add in percent param, frequency, and detector
                  "&percentParam=noPercent&timeResolution=daily&detector=nodetectordetector",
                  fields,
                  # Add in patient ID
                  "&cBiosenseID=", glue_collapse(id_inner, sep = ",or,"))
    
    data_load <- jsonlite::fromJSON(httr::content(
      httr::GET(url, httr::authenticate(keyring::key_list("essence")[1, 2],
                                        keyring::key_get("essence", keyring::key_list("essence")[1, 2]))),
      as = "text"))
    
    data_load
  }
  
  if (bulk == F) {
    # Pull out start and end dates from event ID to make query faster
    # However, BiosenseID != Date so need to add time
    sdate <- format(as.Date(str_sub(event_id, 1, 10), format = "%Y.%m.%d") - 14, "%d%b%Y")
    edate <- format(as.Date(str_sub(event_id, 1, 10), format = "%Y.%m.%d") + 14, "%d%b%Y") 

    reply <- url_call(sdate_inner = sdate, edate_inner = edate, id_inner = event_id)
    
    # Keep track of input ID
    output <- data.frame(rhino_id = event_id, stringsAsFactors = F)
    output <- bind_cols(output, reply$dataDetails)

  } else if (bulk == T) {
    ### Break up the query into smaller date ranges to ease load on server
    ids <- data.frame(rhino_id = event_id, 
                      rhino_date = as.Date(str_sub(event_id, 1, 10), format = "%Y.%m.%d"),
                      stringsAsFactors = F)
    
    # Check all were dates
    if (is.na(min(ids$rhino_date)) | is.na(max(ids$rhino_date))) {
      stop("Something went wrong extracting dates to use. Check IDs")
    }
    
    # Build a list of 1-week dates that covers the range
    min_dates <- seq(min(ids$rhino_date), max(ids$rhino_date), by = '1 week')
    # Add in final if needed since sequence may miss it
    if (max(min_dates) < max(ids$rhino_date)) {min_dates <- c(min_dates, max(min_dates) + weeks(1))}
    
    date_range <- interval(start = min_dates, end = min_dates + days(6))
    
    # Join to each date and restrict to desired period
    dates <- data.frame(rhino_date = rep(unique(ids$rhino_date), length(date_range)),
                        int = rep(date_range, each = length(unique(ids$rhino_date))),
                        int_num = rep(seq(1, length(date_range)), each = length(unique(ids$rhino_date))),
                        stringsAsFactors = F) %>%
      arrange(rhino_date, int) %>%
      filter(rhino_date %within% int) %>%
      dplyr::mutate(int_start = int_start(int), int_end = int_end(int)) %>%
      dplyr::select(-int)
    
    # Join back to main data
    ids <- left_join(ids, dates, by = "rhino_date")
    
    # Figure out how many groups we need
    group_ids <- paste0("group", sort(unique(ids$int_num)))
    message("Dividing into ", length(group_ids), " groups")
    
    # Run query over each group
    output <- lapply(seq_along(group_ids), function(x) {
      message("Working on group ", x, " of ", length(group_ids), " (group name: ", group_ids[x], ")")
      
      input <- ids %>% filter(int_num == sort(unique(ids$int_num))[x])
      sdate <- format(min(input$int_start) - days(7), "%d%b%Y")
      edate <- format(min(input$int_end) + days(7), "%d%b%Y")
      
      message("Start date of interval: ", sdate)
      message("End date of interval: ", edate)
      
      # Check to see if there are a massive number of IDs in this time period
      # If so, split up into smaller batches to avoid overloading the system
      num_ids <- nrow(input)
      
      if (num_ids > group_size) {
        input_split <- split(input, rep(1:ceiling(num_ids/group_size), each = group_size, length.out = num_ids))
        
        message("There are ", length(unique(input$rhino_id)), " IDs to check. ",
                "Because of the large number of IDs, the group will be subdivided into lots of ", 
                group_size, ".")
        
        # Run each group through the query
        reply_details <- bind_rows(lapply(seq_along(input_split), function(x) {
          message(paste0("Working on subgroup ", x, " of ", length(input_split)))
          
          reply <- url_call(sdate_inner = sdate, edate_inner = edate, 
                            id_inner = input_split[[x]][["rhino_id"]])
          
          
          reply_details_inner <- reply$dataDetails
          reply_details_inner
        }))
        
      } else {
        message("There are ", length(unique(input$rhino_id)), " IDs to check")
        
        reply <- url_call(sdate_inner = sdate, edate_inner = edate, id_inner = input$rhino_id)
        reply_details <- reply$dataDetails
      }
      
      # Process bulk data regardless of whether it was subdivided
      # Keep track of input ID
      df <- input %>% dplyr::select(rhino_id, rhino_date)
      
      if (!is.null(nrow(reply_details))) {
        df <- left_join(df, reply_details, by = c("rhino_id" = "C_BioSense_ID"))
        message("Matched ", as.integer(df %>% filter(!is.na(PID)) %>% summarise(count = n())), " IDs \n ")
      } else {
        message("Matched 0 IDs \n ")
      }
      
      df
    })
    
    # Label groups
    names(output) <- group_ids
  }
  
  return(output)
}



### Use this query to return syndrome counts/percentages plus alerts
syndrome_alert_query <- function(user_id = 520, 
                                 sdate = "2019-09-29", edate = today() - 1,
                                 frequency = c("weekly", "daily"), 
                                 syndrome = c("all", "ili", "cli_new", "cli_old", "cli", 
                                              "pneumonia", "influenza"),
                                 ed = F, inpatient = F, ed_uc = F,
                                 age = c("all", "00-04", "05-17", "18-44", "45-64", "65-1000", "unknown"),
                                 race = c("all", "aian", "asian", "black", "nhpi", "other", "white", "unknown"),
                                 ethnicity = c("all", "latino", "non-latino", "unknown"),
                                 hospital = F, 
                                 zip = NULL,
                                 value = c("percent", "count")) {
  
  frequency <- match.arg(frequency)
  syndrome <- match.arg(syndrome)
  age <- match.arg(age, several.ok = T)
  race <- match.arg(race, several.ok = T)
  ethnicity <- match.arg(ethnicity, several.ok = T)
  value <- match.arg(value)
  
  # Format dates properly
  # Throw an error if input not in expected format
  if (is.na(as.Date(sdate, format = "%Y-%m-%d"))) {
    stop("sdate must be %Y-%m-%d format")
  }
  if (is.na(as.Date(edate, format = "%Y-%m-%d"))) {
    stop("sdate must be %Y-%m-%d format")
  }
  if (sdate > edate) {
    stop("sdate must be before edate")
  }
  
  start_date <- format(as.Date(sdate, "%Y-%m-%d"), "%d%b%Y")
  end_date <- format(as.Date(edate, "%Y-%m-%d"), "%d%b%Y")
  
  
  # Restrict to only one filter type
  # Need to check we never want more than one
  if (ed + inpatient + ed_uc == 0 | ed + inpatient + ed_uc > 1) {
    stop("Select only one of 'ED', 'inpatient', and 'ed_uc'")
  }
  
  if (syndrome == "all") {
    category <- ""
    query <- "all"
    syndrome_text <- "all"
  } else if (syndrome == "ili") {
    category <- "&ccddCategory=ili%20ccdd%20v1"
    query <- "ili"
    syndrome_text <- "ILI"
  } else if (syndrome == "cli_old") {
    category <- "&ccddCategory=fever%20and%20cough-sob-diffbr%20v1"
    query <- "cli_old"
    syndrome_text <- "CLI - old definition"
  } else if (syndrome %in% c("cli_new", "cli")) {
    category <- "&ccddCategory=cli%20cc%20with%20cli%20dd%20and%20coronavirus%20dd%20v1"
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
  
  # Percent or not
  if (value == "percent") {
    percent <- "&percentParam=ccddCategory"
  } else if (value == "count") {
    percent <- "&percentParam=noPercent"
  }
  
  # Catch all for detector types
  if (syndrome %in% c("influenza")) {
    detector <- "&detector=probrepswitch"
  } else if (syndrome %in% c("all", "ili", "pneumonia", "cli_old", "cli_new", "cli")) {
    detector <- "&detector=c2"
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
  if ("all" %in% age) {
    age_grp <- ""
    age_text <- "all age"
  } else {
    age_grp <- paste0("&age=", age, collapse = "")
    if (length(age) > 1) {
      age_text <- ifelse(max(c("all", "00-04", "05-17", "unknown") %in% age) == 0 &
                           min(c("18-44", "45-64", "65-1000") %in% age) == 1,
                         "all adult",
                         paste0(age, collapse = ", "))
    } else {
      age_text <- case_when(age == "all" ~ "all age",
                            age == "00-04" ~ "0-4",
                            age == "05-17" ~ "5-17",
                            age == "18-44" ~ "18-44",
                            age == "45-64" ~ "45-64",
                            age == "65-1000" ~ "65+",
                            age == "unknown" ~ "Unk")
    }
  }
  
  # Catch all for race
  if ("all" %in% race) {
    race_grp <- ""
    race_text <- "all"
  } else {
    race_text <- paste(race, sep = ", ")
    
    if ("aian" %in% race) {race_grp_aian <- "&race=1002-5"} else {race_grp_aian <- ""}
    if ("asian" %in% race) {race_grp_asian <- "&race=2028-9"} else {race_grp_asian <- ""}
    if ("black" %in% race) {race_grp_black <- "&race=2054-5"} else {race_grp_black <- ""}
    if ("nhpi" %in% race) {race_grp_nhpi <- "&race=2076-8"} else {race_grp_nhpi <- ""}
    if ("other" %in% race) {race_grp_other <- "&race=2131-1"} else {race_grp_other <- ""}
    if ("white" %in% race) {race_grp_white <- "&race=2106-3"} else {race_grp_white <- ""}
    if ("unknown" %in% race) {race_grp_unk<- "&race=phc1175&race=unk&race=nr"} else {race_grp_unk <- ""}
    
    race_grp <- paste0(race_grp_aian, race_grp_asian, race_grp_black, race_grp_nhpi,
                       race_grp_other, race_grp_white, race_grp_unk)
  }
  
  # Catch all for ethnicity
  if ("all" %in% ethnicity) {
    eth_grp <- ""
    eth_text <- "all"
  } else {
    eth_text <- paste(ethnicity, sep = ", ")
    
    if ("latino" %in% ethnicity) {eth_grp_latino <- "&ethnicity=2135-2"} else {eth_grp_latino <- ""}
    if ("non-latino" %in% ethnicity) {eth_grp_nonlat <- "&ethnicity=2186-5"} else {eth_grp_nonlat <- ""}
    if ("unknown" %in% ethnicity) {eth_grp_unk <- "&ethnicity=unk&ethnicity=nr"} else {eth_grp_unk <- ""}
    
    eth_grp <- paste0(eth_grp_latino, eth_grp_nonlat, eth_grp_unk)
  }
  
  
  # Set things up for hospitals
  if (hospital == T) {
    geog_system <- "hospital"
    geogs <- c("1255", "1297", "1298", "1247", "27090", "1252", "1269", "30509", 
               "1272", "1277", "30529", "1294", "1302", "1303", "1304", "1305", 
               "1307", "1313", "1315", "1316")
    fields <- "&multiStratVal=geography&graphOptions=multipleSmall"
  } else {
    geog_system <- "hospitalregion"
    geogs <- c("wa_king")
    fields <- "&multiStratVal=hospitalGrouping&graphOptions=facetGrid"
  }
  
  
  # Set things up for ZIP codes
  # Note that this will override any other choices like hospital
  if (!is.null(zip)) {
    geog_system <- "zipcode"
    geogs <- paste(zip, collapse = ",")
    data_source <- "&datasource=va_er"
    zip_text <- paste(zip, collapse = ", ")
  } else {
    data_source <- "&datasource=va_hosp"
    zip_text <- "all"
  }
  
  
  # Run query
  output <- dplyr::bind_rows(lapply(geogs, function(x) {
    url <- paste0("https://essence.syndromicsurveillance.org/nssp_essence/api/timeSeries?", 
                  # Add in dates and geographies
                  "startDate=", start_date, "&endDate=", end_date,
                  "&geographySystem=", geog_system, "&geography=", x,
                  # Add in a few other fields including userID
                  data_source, "&medicalGroupingSystem=essencesyndromes&userId=", user_id, 
                  "&aqtTarget=TimeSeries", 
                  # Add in percent param, types of visits, frequency, detector
                  percent, visit_type, "&timeResolution=", frequency, detector,
                  # Add in other config fields
                  config,
                  # Add in syndrome, filter, age, race, and ethnicity
                  category, filter, age_grp, race_grp, eth_grp,
                  # Add in hospital grouping
                  "&stratVal=&graphOnly=true&numSeries=0", fields,
                  "&seriesPerYear=false&nonZeroComposite=false&removeZeroSeries=true&startMonth=January"
    )
    
    data_load <- jsonlite::fromJSON(httr::content(
      httr::GET(url, httr::authenticate(keyring::key_list("essence")[1, 2], 
                            keyring::key_get("essence", keyring::key_list("essence")[1, 2]))), as = "text"))
    
    df <- data_load$timeSeriesData
    
    if (value == "percent") {
      df <- df %>% dplyr::rename(pct = count, expected_pct = expected, levels_pct = levels, 
                          colorID_pct = colorID, color_pct = color) %>%
        dplyr::mutate_at(vars(expected_pct, levels_pct), list(~ as.numeric(.)))
    } else if (value == "count") {
      df <- df %>% dplyr::rename(cnt = count, expected_cnt = expected, levels_cnt = levels, 
                          colorID_cnt = colorID, color_cnt = color) %>%
        dplyr::mutate_at(vars(expected_cnt, levels_cnt), list(~ as.numeric(.)))
    }
    
    # Add in specifics for this data run
    df <- df %>%
      dplyr::mutate(date = lubridate::ymd(date),
             age = age_text,
             race = race_text,
             ethnicity = eth_text,
             zip = zip_text,
             setting = setting,
             query = query,
             syndrome = syndrome_text) %>%
      # Remove specifics of query to save space
      # Also remove percent/count because it is calculated in the main code
      dplyr::select(-details, -altText)
    
    if (hospital == F) {
      df <- df %>% dplyr::mutate(hospital = "all")
    } else {
      df <- df %>% dplyr::mutate(hospital = case_when(
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
        x == 30529 ~ "Seattle Children's Hospital",
        x == 1294 ~ "Snoqualmie Valley Hospital",
        x == 1302 ~ "Swedish Medical Center - Ballard",
        x == 1303 ~ "Swedish Medical Center - Cherry Hill",
        x == 1304 ~ "Swedish Medical Center - First Hill",
        x == 1305 ~ "Swedish Medical Center - Issaquah",
        x == 1307 ~ "Swedish Medical Center - Redmond",
        x == 1313 ~ "University of Washington Medical Center",
        x == 1315 ~ "Valley Medical Center",
        x == 1316 ~ "Virginia Mason Medical Center"
      ))
    }
    
    df
  }))
  
  return(output)
}


### Use this query to get record-level details for each syndrome or COVID testing/dx
# kc_patients = T will restrict to patients with a KC ZIP who attended a facility in
#   King, Snohomish, or Pierce Counties
# kc_patients = F will restrict to all patients who attended a KC facility
syndrome_person_level_query <- function(user_id = 2769, 
                                        sdate = "2019-09-29", edate = today() - 1,
                                        syndrome = c("all", "ili", "cli_new", 
                                                     "cli_old", "cli", "pneumonia",
                                                     "covid_test", "covid_dx_broad",
                                                     "covid_dx_narrow"),
                                        ed = F, inpatient = F,
                                        kc_patients = F) {
  
  syndrome <- match.arg(syndrome)
  
  # Format dates properly
  # Throw an error if input not in expected format
  if (is.na(as.Date(sdate, format = "%Y-%m-%d"))) {
    stop("sdate must be %Y-%m-%d format")
  }
  if (is.na(as.Date(edate, format = "%Y-%m-%d"))) {
    stop("sdate must be %Y-%m-%d format")
  }
  if (sdate > edate) {
    stop("sdate must be before edate")
  }
  
  start_date <- format(as.Date(sdate, "%Y-%m-%d"), "%d%b%Y")
  end_date <- format(as.Date(edate, "%Y-%m-%d"), "%d%b%Y")
  
  
  if (syndrome == "all") {
    category <- ""
    query <- "all"
    condition <- "all"
    syndrome_text <- "all"
  } else if (syndrome == "ili") {
    category <- "&ccddCategory=ili%20ccdd%20v1"
    query <- "ili"
    condition <- "ili"
    syndrome_text <- "ILI"
  } else if (syndrome == "cli_old") {
    category <- "&ccddCategory=fever%20and%20cough-sob-diffbr%20v1"
    query <- "fevcough"
    condition <- "cli_old"
    syndrome_text <- "CLI - old definition"
  } else if (syndrome %in% c("cli_new", "cli")) {
    category <- "&ccddCategory=cli%20cc%20with%20cli%20dd%20and%20coronavirus%20dd%20v1"
    query <- "cli"
    condition <- "cli"
    syndrome_text <- "CLI"
  } else if (syndrome == "pneumonia") {
    category <- "&ccddCategory=cdc%20pneumonia%20ccdd%20v1"
    query <- "ncovpneumo"
    condition <- "pneumonia"
    syndrome_text <- "Pneumonia"
  } else if (syndrome == "covid_test") {
    category <- "&procedureCombo=%5E87635%5E,%5E86328%5E,%5E86769%5E,%5Ecovid%5E,%5Esars-cov-2%5E"
    query <- "sars-cov-2_testing"
    condition <- "tested_for_covid"
    syndrome_text <- "SARS-COV-2 testing"
  } else if (syndrome == "covid_dx_broad") {
    category <- "&ccddCategory=cdc%20coronavirus-dd%20v1"
    query <- "covid_dx_broad"
    condition <- "diagnosed_with_covid_broad"
    syndrome_text <- "Diagnosed with COVID-19 (broad definition)"
  } else if (syndrome == "covid_dx_narrow") {
    category <- "&dischargeDiagnosis=%5EU.07%5E,%5EU07%5E"
    query <- "covid_dx_narrow"
    condition <- "diagnosed_with_covid_narrow"
    syndrome_text <- "Diagnosed with COVID-19 (narrow definition)"
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

  
  # Catch all for visit types
  if (syndrome %in% c("all", "covid_test", "covid_dx_broad", "covid_dx_narrow")) {
    visit_types <- "&hospFacilityType=emergency%20care&hospFacilityType=urgent%20care&hospFacilityType=primary%20care"
  } else if (syndrome %in% c("ili", "cli_new", "cli_old", "cli", "pneumonia")) {
    visit_types <- "&hospFacilityType=emergency%20care"
  }
  
  # Geography systems
  if (kc_patients == F) {
    datasource <- "&datasource=va_hosp"
    geography <- "&geographySystem=hospitalregion"
    facility <- ""
  } else {
    datasource <- "&datasource=va_er"
    geography <- "&geographySystem=region"
    facility <- paste0("&erFacility=1239&erFacility=1246&erFacility=1283&erFacility=1301", 
                       "&erFacility=1306&erFacility=1237&erFacility=1250&erFacility=1308", 
                       "&erFacility=1295&erFacility=1296&erFacility=1299&erFacility=1247", 
                       "&erFacility=27090&erFacility=1252&erFacility=1255&erFacility=1297", 
                       "&erFacility=1298&erFacility=1269&erFacility=30509&erFacility=1272", 
                       "&erFacility=1277&erFacility=1294&erFacility=1302&erFacility=1303", 
                       "&erFacility=1304&erFacility=1305&erFacility=1307&erFacility=1313", 
                       "&erFacility=1316&erFacility=30529&erFacility=1315")
  }

  

  
  url <- paste0("https://essence.syndromicsurveillance.org/nssp_essence/api/dataDetails?", 
                # Add in dates and geographies
                "startDate=", start_date, "&endDate=", end_date,
                datasource, geography, "&geography=wa_king", 
                # Add in a few other fields including userID
                "&medicalGroupingSystem=essencesyndromes&userId=", user_id, 
                "&aqtTarget=DataDetails&refValues=true",
                # Add in percent param, types of visits, frequency, detector
                "&percentParam=noPercent", 
                "&hospFacilityType=emergency%20care&hospFacilityType=urgent%20care&hospFacilityType=primary%20care", 
                "&timeResolution=daily&detector=nodetectordetector",
                # Add in demographics
                "&field=Age&field=AgeGroup&field=Race_flat&field=Ethnicity_flat&field=Sex&field=Zipcode&field=C_Patient_County", 
                "&field=Height&field=Height_Units&field=Weight&field=Weight_Units&field=Body_Mass_Index&field=Smoking_Status_Code",
                # Add in clinical aspects
                "&field=Date&field=HasBeenE&field=HasBeenI&field=HasBeenO",
                "&field=C_BioSense_ID&field=PID&field=age&field=ChiefComplaintParsed&field=DateTime&field=FacilityName", 
                "&field=CCDD&field=CCDDCategory_flat&field=Admit_Reason_Combo&field=DischargeDiagnosis&field=Diagnosis_Combo&field=Procedure_Combo",
                "&field=TriageNotesParsed&field=hospFacilityType&field=HospitalName",
                "&field=Admit_Date_Time&field=Discharge_Date_Time&field=DischargeDisposition&field=DispositionCategory&field=MinutesFromVisitToDischarge",
                "&field=C_Death&field=C_Death_Source&field=Death_Date_Time",
                # Add in syndrome and filters
                category, filter, facility)
  
  data_load <- jsonlite::fromJSON(httr::content(
    httr::GET(url, httr::authenticate(keyring::key_list("essence")[1, 2], 
                          keyring::key_get("essence", keyring::key_list("essence")[1, 2]))), as = "text"))
  
  df <- data_load$dataDetails
  
  if (is.null(nrow(df))) {
    warning("No results returned")
  } else {
    # Add in details to the data frame
    df <- df %>%
      dplyr::mutate(condition = condition,
                    setting = setting,
                    syndrome = syndrome_text)
    
    return(df)
  }
}


### Use this query to recode output from syndrome_person_level_query runs
# Need to ensure that the fields referenced here stay in sync with those produced by
# syndrome_person_level_query
essence_recode <- function(df) {
  # Bring in reference data for joining
  recodes <- read.csv("https://raw.githubusercontent.com/PHSKC-APDE/DOHdata/master/essence/essence_recodes.csv", 
                      stringsAsFactors = F)
  
  zips <- read.csv("https://raw.githubusercontent.com/PHSKC-APDE/reference-data/master/spatial_data/zip_to_region.csv",
                   stringsAsFactors = F)
  zips <- zips %>% mutate(zip = as.character(zip),
                          cc_region = case_when(
                            cc_region == 'east' ~ "East",
                            cc_region == 'north' ~ "North",
                            cc_region == 'seattle' ~ "Seattle",
                            cc_region == 'south' ~ "South",
                            TRUE ~ cc_region))
  
  output <- df %>%
    left_join(., filter(recodes, category == "Smoking_Status_Code") %>% select(code, value_display),
              by = c("Smoking_Status_Code" = "code")) %>% rename(smoking_text = value_display) %>%
    left_join(., select(zips, zip, cc_region), by = c("ZipCode" = "zip")) %>%
    mutate_at(vars(Age, Height, Weight, HasBeenE, HasBeenI, HasBeenO), list(~ as.numeric(.))) %>%
    mutate(date = as.Date(str_sub(C_BioSense_ID, 1, 10), format = "%Y.%m.%d"),
           setting = ifelse(HasBeenI == 1, "hosp", "ed"),
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
           sex = case_when(Sex == "F" ~ "Female", Sex == "M" ~ "Male"),
           race = case_when(
             str_count(Race_flat, ";") > 2 ~ "Multiple",
             str_detect(Race_flat, "1002-5") ~ "AI/AN",
             str_detect(Race_flat, "2028-9") ~ "Asian",
             str_detect(Race_flat, "2054-5") ~ "Black",
             str_detect(Race_flat, "2076-8") ~ "NH/PI",
             str_detect(Race_flat, "2131-1") ~ "Other race",
             str_detect(Race_flat, "2106-3") ~ "White"),
           ethnicity = case_when(
             str_detect(Ethnicity_flat, "2135-2") ~ "Latino",
             str_detect(Ethnicity_flat, "2186-5") ~ "Not Latino"),
           height_m = case_when(tolower(Height_Units) == "centimeter" ~ Height / 100,
                                tolower(Height_Units) == "meter" ~ Height,
                                tolower(Height_Units) %in% c("inch", "inch [length]") ~ Height * 0.0254,
                                tolower(Height_Units) %in% c("foot", "foot [length]") ~ Height * 0.3048),
           weight_kg = case_when(tolower(Weight_Units) %in% c("kilogram", "kilogram [si mass units]") ~ Weight,
                                 tolower(Weight_Units) == "pound" ~ Weight / 2.2,
                                 tolower(Weight_Units) == "ounce" ~ Weight / 0.0283495),
           bmi = ifelse(!is.na(height_m) & !is.na(weight_kg) & Age >= 20,
                        round(weight_kg / height_m ^ 2, 3), NA),
           overweight = case_when(bmi >= 25 ~ "Overweight", bmi < 25 ~ "Not overweight"),
           obese = case_when(bmi >= 30 ~ "Obese", bmi < 30 ~ "Not obese"),
           obese_severe = case_when(bmi >= 40 ~ "Severely obese", bmi < 40 ~ "Not severely obese"),
           smoker_current = case_when(smoking_text %in% c("Current every day smoker",
                                                          "Current light tobacco smoker",
                                                          "Current some day smoker",
                                                          "Current heavy tobacco smoker") ~ "Current smoker",
                                      smoking_text %in% c("Never smoker", "Former smoker") ~ "Not current smoker"),
           smoker_general = case_when(smoking_text %in% c("Current every day smoker",
                                                          "Current light tobacco smoker",
                                                          "Current some day smoker",
                                                          "Current heavy tobacco smoker") ~ "Current smoker",
                                      smoking_text == "Former smoker" ~ "Former smoker",
                                      smoking_text  == "Never smoker" ~ "Never smoker"),
           kc_zip = ifelse(!is.na(cc_region), 1L, 0L),
           covid_test = case_when(
             str_detect(Procedure_Combo, "(87635|86328|86769)") ~ 1L,
             str_detect(tolower(Procedure_Combo), "(covid|sars-cov-2)") ~ 1L,
             TRUE ~ 0L),
           covid_dx_broad = case_when(
             str_detect(Diagnosis_Combo, "(U07\\.1|U071)") ~ 1L,
             str_detect(Diagnosis_Combo, "(J12\\.89|J1289)") ~ 1L,
             str_detect(Diagnosis_Combo, "(B97\\.29|B9729)") ~ 1L,
             TRUE ~ 0L),
           covid_dx_narrow = case_when(
             str_detect(Diagnosis_Combo, "(U07\\.1|U071)") ~ 1L,
             TRUE ~ 0L),
           cli = ifelse(str_detect(tolower(CCDDCategory_flat), "cli cc with cli dd and coronavirus dd v1"), 1L, 0L),
           pneumo = ifelse(str_detect(tolower(CCDDCategory_flat), "cdc pneumonia ccdd v1"), 1L, 0L),
           cli_pneumo = ifelse(cli == 1 | pneumo == 1, 1L, 0L),
           ili = ifelse(str_detect(tolower(CCDDCategory_flat), "ili ccdd v1"), 1L, 0L)
    )
  
  return(output)
}
