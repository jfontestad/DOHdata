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
    dx_fields <- "&field=dischargeDiagnosis&field=Diagnosis_Combo&field=HasBeenE&field=HasBeenI"
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
event_query <- function(event_id = NULL, bulk = F) {
  
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
    "&field=HasBeenE&field=HasBeenI&field=AdmissionTypeCategory&field=C_Patient_Class&field=PatientClassList",
    "&field=TriageNotesParsed",
    "&field=Admit_Reason_Combo&field=Diagnosis_Combo&field=Procedure_Combo&field=Medication_Combo",
    "&field=CCDDCategory_flat",
    "&field=Onset_Date&field=Initial_Temp_Calc&field=HighestTemp_Calc&field=Initial_Pulse_Oximetry_Calc",
    "&field=Systolic_Blood_Pressure&field=Diastolic_Blood_Pressure&field=Systolic_Diastolic_Blood_Pressure",
    "&field=Admit_Date_Time&field=Discharge_Date_Time&field=DischargeDisposition&field=DispositionCategory&field=MinutesFromVisitToDischarge",
    "&field=C_Death&field=C_Death_Source&field=Death_Date_Time")
  
  
  if (bulk == F) {
    # Pull out start and end dates from event ID to make query faster
    # However, BiosenseID != Date so need to add time
    sdate <- format(as.Date(str_sub(event_id, 1, 10), format = "%Y.%m.%d") - 14, "%d%b%Y")
    edate <- format(as.Date(str_sub(event_id, 1, 10), format = "%Y.%m.%d") + 14, "%d%b%Y") 
    
    url <- paste0("https://essence.syndromicsurveillance.org/nssp_essence/api/dataDetails?", 
                  # Add in dates and geographies
                  "startDate=", sdate, "&endDate=", edate,
                  "&geography=wa&geographySystem=hospitalstate", 
                  # Add in a few other fields including userID
                  "&datasource=va_hosp&medicalGroupingSystem=essencesyndromes&userId=3544",
                  "&aqtTarget=DataDetails", 
                  # Add in percent param, frequency, and detector
                  "&percentParam=noPercent&timeResolution=daily&detector=nodetectordetector",
                  fields,
                  # Add in patient ID
                  "&cBiosenseID=", event_id)
    print(url)
    data_load <- jsonlite::fromJSON(httr::content(
      httr::GET(url, httr::authenticate(keyring::key_list("essence")[1, 2], 
                                        keyring::key_get("essence", keyring::key_list("essence")[1, 2]))), 
      as = "text")) #, encoding = "UTF-8"))
    
    # Keep track of input ID
    output <- data.frame(rhino_id = event_id, stringsAsFactors = F)
    output <- bind_cols(output, data_load$dataDetails)

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
      message("There are ", length(unique(input$rhino_id)), " IDs to check")
      
      url <- paste0("https://essence.syndromicsurveillance.org/nssp_essence/api/dataDetails?",
                    # Add in dates and geographies
                    "startDate=", sdate, "&endDate=", edate,
                    "&geography=wa&geographySystem=hospitalstate",
                    # Add in a few other fields including userID
                    "&datasource=va_hosp&medicalGroupingSystem=essencesyndromes&userId=3544",
                    "&aqtTarget=DataDetails",
                    # Add in percent param, frequency, and detector
                    "&percentParam=noPercent&timeResolution=daily&detector=nodetectordetector",
                    fields,
                    # Add in patient ID
                    "&cBiosenseID=", glue_collapse(input$rhino_id, sep = ",or,"))
      
      data_load <- jsonlite::fromJSON(httr::content(
        httr::GET(url, httr::authenticate(keyring::key_list("essence")[1, 2],
                                          keyring::key_get("essence", keyring::key_list("essence")[1, 2]))),
        as = "text"))
      
      # Keep track of input ID
      df <- input %>% dplyr::select(rhino_id, rhino_date)
      
      if (!is.null(nrow(data_load$dataDetails))) {
        df <- left_join(df, data_load$dataDetails, by = c("rhino_id" = "C_BioSense_ID"))
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
                                 age = c("None", "00-04", "05-17", "18-44", "45-64", "65-1000", "unknown"),
                                 race = c("all", "aian", "asian", "black", "nhpi", "other", "white", "unknown"),
                                 ethnicity = c("all", "latino", "non-latino", "unknown"),
                                 hospital = F, value = c("percent", "count")) {
  
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
  
  # Catch all for detector types
  if (syndrome %in% c("influenza") & value == "percent") {
    detector <- "&detector=probrepswitch"
    percent <- "&percentParam=ccddCategory"
  } else if (syndrome %in% c("influenza") & value == "count") {
    detector <- "&detector=probrepswitch"
    percent <- "&percentParam=noPercent"
  } else if (syndrome %in% c("all", "ili", "pneumonia", "cli_old", "cli_new", "cli") & value == "percent") {
    detector <- "&detector=c2"
    percent <- "&percentParam=ccddCategory"
  } else if (syndrome %in% c("all", "ili", "pneumonia", "cli_old", "cli_new", "cli") & value == "count") {
    detector <- "&detector=probregv2"
    percent <- "&percentParam=noPercent"
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
  if ("None" %in% age) {
    age_grp <- ""
    age_text <- "all age"
  } else {
    age_grp <- paste0("&age=", age, collapse = "")
    if (length(age) > 1) {
      age_text <- ifelse(max(c("None", "00-04", "05-17", "unknown") %in% age) == 0 &
                           min(c("18-44", "45-64", "65-1000") %in% age) == 1,
                         "all adult",
                         paste0(age, collapse = ", "))
    } else {
      age_text <- case_when(age == "None" ~ "all age",
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
    if ("unknown" %in% race) {race_grp_unk<- "&raace=phc1175&race=unk&race=nr"} else {race_grp_unk <- ""}
    
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
    if ("non-latino" %in% ethnicity) {eth_grp_nonlat <- "&ethnicity=2028-9"} else {eth_grp_nonlat <- ""}
    if ("unknown" %in% ethnicity) {eth_grp_unk <- "&ethnicity=unk&ethnicity=nr"} else {eth_grp_unk <- ""}
    
    eth_grp <- paste0(eth_grp_latino, eth_grp_nonlat, eth_grp_unk)
  }
  
 
  
  # Set things up for hospitals
  if (hospital == T) {
    geog_system <- "hospital"
    geogs <- c("1255", "1297", "1298", "1247", "27090", "1252", "1269", "30509", 
               "1272", "1277", "30529", "1294", "1302", "1303", "1304", "1305", 
               "1307", "1313", "1316")
    fields <- "&multiStratVal=geography&graphOptions=multipleSmall"
  } else {
    geog_system <- "hospitalregion"
    geogs <- c("wa_king")
    fields <- "&multiStratVal=hospitalGrouping&graphOptions=facetGrid"
  }
  
  
  output <- dplyr::bind_rows(lapply(geogs, function(x) {
    url <- paste0("https://essence.syndromicsurveillance.org/nssp_essence/api/timeSeries?", 
                  # Add in dates and geographies
                  "startDate=", start_date, "&endDate=", end_date,
                  "&geographySystem=", geog_system, "&geography=", x,
                  # Add in a few other fields including userID
                  "&datasource=va_hosp&medicalGroupingSystem=essencesyndromes&userId=", user_id, 
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
        x == 1316 ~ "Virginia Mason Medical Center"
      ))
    }
    
    df
  }))
  
  return(output)
}


### Use this querty to get record-level details for each syndrome
syndrome_person_level_query <- function(user_id = 2769, 
                                        frequency = c("weekly", "daily"), 
                                        sdate = "2019-09-29", edate = today() - 1,
                                        syndrome = c("none", "ili", "cli_new", 
                                                     "cli_old", "cli", "pneumonia"),
                                        ed = F, inpatient = F) {
  
  frequency <- match.arg(frequency)
  syndrome <- match.arg(syndrome)
  
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
  
  
  if (syndrome == "ili") {
    category <- "&ccddCategory=ili%20ccdd%20v1"
    query <- "ili"
    condition <- "ili"
  } else if (syndrome == "cli_old") {
    category <- "&ccddCategory=fever%20and%20cough-sob-diffbr%20v1"
    query <- "fevcough"
    condition <- "cli_old"
  } else if (syndrome %in% c("cli_new", "cli")) {
    category <- "&ccddCategory=cli%20cc%20with%20cli%20dd%20and%20coronavirus%20dd%20v1"
    query <- "cli"
    condition <- "cli"
  } else if (syndrome == "pneumonia") {
    category <- paste0("&dischargeDiagnosisApplyTo=subsyndromeFreeText&dischargeDiagnosis=", 
                       "%5Epneumonia%5E,or,%5E%5B;/%20%5DJ12.%5B89%5D%5E,or,%5E%5B;/%20%5DJ12%5B89%5D%5E,or,",
                       "%5E%5B;/%20%5DJ168%5E,or,%5E%5B;/%20%5DJ16.8%5E,or,%5E%5B;/%20%5DJ1%5B78%5D%5E,or,",
                       "%5E%5B;/%20%5DJ851%5E,or,%5E%5B;/%20%5DJ85.1%5E,or,%5E%5B;/%20%5DJ15.9%5E,or,",
                       "%5E%5B;/%20%5DJ159%5E,or,%5E%5B;/%20%5D233604007%5E,or,%5E%5B;/%20%5D385093006%5E,or,",
                       "%5E%5B;/%20%5D301000005%5E,or,%5E%5B;/%20%5D301001009%5E,or,%5E%5B;/%20%5D233606009%5E,or,",
                       "%5E%5B;/%20%5D407671000%5E,or,%5E%5B;/%20%5D301003007%5E,or,%5E%5B;/%20%5D75570004%5E,or,",
                       "%5E%5B;/%20%5D300999006%5E,or,%5E%5B;/%20%5D301002002%5E,or,%5E%5B;/%20%5D396285007%5E,or,",
                       "%5E%5B;/%20%5D426696003%5E,or,%5E%5B;/%20%5D312342009%5E,or,%5E%5B;/%20%5D53084003%5E,or,",
                       "%5E%5B;/%20%5D278516003%5E,or,%5E%5B;/%20%5D64667001%5E,or,%5E%5B;/%20%5D236302005%5E,or,",
                       "%5E%5B;/%20%5D196112005%5E,or,%5E%5B;/%20%5D471272001%5E,or,%5E%5B;/%20%5D7063008%5E,or,",
                       "%5E%5B;/%20%5D700250006%5E,or,%5E%5B;/%20%5D425996009%5E,or,%5E%5B;/%20%5D123590007%5E,or,",
                       "%5E%5B;/%20%5D44274007%5E,or,%5E%5B;/%20%5D68409003%5E,or,%5E%5B;/%20%5D55679008%5E,or,",
                       "%5E%5B;/%20%5D441590008%5E,or,%5E%5B;/%20%5D57702005%5E")
    query <- "ncovpneumo"
    condition <- "pneumonia"
  } else if (syndrome == "none") {
    category <- ""
    condition <- ""
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
  
  # Catch all for percentParam types
  if (syndrome %in% c("ili", "cli_new", "cli_old", "cli", "none")) {
    percent <- "&percentParam=ccddCategory"
  } else if (syndrome %in% c("pneumonia")) {
    percent <- "&percentParam=dischargeDiagnosis"
  }
  
  # Catch all for visit types
  if (syndrome %in% c("none")) {
    visit_types <- "&hospFacilityType=emergency%20care&hospFacilityType=urgent%20care&hospFacilityType=primary%20care"
  } else if (syndrome %in% c("ili", "cli_new", "cli_old", "cli", "pneumonia")) {
    visit_types <- "&hospFacilityType=emergency%20care"
  }
  
  # Catch all for detector types
  if (syndrome %in% c("cli_new", "cli_old", "cli", "none")) {
    detector <- "&detector=nodetectordetector"
  } else if (syndrome %in% c("ili", "pneumonia")) {
    detector <- "&detector=c2"
  }
  
  url <- paste0("https://essence.syndromicsurveillance.org/nssp_essence/api/dataDetails?", 
                # Add in dates and geographies
                "startDate=", start_date, "&endDate=", end_date,
                "&geography=wa_king&geographySystem=hospitalregion", 
                # Add in a few other fields including userID
                "&datasource=va_hosp&medicalGroupingSystem=essencesyndromes&userId=", user_id, 
                "&aqtTarget=DataDetails&refValues=true", 
                # Add in percent param, types of visits, frequency, detector
                percent, visit_types, "&timeResolution=", frequency, detector,
                # Add in rowFields
                "&field=age&field=ChiefComplaintParsed&field=DateTime&field=FacilityName&field=Zipcode",
                "&field=Sex&field=Date&field=HospitalName&field=Age&field=CCDD",
                # Add in syndrome and filter
                category, filter)
  
  data_load <- jsonlite::fromJSON(httr::content(
    httr::GET(url, httr::authenticate(keyring::key_list("essence")[1, 2], 
                          keyring::key_get("essence", keyring::key_list("essence")[1, 2]))), as = "text"))
  
  df <- data_load$dataDetails
  
  # Add in details to the data frame
  df <- df %>%
    dplyr::mutate(condition = condition,
           setting = setting)
  
  return(df)
}


