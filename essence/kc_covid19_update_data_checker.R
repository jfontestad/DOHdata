## ---------------------------
##
## Script name: kc_covid19_update_data_checker
##
## Purpose of script: Alert people if the syndromic surveillance report completed
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2020-04-19
## Email: alastair.matheson@kingcounty.gov
##
## ---------------------------
##
## Notes: Requires a credential for Tableau server (called tableau_server) 
##   to be stored in Windows credential manager and accessed via keyring package.
##
## ---------------------------

#### SET OPTIONS AND BRING IN PACKAGES ####
options(scipen = 6, digits = 4, warning.length = 8170)

if (!require("pacman")) {install.packages("pacman")}
pacman::p_load(tidyverse, glue, RDCOMClient, lubridate, keyring, jsonlite, httr)

### Set up folder to check
output_path_pdf <- "//phshare01/cdi_share/Analytics and Informatics Team/Data Requests/2020/372_nCoV Essence Extract"


#### CHECK FILES ####
### See when the daily file was last updated
last_mod_ndly <- file.mtime(file.path(output_path_pdf, "From Natasha on March 13", "ndly.csv"))
last_mod_all_ed <- file.mtime(file.path(output_path_pdf, "From Natasha on March 13", "pdly_full_all_ed_summary.csv"))

# Also check weekly file if today is Tuesday
if (wday(today(), label = F, week_start = getOption("lubridate.week.start", 1)) == 2) {
  tuesday <- T
  
  last_mod_wkly <- file.mtime(file.path(output_path_pdf, "From Natasha on March 13", "nwkly.csv"))
  
  if (date(last_mod_wkly) != today()) {
    weekly_error <- T
  } else {
    weekly_error <- F
  }
} else {
  tuesday <- F
  weekly_error <- F
}

if (date(last_mod_ndly) != today() | date(last_mod_all_ed) != today()) {
  daily_error <- T
} else {
  daily_error <- F
}


#### GENERATE TEXT ####
if (daily_error == T & tuesday == F) {
  error_text <- "one of the daily syndromic data files (ndly.csv, pdly_full_all_ed_sumary.csv) was not updated today."
} else if (daily_error == T & tuesday == T & weekly_error == F) {
  error_text <- paste0("one of the daily syndromic data files (ndly.csv, pdly_full_all_ed_sumary.csv) was not updated today ", 
                       "(and the weekly code likely failed to even start).")
} else if (daily_error == T & weekly_error == T) {
  error_text <- "both the daily (ndly.csv, pdly_full_all_ed_sumary.csv) and weekly (nwkly.csv) syndromic data files were not updated today."
} else if (daily_error == F & weekly_error == T) {
  error_text <- "the weekly syndromic data file (nwkly.csv) was not updated today (but the daily file (ndly.csv) was)."
} else if (daily_error == F & tuesday == F) {
  error_text <- "the daily syndromic data files (ndly.csv, pdly_full_all_ed_sumary.csv) were updated today"
} else if (daily_error == F & tuesday == T & weekly_error == F) {
  error_text <- "the daily (ndly.csv, pdly_full_all_ed_sumary.csv) and weekly (wkly.csv) syndromic data files were all updated today"
}

if (daily_error == T | weekly_error == T) {
  subject <- "Error in daily syndromic data refresh"
  body_text <- paste0("This email is generated if any file's last modified date is not as expected. </p>",
  "<p>Check <a href = 'file:\\\\Phshare01\\cdi_share\\Analytics and Informatics Team\\Data Requests\\2020\\372_nCoV Essence Extract\\From Natasha on March 13'>
  \\\\Phshare01\\cdi_share\\Analytics and Informatics Team\\Data Requests\\2020\\372_nCoV Essence Extract\\From Natasha on March 13</a> to see which files were changed. ",
  "<br>If the pdly-xxx.csv files were modified, the error is likely in the daily script.", 
  "<br>If the daily file was modified but the weekly one was not, look in the weekly section of the code.</p>")
} else {
  subject <- "Daily syndromic data refresh completed successfully"
  body_text <- paste0("<p>Go to <a href = 'https://tableau.kingcounty.gov/#/site/DPH_CDIMMS/views/Essence-SummaryofRespiratorySyndromes-internal/Pg1-CLIED?Set%20frequency=Daily'>", 
                      "https://tableau.kingcounty.gov/#/site/DPH-COVID/views/Essence-SummaryofRespiratorySyndromes-internal/Pg1-CLIED?Set%20frequency=Daily</a>", 
                      " to see the dashboard and check the data extract also completed.</p>")
}


#### AUTOMATICALLY EXPORT PDF OF DAILY DATA ####
# NB. Can't pass a filter to a workbook PDF download so can't automate weekly download
# NB. This approach assumes that the datasource in Tableau is refreshed on schedule.

if (daily_error == F) {
  ### Connect to REST API
  server_name <- "tableau.kingcounty.gov"
  api <- 3.5 # This is specific to Tableau 2019.3. Use API 3.8 when we upgrade to 2020.2
  
  auth_creds <- jsonlite::toJSON(list(credentials = list(
    name = key_list("tableau_server")[["username"]], 
    password = key_get("tableau_server", key_list("tableau_server")[["username"]]), 
    site = list(contentUrl = "DPH-COVID"))), auto_unbox = T)
  
  signin_req <- POST(paste0("https://", server_name, "/", "api/", api, "/", "auth/signin"), 
                   body = auth_creds,
                   config = add_headers(c(accept="application/json", `content-type`="application/json")),
                   encode = "json") %>% stop_for_status(auth_req)
  
  auth_pload <- content(signin_req, "text") %>% fromJSON()
  
  message("Sign in successful")
  
  api_cred <- list(site_id = auth_pload$credentials$site$id, token = auth_pload$credentials$token)
  
  
  ### Note: hard coding ESSENCE workbook ID for now, could use another query to dynamically get it
  ### Here's how to get a list of workbooks/data sources and their IDs
  # workbook_list_response <- GET(paste0("https://", server_name, "/api/", api, "/sites/",
  #                                      api_cred$site_id, "/workbooks/"),
  #                               add_headers("x-tableau-auth" = api_cred$token))
  # workbook_list <- content(workbook_list_response)
  # view_list_response <- GET(paste0("https://", server_name, "/api/", api, "/sites/",
  #                                      api_cred$site_id, "/views/"),
  #                               add_headers("x-tableau-auth" = api_cred$token))
  # view_list <- content(view_list_response)
  # datasource_list_response <- GET(paste0("https://", server_name, "/api/", api, "/sites/",
  #                                        api_cred$site_id, "/datasources/"),
  #                                 add_headers("x-tableau-auth" = api_cred$token))
  # datasource_list <- content(datasource_list_response)
  
  # Check that the data sources refreshed as planned
  ds_alerts <- content(GET(paste0("https://", server_name, "/api/", api, "/sites/", 
                                          api_cred$site_id, "/datasources/1463ec4f-cff7-4b74-bc6b-977f20eb0cde"), 
                                   add_headers("x-tableau-auth" = api_cred$token)))[["datasource"]]
  ds_all_ed_summary <- content(GET(paste0("https://", server_name, "/api/", api, "/sites/", 
                                          api_cred$site_id, "/datasources/4ec3d06b-f1b3-4390-b32b-c2bc87d86594"), 
                                   add_headers("x-tableau-auth" = api_cred$token)))[["datasource"]]
  
  # Extract last update time
  ds_alerts_update <- with_tz(ymd_hms(ds_alerts$updatedAt), tzone = "America/Los_Angeles")
  ds_all_ed_update <- with_tz(ymd_hms(ds_all_ed_summary$updatedAt), tzone = "America/Los_Angeles")
  
  
  # If extracts haven't refreshed, issue a warning
  # (could trigger a refresh via the API but a human should look at why it wasn't refreshed)
  
  if (ds_alerts_update >= last_mod_ndly & ds_all_ed_update >= last_mod_all_ed) {
    # Set up query
    pdf_req <- GET(paste0("https://", server_name, "/api/", api, "/sites/", api_cred$site_id, "/workbooks/",
                          "be998f6f-2604-46b3-9fd6-f9a45e74443f", "/pdf?type=Letter&orientation=Portrait"),
                   add_headers("x-tableau-auth" = api_cred$token),
                   write_disk(path = paste0(output_path_pdf, "/Reports - daily/KC_SYNDROMIC_", 
                                            Sys.Date(), "_DAILY.pdf"), overwrite = T))
    
    
    #### TEST
    # pdf_maker <- function(freq = c("daily", "weekly")) {
    #   # Set up daily/weekly names
    #   freq <- match.arg(freq)
    #   if (freq == "daily") {freq_file <- "DAILY"} else {freq_file <- "WEEKLY"}
    #   
    #   # Set up part of URI
    #   uri_time <- paste0("https://", server_name, "/api/", api, "/sites/", api_cred$site_id, "/views/",
    #                        "b6c46369-ebcb-4b3e-81c4-ec0ad0f59249", "/pdf", 
    #                        "?type=Letter&orientation=Portrait&maxAge=1")
    #   uri_demog <- paste0("https://", server_name, "/api/", api, "/sites/", api_cred$site_id, "/views/",
    #                      "2c9085bd-0259-486e-b73c-1db305e00f5d", "/pdf", 
    #                      "?type=Letter&orientation=Portrait&maxAge=1")
    #   uri_map <- paste0("https://", server_name, "/api/", api, "/sites/", api_cred$site_id, "/views/",
    #                      "dfd45fb5-f54a-41a3-8d8d-394a7da005c1", "/pdf", 
    #                      "?type=Letter&orientation=Portrait&maxAge=1")
    #   uri_note <- paste0("https://", server_name, "/api/", api, "/sites/", api_cred$site_id, "/views/",
    #                     "e3d176c1-eff8-4ed6-b2ee-350f2b109350", "/pdf", 
    #                     "?type=Letter&orientation=Portrait&maxAge=1")
    #   
    #   # Set up file output
    #   file_path <- function(pg = 1) {
    #     path <- paste0(output_path_pdf, "/Reports - ", freq, "/KC_SYNDROMIC_", Sys.Date(), 
    #                    "_", freq_file, "_p", pg, ".pdf")
    #   }
    #   
    #   
    #   ### CLI TIME SERIES
    #   # Produce all and age pages for ED CLI
    #   GET(paste0(uri_time, "&vf_query=cli&vf_Set%20frequency=", freq, "&vf_Category=all",
    #              "&vf_All%20ED%20visits%20or%20hospitalizations=ed"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(1), overwrite = T))
    #   
    #   GET(paste0(uri_time, "&vf_query=cli&vf_Set%20frequency=", freq, "&vf_Category=age_grp",
    #              "&vf_All%20ED%20visits%20or%20hospitalizations=ed"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(2), overwrite = T))
    #   
    #   # Race/eth page for ED CLI (always weekly even in daily runs)
    #   GET(paste0(uri_time, "&vf_query=cli&vf_Set%20frequency=weekly&vf_Category=race_eth",
    #              "&vf_All%20ED%20visits%20or%20hospitalizations=ed"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(3), overwrite = T))
    #   
    #   # Produce all and age for CLI hospitalizations
    #   GET(paste0(uri_time, "&vf_query=cli&vf_Set%20frequency=weekly&vf_Category=all",
    #              "&vf_All%20ED%20visits%20or%20hospitalizations=hosp"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(4), overwrite = T))
    #   
    #   GET(paste0(uri_time, "&vf_query=cli&vf_Set%20frequency=", freq, "&vf_Category=age_grp",
    #              "&vf_All%20ED%20visits%20or%20hospitalizations=hosp"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(5), overwrite = T))
    #   
    #   # Race/eth page for CLI hosp (always weekly even in daily runs)
    #   GET(paste0(uri_time, "&vf_query=cli&vf_Set%20frequency=weekly&vf_Category=race_eth",
    #              "&vf_All%20ED%20visits%20or%20hospitalizations=hosp"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(6), overwrite = T))
    #   
    #   
    #   ### PNEUMO TIME SERIES
    #   # Produce all and age pages for ED CLI
    #   GET(paste0(uri_time, "&vf_query=Pneumonia%20only&vf_Set%20frequency=", freq, "&vf_Category=all",
    #              "&vf_All%20ED%20visits%20or%20hospitalizations=ed"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(7), overwrite = T))
    #   
    #   GET(paste0(uri_time, "&vf_query=Pneumonia%20only&vf_Set%20frequency=", freq, "&vf_Category=age_grp",
    #              "&vf_All%20ED%20visits%20or%20hospitalizations=ed"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(8), overwrite = T))
    #   
    #   # Produce all and age for CLI hospitalizations
    #   GET(paste0(uri_time, "&vf_query=pneumo&vf_Set%20frequency=", freq, "&vf_Category=all",
    #              "&vf_All%20ED%20visits%20or%20hospitalizations=hosp"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(9), overwrite = T))
    #   
    #   GET(paste0(uri_time, "&vf_query=pneumo&vf_Set%20frequency=", freq, "&vf_Category=age_grp",
    #              "&vf_All%20ED%20visits%20or%20hospitalizations=hosp"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(10), overwrite = T))
    #   
    #   
    #   ### ALL VISITS
    #   # ED
    #   GET(paste0(uri_time, "&vf_query=all&vf_Set%20frequency=", freq, "&vf_Category=all",
    #              "&vf_All%20ED%20visits%20or%20hospitalizations=ed"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(11), overwrite = T))
    #   
    #   GET(paste0(uri_time, "&vf_query=all&vf_Set%20frequency=", freq, "&vf_Category=age_grp",
    #              "&vf_All%20ED%20visits%20or%20hospitalizations=ed"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(12), overwrite = T))
    #   
    #   # Hosp
    #   GET(paste0(uri_time, "&vf_query=all&vf_Set%20frequency=", freq, "&vf_Category=all",
    #              "&vf_All%20ED%20visits%20or%20hospitalizations=hosp"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(13), overwrite = T))
    #   
    #   GET(paste0(uri_time, "&vf_query=all&vf_Set%20frequency=", freq, "&vf_Category=age_grp",
    #              "&vf_All%20ED%20visits%20or%20hospitalizations=hosp"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(14), overwrite = T))
    #   
    #   
    #   ### CLI DEMOG BREAKDOWN (always weekly)
    #   # Produce race breakdown for testing
    #   GET(paste0(uri_demog, "&vf_query=cli&vf_Set%20frequency=weekly&vf_Category=race_eth"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(15), overwrite = T))
    #   
    #   ### CLI MAP (always weekly)
    #   # Testing among all visits
    #   GET(paste0(uri_map, "&vf_query=all&vf_Set%20frequency=weekly",
    #              "vf_Choose%20what%20to%20display%20on%20map=tests"),
    #       add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(16), overwrite = T))
    #   
    #   ### NOTES
    #   GET(uri_note, add_headers("x-tableau-auth" = api_cred$token),
    #       write_disk(path = file_path(17), overwrite = T))
    #   
    # }
    # 
    # 
    # pdf_maker(freq = "daily")
    
    
    #### END TEST ####
    
    
    
    body_text <- paste0(body_text, "<p>An exported pdf of the DAILY version of the Tableau workbook can be found here: <br>",
                        "<a href = 'file:\\\\Phshare01\\cdi_share\\Analytics and Informatics Team\\Data Requests\\2020\\372_nCoV Essence Extract\\Reports - daily'>", 
                        "\\\\Phshare01\\cdi_share\\Analytics and Informatics Team\\Data Requests\\2020\\372_nCoV Essence Extract\\Reports - daily</a></p>")
  } else {
    body_text <- paste0(body_text, "<p>The Tableau data extracts were not refreshed after the new data was exported ",
                        "so a PDF was not downloaded.")
  }
  
}


#### SEND EMAIL ####
# Set up COM API
outlook_app <- COMCreate("Outlook.Application")
# Create an email
outlook_mail <- outlook_app$CreateItem(0)
# Configure email
outlook_mail[["To"]] <- "alastair.matheson@kingcounty.gov" #; jlenahan@kingcounty.gov"
outlook_mail[["subject"]] <- subject
additional_msg <- ""
outlook_mail[["htmlbody"]] <- paste0(
  "<p>This is an automatically generated email to let you know that ", error_text,  "</p>",
  "<p>As a reminder, the daily file should be updated shortly after 4 PM each day and the weekly file each Tuesday at 4 PM. ", 
  body_text)

# Send email
outlook_mail$Send()

#### DISCONNECT FROM TABLEAU SERVER ####
signout_req <- POST(paste0("https://", server_name, "/", "api/", api, "/", "auth/signout"), 
                 config = add_headers(c(accept="application/json", `content-type`="application/json")),
                 encode = "json")

message("Daily check complete")

