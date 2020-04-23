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
output_path <- "//phshare01/cdi_share/Analytics and Informatics Team/Data Requests/2020/372_nCoV Essence Extract"


#### CHECK FILES ####
### See when the daily file was last updated
last_mod <- file.mtime(file.path(output_path, "ndly.csv"))

# Also check weekly file if today is Monday
if (wday(today(), label = F, week_start = getOption("lubridate.week.start", 1)) == 1) {
  monday <- T
  
  last_mod_wkly <- file.mtime(file.path(output_path, "nwkly.csv"))
  
  if (date(last_mod_wkly) != today()) {
    weekly_error <- T
  } else {
    weekly_error <- F
  }
} else {
  monday <- F
  weekly_error <- F
}

if (date(last_mod) != today()) {
  daily_error <- T
} else {
  daily_error <- F
}


#### GENERATE TEXT ####
if (daily_error == T & monday == F) {
  error_text <- "the daily syndromic data file (ndly.csv) was not updated today."
} else if (daily_error == T & monday == T & weekly_error == F) {
  error_text <- paste0("the daily syndromic data file (ndly.csv) was not updated today ", 
                       "(and the weekly code likely failed to even start).")
} else if (daily_error == T & weekly_error == T) {
  error_text <- "both the daily (ndly.csv) and weekly (nwkly.csv) syndromic data files were not updated today."
} else if (daily_error == F & weekly_error == T) {
  error_text <- "the weekly syndromic data file (nwkly.csv) was not updated today (but the daily file (ndly.csv) was)."
} else if (daily_error == F & monday == F) {
  error_text <- "the daily syndromic data file (ndly.csv) was updated today"
} else if (daily_error == F & monday == T & weekly_error == F) {
  error_text <- "both the daily (ndly.csv) and weekly (wkly.csv) syndromic data files were updated today"
}

if (daily_error == T | weekly_error == T) {
  subject <- "Error in daily syndromic data refresh"
  body_text <- paste0("This email is generated if either file's last modified date is not as expected. </p>",
  "<p>Check <a href = 'file:\\\\Phshare01\\cdi_share\\Analytics and Informatics Team\\Data Requests\\2020\\372_nCoV Essence Extract\\From Natasha on March 13'>
  \\\\Phshare01\\cdi_share\\Analytics and Informatics Team\\Data Requests\\2020\\372_nCoV Essence Extract\\From Natasha on March 13</a> to see which files were changed. ",
  "<br>If the pdly-xxx.csv files were modified, the error is likely in the daily script.", 
  "<br>If the daily file was modified but the weekly one was not, look in the weekly section of the code.</p>")
} else {
  subject <- "Daily syndromic data refresh completed successfully"
  body_text <- paste0("Go to <a href = 'https://tableau.kingcounty.gov/#/site/DPH_CDIMMS/views/Essence-SummaryofRespiratorySyndromes-internal/Pg1-CLIED?Set%20frequency=Daily'>", 
                      "https://tableau.kingcounty.gov/#/site/DPH_CDIMMS/views/Essence-SummaryofRespiratorySyndromes-internal/Pg1-CLIED?Set%20frequency=Daily</a>", 
                      " to see the dashboard, check the data extract also completed, and download a PDF for the daily email")
}


#### AUTOMATICALLY EXPORT PDF OF DAILY DATA ####
# NB. The Tableau API does not currently allow for a parameter filter to be applied
#     to a workbook before downloading as a PDF. So only daily data can be automated this way

# Also NB. This approach assumes that the datasource in Tableau is refreshed on schedule.

if (daily_error == F | daily_error == T) {
  ### Connect to REST API
  server_name <- "tableau.kingcounty.gov"
  api <- 3.5
  
  auth_creds <- jsonlite::toJSON(list(credentials = list(
    name = key_list("tableau_server")[["username"]], 
    password = key_get("tableau_server", key_list("tableau_server")[["username"]]), 
    site = list(contentUrl = "DPH_CDIMMS"))), auto_unbox = T)
  
  auth_req <- POST(paste0("https://", server_name, "/", "api/", api, "/", "auth/signin"), 
                   body = auth_creds,
                   config = add_headers(c(accept="application/json", `content-type`="application/json")),
                   encode = "json") %>% stop_for_status(auth_req)
  
  auth_pload <- content(auth_req, "text") %>% fromJSON()
  
  message("Sign in successful")
  
  api_cred <- list(site_id = auth_pload$credentials$site$id, token = auth_pload$credentials$token)
  
  
  # Note: hard coding ESSENCE workbook ID for now, could use another query to dynamically get it
  # Set up query
  pdf_req <- GET(paste0("https://", server_name, "/api/", api, "/sites/", api_cred$site_id, "/workbooks/",
                        "3a1dce3f-4d89-48fb-96ad-e16d5e35add5", "/pdf?type=Letter&orientation=Portrait"),
                 add_headers("x-tableau-auth" = api_cred$token),
                 write_disk(path = paste0(output_path, "/Reports - daily/KC_SYNDROMIC_", Sys.Date(), "_DAILY.pdf"), overwrite = T))
  
  body_text <- paste0(body_text, "<p>An exported pdf of the DAILY version of the Tableau workbook can be found here: <br>",
                      "<a href = '\\phshare01\CDI_SHARE\Analytics and Informatics Team\Data Requests\2020\372_nCoV Essence Extract\Reports - daily'>", 
                      "\\phshare01\CDI_SHARE\Analytics and Informatics Team\Data Requests\2020\372_nCoV Essence Extract\Reports - daily</a></p>")
}


#### SEND EMAIL ####
# Set up COM API
outlook_app <- COMCreate("Outlook.Application")
# Create an email
outlook_mail <- outlook_app$CreateItem(0)
# Configure email
outlook_mail[["To"]] <- "alastair.matheson@kingcounty.gov; jlenahan@kingcounty.gov"
outlook_mail[["subject"]] <- subject
additional_msg <- ""
outlook_mail[["htmlbody"]] <- paste0(
  "<p>This is an automatically generated email to let you know that ", error_text,  "</p>",
  "<p>As a reminder, the daily file should be updated shortly after 4 PM each day and the weekly file each Monday at 4 PM. ", 
  body_text)

# Send email
outlook_mail$Send()

message("Daily check complete")

