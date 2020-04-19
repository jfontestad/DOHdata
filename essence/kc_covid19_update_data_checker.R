## ---------------------------
##
## Script name: kc_covid19_update_data_checker
##
## Purpose of script: Alert people if the syndromic surveillance report failed
##
## Author: Alastair Matheson, Public Health - Seattle & King County
## Date Created: 2020-04-19
## Email: alastair.matheson@kingcounty.gov
##
## ---------------------------
##
## Notes:
##   
##
## ---------------------------

#### SET OPTIONS AND BRING IN PACKAGES ####
options(scipen = 6, digits = 4, warning.length = 8170)

if (!require("pacman")) {install.packages("pacman")}
pacman::p_load(tidyverse, glue, RDCOMClient, lubridate)

### Set up folder to check
output_path <- "//phshare01/cdi_share/Analytics and Informatics Team/Data Requests/2020/372_nCoV Essence Extract/From Natasha on March 13"

### See when the daily file was last updated
last_mod <- file.mtime(file.path(output_path, "ndly.csv"))

# Also check weekly file if today is Monday
if (wday(today(), label = F, week_start = getOption("lubridate.week.start", 1)) == 1) {
  last_mod_wkly <- file.mtime(file.path(output_path, "nwkly.csv"))
  
  if (date(last_mod_wkly) != today()) {
    weekly_error <- T
  } else {
    weekly_error <- F
  }
} else {
  weekly_error <- F
}

if (date(last_mod) != today() & weekly_error == F) {
  error <- T
  error_text <- "the daily syndromic data file (ndly.csv) was not updated today."
} else if (date(last_mod) != today() & weekly_error == T) {
  error <- T
  error_text <- "both the daily (ndly.csv) and weekly (nwkly.csv) syndromic data files were not updated today."
} else if (date(last_mod) == today() & weekly_error == T) {
  error <- T
  error_text <- "the weekly (nwkly.csv) syndromic data file was not updated today (but the daily file (ndly.csv) was)."
} else {
  error <- F
  error_text <- ""
}


if (error == F) {
  # Set up COM API
  outlook_app <- COMCreate("Outlook.Application")
  # Create an email
  outlook_mail <- outlook_app$CreateItem(0)
  # Configure email
  outlook_mail[["To"]] <- "alastair.matheson@kingcounty.gov; jlenahan@kingcounty.gov"
  outlook_mail[["subject"]] <- "Daily syndromic report failed to run"
  additional_msg <- ""
  outlook_mail[["htmlbody"]] <- paste0(
    "<p>This is an automatically generated email to let you know that ", error_text,  "</p>",
    "<p>As a reminder, the daily file should be updated shortly after 4 PM each day and the weekly file each Monday at 4 PM. ", 
    "This email is generated if either file's last modified date is not as expected. </p>",
    "<p>Check <a href = 'file:\\\\Phshare01\\cdi_share\\Analytics and Informatics Team\\Data Requests\\2020\\372_nCoV Essence Extract\\From Natasha on March 13'>
  \\\\Phshare01\\cdi_share\\Analytics and Informatics Team\\Data Requests\\2020\\372_nCoV Essence Extract\\From Natasha on March 13</a> to see which files were changed. ",
    "<br>If the pdly-xxx.csv files were modified, the error is likely in the daily script.", 
    "<br>If the daily file was modified but the weekly one was not, look in the weekly section of the code.</p>"
  )
  
  # Send email
  outlook_mail$Send()
  
}

