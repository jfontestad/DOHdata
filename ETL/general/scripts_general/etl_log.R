#### FUNCTIONS TO LOAD DATA TO metadata.etl_log TABLE AND RETRIEVE DATA
# Alastair Matheson
# Created:        2019-05-22
# Last modified:  2019-05-22

# Note: these functions are for non-claims data
# Use https://github.com/PHSKC-APDE/claims_data/blob/master/claims_db/db_loader/scripts_general/etl_log.R
#  for claims data

# auto_proceed = T allows skipping of checks against existing ETL entries. 
# Use with caution to avoid creating duplicate entries.
# Note that this will not overwrite checking for near-exact matches. 


load_metadata_etl_log_f <- function(conn = NULL,
                                    data_source = NULL,
                                    date_min = NULL,
                                    date_max = NULL,
                                    date_issue = NULL,
                                    date_delivery = NULL,
                                    note = NULL,
                                    auto_proceed = F) {
  
  #### ERROR CHECKS ####
  if (is.null(conn)) {
    message("No DB connection specificed, trying PH_APDEStore50")
    conn <- odbc::dbConnect(odbc(), "PH_APDEStore50")
  }
  
  if (is.null(data_source) | !data_source %in% c("birth", "birth_geo", "bskhs", "death", "hospital")) {
    stop("Enter a data source (one of the following: 'birth', 'birth_geo', 'bskhs', 
         'death', 'hospital'")
  }
  
  if (is.null(date_min) | is.null(date_max)) {
    stop("Both date_min and date_max must be entered. 
         Use YYYY-01-01 and YYYY-12-31 for full-year data.")
  }
  
  if (is.null(date_issue) | is.null(date_delivery)) {
    stop("Both date_issue and date_delivery must be entered.
         Use YYYY-MM-15 if only a month is known.")
  }
  
  if (is.na(as.Date(as.character(date_min), format = "%Y-%m-%d")) |
      is.na(as.Date(as.character(date_max), format = "%Y-%m-%d")) |
      is.na(as.Date(as.character(date_issue), format = "%Y-%m-%d")) |
      is.na(as.Date(as.character(date_delivery), format = "%Y-%m-%d"))) {
    stop("Dates must be in YYYY-MM-DD format and in quotes")
  }
  
  if (is.null(note)) {
    stop("Enter a note to describe this data")
  }
  
  
  #### CHECK EXISTING ENTRIES ####
  latest <- odbc::dbGetQuery(conn, 
                             "SELECT TOP (1) * FROM metadata.etl_log
                             ORDER BY etl_batch_id DESC")
  
  latest_source <- odbc::dbGetQuery(conn, 
                                    glue::glue_sql(
                                      "SELECT TOP (1) * FROM metadata.etl_log
                                      WHERE data_source = {data_source} 
                                      ORDER BY etl_batch_id DESC",
                                      .con = conn))
  
  matches <- odbc::dbGetQuery(conn, 
                              glue::glue_sql(
                                "SELECT * FROM metadata.etl_log
                                WHERE data_source = {data_source} AND 
                                date_max = {date_max} AND 
                                date_issue = {date_issue} AND
                                date_delivery = {date_delivery}
                                ORDER BY etl_batch_id DESC",
                                .con = conn))
  
  #### SET DEFAULTS ####
  proceed <- T # Move ahead with the load
  if (nrow(latest) > 0) {
    etl_batch_id <- latest$etl_batch_id + 1
  } else {
    etl_batch_id <- 1
  }
  
  
  #### CHECK AGAINST EXISTING ENTRIES ####
  # (assume if there is no record for this data source already then yes)
  if (nrow(matches) > 0) {
    print(matches)
    
    proceed_msg <- glue::glue("There are already entries in the table that \\
                              look similar to what you are attempting to enter. \\
                              See the console window. \n
                              
                              Do you still want to make a new entry?")
    proceed <- askYesNo(msg = proceed_msg)
  } else {
    if (nrow(latest) > 0 & nrow(latest_source) > 0 & auto_proceed == F) {
      proceed_msg <- glue::glue("
                                The most recent entry in the etl_log is as follows:
                                etl_batch_id: {latest[1]}
                                data_source: {latest[2]}
                                date_min: {latest[3]}
                                date_max: {latest[4]}
                                date_issue: {latest[5]}
                                date_delivery: {latest[6]}
                                note: {latest[7]}
                                
                                The most recent entry in the etl_log FOR THIS DATA SOURCE is as follows:
                                etl_batch_id: {latest_source[1]}
                                data_source: {latest_source[2]}
                                date_min: {latest_source[3]}
                                date_max: {latest_source[4]}
                                date_issue: {latest_source[5]}
                                date_delivery: {latest_source[6]}
                                note: {latest_source[7]}
                                
                                Do you still want to make a new entry?",
                                .con = conn)
      
      proceed <- askYesNo(msg = proceed_msg)
      
    }
  }

  
  if (is.na(proceed)) {
    stop("ETL log load cancelled at user request")
    
  } else if (proceed == F & nrow(matches) > 0) {
    reuse <- askYesNo(msg = "Would you like to reuse the most recent existing entry that matches?")
    
    if (reuse == T) {
      etl_batch_id <- matches$etl_batch_id[1]
      
      message(glue::glue("Reusing ETL batch #{etl_batch_id}"))
      return(etl_batch_id)
      
    } else {
      stop("ETL log load cancelled at user request")
    }
    
  } else if (proceed == T) {
    sql_load <- glue::glue_sql(
      "INSERT INTO metadata.etl_log 
      (etl_batch_id, data_source, date_min, date_max, date_issue, date_delivery, note) 
      VALUES ({etl_batch_id}, {data_source}, {date_min}, {date_max}, 
      {date_issue}, {date_delivery}, {note})",
      .con = conn
    )
    
    odbc::dbGetQuery(conn, sql_load)
    
    # Finish with a message and return the latest etl_batch_id
    # (users should be assigning this to a current_batch_id object)
    message(glue::glue("ETL batch #{etl_batch_id} loaded"))
    return(etl_batch_id)
  }
  

  
}


#### FUNCTION TO DISPLAY DATA ASSOCIATED WITH AN ETL_BATCH_ID
# Used to check that the right current_etl_id is being used
retrieve_metadata_etl_log_f <- function(conn = NULL, etl_batch_id = NULL) {
  ### Error checks
  if (is.null(conn)) {
    message("No DB connection specificed, trying PH_APDEStore50")
    conn <- odbc::dbConnect(odbc(), "PH_APDEStore50")
  }
  
  if (is.null(etl_batch_id)) {
    stop("Enter an etl_batch_id")
  }
  
  ### run query
  odbc::dbGetQuery(conn, 
                   glue::glue_sql("SELECT * FROM metadata.etl_log
                                  WHERE etl_batch_id = {etl_batch_id}",
                                  .con = conn))
}