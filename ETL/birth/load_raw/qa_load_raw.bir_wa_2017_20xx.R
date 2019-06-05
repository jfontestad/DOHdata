###############################################################################
# Alastair Matheson
# 2019-06

# Code to QA load_raw.bir_wa_2017_20xx

###############################################################################


qa_load_raw_bir_wa_2017_20xx_f <- function(conn = db_apde,
                                           load_only = F) {
  
  # If this is the first time ever loading data, only load values.
  #   Otherwise, check against existing QA values
  
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  # List of years
  years <- as.list(seq(2017, 2017))
  
  # Rows in current table
  row_count_tot <- as.numeric(dbGetQuery(conn, 
                                         "SELECT COUNT (*) FROM load_raw.bir_wa_2017_20xx"))
  
  # Rows by year
  row_count_yr <- dbGetQuery(conn, 
                             "SELECT dob_yr, COUNT (*) AS count FROM load_raw.bir_wa_2017_20xx
                                   GROUP BY dob_yr")

  
  #### RUN COMPARISONS TO PREVIOUS DATA IF REQUIRED ####
  ### NB. These have been coded but not yet fully tested
  
  if (load_only == F) {
    ### Pull out min/max etl_batch_id
    batch_etl_id_min <- dbGetQuery(conn, "SELECT MIN (batch_etl_id) FROM load_raw.bir_wa_2017_20xx")
    batch_etl_id_max <- dbGetQuery(conn, "SELECT MAX (batch_etl_id) FROM load_raw.bir_wa_2017_20xx")
    
    
    #### COUNT OVERALL NUMBER OF ROWS ####
    # Pull in the reference value (rows overall)
    previous_row_count_tot <- as.numeric(
      dbGetQuery(conn, 
                 glue_sql("SELECT a.qa_value FROM
                          (SELECT * FROM metadata.qa_bir_values
                            WHERE table_name = 'load_raw.bir_wa_2017_20xx' AND
                            qa_item = 'row_count' AND
                            date_min = '2017-01-01' AND
                            date_max = '{max_yr}-12-31') a
                          INNER JOIN
                          (SELECT MAX(qa_date) AS max_date, date_min, date_max 
                            FROM metadata.qa_bir_values
                            WHERE table_name = 'load_raw.bir_wa_2017_20xx' AND
                            qa_item = 'row_count' AND
                            date_min = '2017-01-01' AND
                            date_max = '{max_yr}-12-31'
                            GROUP BY date_min, date_max) b
                          ON a.qa_date = b.max_date AND 
                          a.date_min = b.date_min AND
                          a.date_max = b.date_max",
                          max_yr = as.numeric(years[length(years)]),
                          .con = conn)))
    
    row_diff <- row_count_tot != previous_row_count_tot
    
    if (row_diff == T) {
      row_diff_msg <- glue(
        "Warning: row counts differed in the most recent load of 2017-20xx data \n
      ({row_count_tot} in current load vs. {previous_row_count_tot} previously) \n
      \n
      Was this expected behavior? \n
      Select 'Yes' to pass QA and update reference values. \n
      Select 'No' or 'Cancel' to fail QA and investigate further")
      
      row_diff_proceed <- askYesNo(msg = row_diff_msg)
      
      if (row_diff_proceed == F | is.na(row_diff_proceed)) {
        dbGetQuery(
          conn = conn,
          glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({batch_etl_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_2017_20xx',
                           'Number overall rows compared to most recent run', 
                           'FAIL', 
                           {Sys.time()}, 
                           'Overall row counts differed in the most recent load 
                           ({row_count_tot} vs. {previous_row_count_tot} previously)  
                           (maximum batch_etl_id shown, 
                           range {batch_etl_id_min}:{batch_etl_id_max})')",
                   max_yr = as.numeric(years[length(years)]),
                   .con = conn))
        
        stop(glue("Overall row counts differed from last time. 
                        Check metadata.qa_bir for details"))
      } else {
        dbGetQuery(
          conn = conn,
          glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({batch_etl_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_2017_20xx',
                           'Number new rows compared to most recent run', 
                           'PASS', 
                           {Sys.time()}, 
                           'There were {row_count_tot} rows in the most recent table 
                           (vs. {previous_row_count_tot} previously) 
                           (maximum batch_etl_id shown, 
                           range {batch_etl_id_min}:{batch_etl_id_max})')",
                   max_yr = as.numeric(years[length(years)]),
                   .con = conn))
      }
    } else {
      dbGetQuery(
        conn = conn,
        glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({batch_etl_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_2017_20xx',
                           'Number new rows compared to most recent run', 
                           'PASS', 
                           {Sys.time()}, 
                           'There were {row_count_tot} rows in the most recent table 
                           (vs. {previous_row_count_tot} previously) 
                           (maximum batch_etl_id shown, 
                           range {batch_etl_id_min}:{batch_etl_id_max})')",
                 max_yr = as.numeric(years[length(years)]),
                 .con = conn))
    }
    
    
    #### COUNT NUMBER OF ROWS BY YEAR ####
    # Pull in the reference value (for each year)
    previous_row_count_yr <- lapply(years, function(x) {
      
      year_sql <- glue_sql("SELECT year(a.date_min) AS dob_yr, a.qa_value FROM
                                 (SELECT * FROM metadata.qa_bir_values
                                   WHERE table_name = 'load_raw.bir_wa_2017_20xx' AND
                                   qa_item = 'row_count' AND
                                   date_min = '{x}-01-01' AND
                                   date_max = '{x}-12-31') a
                                 INNER JOIN
                                 (SELECT MAX(qa_date) AS max_date, date_min, date_max  
                                   FROM metadata.qa_bir_values
                                   WHERE table_name = 'load_raw.bir_wa_2017_20xx' AND
                                   qa_item = 'row_count' AND
                                   date_min = '{x}-01-01' AND
                                   date_max = '{x}-12-31'
                                   GROUP BY date_min, date_max) b
                                 ON a.qa_date = b.max_date AND 
                                 a.date_min = b.date_min AND
                                 a.date_max = b.date_max",
                           .con = conn)
      
      result <- as.numeric(dbGetQuery(conn, year_sql))
      return(result)
    })
    # Turn into a data frame
    previous_row_count_yr <- data.frame(
      dob_yr = sapply(previous_row_count_yr, function(x) x[1]),
      count_prev = sapply(previous_row_count_yr, function(x) x[2]))
    
    # Join to current counts
    previous_row_count_yr <- left_join(previous_row_count_yr, row_count_yr, by = "dob_yr")
    
    if (all(row_count_yr$count == previous_row_count_yr$count_prev) == F) {
      # Find which years are not equal
      row_count_yr_mismatch <- previous_row_count_yr %>%
        filter(count_prev != count)
      
      
      row_diff_msg <- glue(
        "Warning: row counts for some years differed in the most recent load of 2017-20xx data \n
        The following years were mismatched: \n
        {glue_collapse(glue_data(row_count_yr_mismatch, 
                              '{dob_yr}: {count_prev} previously vs {count} now'),
                            sep = '\n')}
      \n
      Was this expected behavior? \n
      Select 'Yes' to pass QA and update reference values. \n
      Select 'No' or 'Cancel' to fail QA and investigate further")
      
      row_diff_proceed <- askYesNo(msg = row_diff_msg)
      
      if (row_diff_proceed == F | is.na(row_diff_proceed)) {
        dbGetQuery(
          conn = conn,
          glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({batch_etl_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_2017_20xx',
                           'Number rows compared to most recent run BY YEAR', 
                           'FAIL', 
                           {Sys.time()}, 
                           'Row counts differed in the most recent load for the following years: {row_count_yr_mismatch$dob_yr*} 
                           (maximum batch_etl_id shown, range {batch_etl_id_min}:{batch_etl_id_max})')",
                   max_yr = as.numeric(years[length(years)]),
                   .con = conn))
        
        stop(glue("Row counts differed from last time in some years. 
                        Check metadata.qa_bir for details"))
      } else {
        dbGetQuery(
          conn = conn,
          glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({batch_etl_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_2017_20xx',
                           'Number rows compared to most recent run BY YEAR', 
                           'PASS', 
                           {Sys.time()}, 
                           'Some differences in row counts but these were expected  
                           (maximum batch_etl_id shown, range {batch_etl_id_min}:{batch_etl_id_max})')",
                   max_yr = as.numeric(years[length(years)]),
                   .con = conn))
      }
    } else {
      dbGetQuery(
        conn = conn,
        glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({batch_etl_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_2017_20xx',
                           'Number rows compared to most recent run BY YEAR', 
                           'PASS', 
                           {Sys.time()}, 
                           'Row counts matched (maximum batch_etl_id shown, range {batch_etl_id_min}:{batch_etl_id_max})')",
                 max_yr = as.numeric(years[length(years)]),
                 .con = conn))
    }
  }
  
  #### END COMPARISONS TO PREVIOUS DATA ####
  
  
  
  #### RUN GENERAL QA TASKS ####
  #### RANGE OF YEARS ####
  # Exclude 2009, though this should be corrected earlier
  min_yr <- min(row_count_yr$dob_yr[which(row_count_yr$dob_yr != 9)])
  max_yr <- max(row_count_yr$dob_yr[which(row_count_yr$dob_yr != 9)])
  count_yr <- length(unique(row_count_yr$dob_yr))
  
  if (min_yr != 2003 | max_yr != 2016 | count_yr != 14) {
    dbGetQuery(
      conn = conn,
      glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({batch_etl_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_2017_20xx',
                           'Range of years in the data', 
                           'FAIL', 
                           {Sys.time()}, 
                           'There were {count_yr} years included in the data with a 
                           minimum year of {min_yr} and maximum year of {max_yr} 
                           (should be 14 years from 2017-20xx, though 2009 loads as '9')
                           (maximum batch_etl_id shown, range {batch_etl_id_min}:{batch_etl_id_max})')",
               max_yr = as.numeric(years[length(years)]),
               .con = conn))
    
    stop(glue("Number and range of years outside what was expected. 
                        Check metadata.qa_bir for details"))
  } else {
    dbGetQuery(
      conn = conn,
      glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({batch_etl_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_2017_20xx',
                           'Range of years in the data', 
                           'PASS', 
                           {Sys.time()}, 
                           'There were {count_yr} years included in the data with a 
                           minimum year of {min_yr} and maximum year of {max_yr} (as expected)
                           (maximum batch_etl_id shown, range {batch_etl_id_min}:{batch_etl_id_max})')",
               max_yr = as.numeric(years[length(years)]),
               .con = conn))
  }
  
  #### DUPLICATE CERT NUMBERS ####
  cert_count <- as.numeric(dbGetQuery(conn, 
                                      "SELECT COUNT (DISTINCT certno_e) 
                                            FROM load_raw.bir_wa_2017_20xx"))
  
  if (cert_count != row_count_tot) {
    dbGetQuery(
      conn = conn,
      glue_sql("INSERT INTO metadata.qa_bir
                  (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({batch_etl_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_2017_20xx',
                           'Distinct cert numbers', 
                           'FAIL', 
                           {Sys.time()}, 
                           'There were {cert_count} distinct IDs but {row_count_tot} rows (should be the same)')",
               max_yr = as.numeric(years[length(years)]),
               .con = conn))
    
    stop(glue("Number of birth certs doesn't match number of rows. Check metadata.qa_bir for details."))
  } else {
    dbGetQuery(
      conn = conn,
      glue_sql("INSERT INTO metadata.qa_bir
                  (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({batch_etl_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_2017_20xx',
                           'Distinct cert numbers', 
                           'PASS', 
                           {Sys.time()}, 
                           'The number of distinct birth certificates matched the number of rows ({cert_count})')",
               max_yr = as.numeric(years[length(years)]),
               .con = conn))
    
  }
  
  
  #### NUMBER OF CERTS PER YEAR ####
  row_count_yr_outlier <- row_count_yr %>%
    filter(count <80000 | count > 100000)
  
  if (nrow(row_count_yr_outlier) >= 1) {
    dbGetQuery(
      conn = conn,
      glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({batch_etl_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_2017_20xx',
                           'Number rows BY YEAR', 
                           'FAIL', 
                           {Sys.time()}, 
                           'Row counts fell outside the expected range (80-100k) for the following years: {row_count_yr_outlier$dob_yr*} 
                           (maximum batch_etl_id shown, range {batch_etl_id_min}:{batch_etl_id_max})')",
               max_yr = as.numeric(years[length(years)]),
               .con = conn))
    
    stop(glue("Some row counts fell outside expected range (80-100k). Check metadata.qa_bir for details"))
  } else {
    dbGetQuery(
      conn = conn,
      glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({batch_etl_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_2017_20xx',
                           'Number rows BY YEAR', 
                           'PASS', 
                           {Sys.time()}, 
                           'The number of rows fell in the expected range (80-100k) for all years 
                           (maximum batch_etl_id shown, range {batch_etl_id_min}:{batch_etl_id_max})')",
               max_yr = as.numeric(years[length(years)]),
               .con = conn))
  }
  
  
  
  #### LOAD VALUES TO QA_VALUES TABLE ####
  ### Overall row counts
  load_sql <- glue_sql("INSERT INTO metadata.qa_bir_values
                             (table_name, date_min, date_max, qa_item, 
                               qa_value, qa_date, note) 
                             VALUES ('load_raw.bir_wa_2017_20xx',
                                     '2017-01-01',
                                     '{max_yr}-12-31',
                                     'row_count', 
                                     {row_count_tot}, 
                                     {Sys.time()}, 
                                     '')",
                       max_yr = as.numeric(years[length(years)]),
                       .con = conn)
  
  dbGetQuery(conn = conn, load_sql)
  
  
  ### Row counts by year
  lapply(years, function(x) {
    
    row_count_yr_value <- as.numeric(row_count_yr$count[row_count_yr$dob_yr == x])
    
    load_sql <- glue_sql("INSERT INTO metadata.qa_bir_values
                             (table_name, date_min, date_max, qa_item, 
                               qa_value, qa_date, note) 
                             VALUES ('load_raw.bir_wa_2017_20xx',
                                     '{x}-01-01',
                                     '{x}-12-31',
                                     'row_count', 
                                     {row_count_yr_value}, 
                                     {Sys.time()}, 
                                     '')",
                         .con = conn)
    
    dbGetQuery(conn = conn, load_sql)
    
  })
  
}