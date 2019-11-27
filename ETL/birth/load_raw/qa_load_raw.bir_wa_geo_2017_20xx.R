###############################################################################
# Alastair Matheson
# 2019-06

# Code to QA load_raw.bir_wa_geo_2017_20xx

###############################################################################


qa_load_raw_bir_wa_geo_2017_20xx_f <- function(conn = db_apde,
                                           load_only = F) {
  
  # If this is the first time ever loading data, only load values.
  #   Otherwise, check against existing QA values
  
  
  #### PULL OUT VALUES NEEDED MULTIPLE TIMES ####
  # List of years
  years <- as.list(seq(2017, 2018))
  
  # Rows in current table
  row_count_tot <- as.numeric(dbGetQuery(conn, 
                                         "SELECT COUNT (*) FROM load_raw.bir_wa_geo_2017_20xx"))
  
  # Rows by year
  row_count_yr <- dbGetQuery(conn, 
                             "SELECT a.date_of_birth_year, COUNT (*) as count FROm
                             (SELECT LEFT(birth_cert_encrypt, 4) AS date_of_birth_year FROM load_raw.bir_wa_geo_2017_20xx) a
                             GROUP BY a.date_of_birth_year
                             ORDER BY a.date_of_birth_year")
  row_count_yr <- row_count_yr %>% mutate(date_of_birth_year = as.numeric(date_of_birth_year))
  
  # Min/max etl_batch_id
  etl_batch_id_min <- as.numeric(dbGetQuery(conn, "SELECT MIN (etl_batch_id) FROM load_raw.bir_wa_geo_2017_20xx"))
  etl_batch_id_max <- as.numeric(dbGetQuery(conn, "SELECT MAX (etl_batch_id) FROM load_raw.bir_wa_geo_2017_20xx"))

  
  #### RUN COMPARISONS TO PREVIOUS DATA IF REQUIRED ####
  ### NB. These have been coded but not yet fully tested
  
  if (load_only == F) {
    #### COUNT OVERALL NUMBER OF ROWS ####
    # Pull in the reference value (rows overall)
    previous_row_count_tot <- as.numeric(
      dbGetQuery(conn, 
                 glue_sql("SELECT a.qa_value FROM
                          (SELECT * FROM metadata.qa_bir_values
                            WHERE table_name = 'load_raw.bir_wa_geo_2017_20xx' AND
                            qa_item = 'row_count' AND
                            date_min = '2017-01-01' AND
                            date_max = '{max_yr}-12-31') a
                          INNER JOIN
                          (SELECT MAX(qa_date) AS max_date, date_min, date_max 
                            FROM metadata.qa_bir_values
                            WHERE table_name = 'load_raw.bir_wa_geo_2017_20xx' AND
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
                   VALUES ({etl_batch_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_geo_2017_20xx',
                           'Number overall rows compared to most recent run', 
                           'FAIL', 
                           {Sys.time()}, 
                           'Overall row counts differed in the most recent load 
                           ({row_count_tot} vs. {previous_row_count_tot} previously)  
                           (maximum etl_batch_id shown, 
                           range {etl_batch_id_min}:{etl_batch_id_max})')",
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
                   VALUES ({etl_batch_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_geo_2017_20xx',
                           'Number new rows compared to most recent run', 
                           'PASS', 
                           {Sys.time()}, 
                           'There were {row_count_tot} rows in the most recent table 
                           (vs. {previous_row_count_tot} previously) 
                           (maximum etl_batch_id shown, 
                           range {etl_batch_id_min}:{etl_batch_id_max})')",
                   max_yr = as.numeric(years[length(years)]),
                   .con = conn))
      }
    } else {
      dbGetQuery(
        conn = conn,
        glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({etl_batch_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_geo_2017_20xx',
                           'Number new rows compared to most recent run', 
                           'PASS', 
                           {Sys.time()}, 
                           'There were {row_count_tot} rows in the most recent table 
                           (vs. {previous_row_count_tot} previously) 
                           (maximum etl_batch_id shown, 
                           range {etl_batch_id_min}:{etl_batch_id_max})')",
                 max_yr = as.numeric(years[length(years)]),
                 .con = conn))
    }
    
    
    #### COUNT NUMBER OF ROWS BY YEAR ####
    # Pull in the reference value (for each year)
    previous_row_count_yr <- lapply(years, function(x) {
      
      year_sql <- glue_sql("SELECT year(a.date_min) AS date_of_birth_year, a.qa_value FROM
                                 (SELECT * FROM metadata.qa_bir_values
                                   WHERE table_name = 'load_raw.bir_wa_geo_2017_20xx' AND
                                   qa_item = 'row_count' AND
                                   date_min = '{x}-01-01' AND
                                   date_max = '{x}-12-31') a
                                 INNER JOIN
                                 (SELECT MAX(qa_date) AS max_date, date_min, date_max  
                                   FROM metadata.qa_bir_values
                                   WHERE table_name = 'load_raw.bir_wa_geo_2017_20xx' AND
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
      date_of_birth_year = sapply(previous_row_count_yr, function(x) x[1]),
      count_prev = sapply(previous_row_count_yr, function(x) x[2]))
    
    # Join to current counts
    previous_row_count_yr <- left_join(previous_row_count_yr, row_count_yr, by = "date_of_birth_year")
    
    if (all(row_count_yr$count == previous_row_count_yr$count_prev) == F) {
      # Find which years are not equal
      row_count_yr_mismatch <- previous_row_count_yr %>%
        filter(count_prev != count)
      
      
      row_diff_msg <- glue(
        "Warning: row counts for some years differed in the most recent load of 2017-20xx data \n
        The following years were mismatched: \n
        {glue_collapse(glue_data(row_count_yr_mismatch, 
                              '{date_of_birth_year}: {count_prev} previously vs {count} now'),
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
                   VALUES ({etl_batch_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_geo_2017_20xx',
                           'Number rows compared to most recent run BY YEAR', 
                           'FAIL', 
                           {Sys.time()}, 
                           'Row counts differed in the most recent load for the following years: ",
                            "{row_count_yr_mismatch$date_of_birth_year*} ",
                            "(maximum etl_batch_id shown, range {etl_batch_id_min}:{etl_batch_id_max})')",
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
                   VALUES ({etl_batch_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_geo_2017_20xx',
                           'Number rows compared to most recent run BY YEAR', 
                           'PASS', 
                           {Sys.time()}, 
                           'Some differences in row counts but these were expected ",
                           "(maximum etl_batch_id shown, range {etl_batch_id_min}:{etl_batch_id_max})')",
                   max_yr = as.numeric(years[length(years)]),
                   .con = conn))
      }
    } else {
      dbGetQuery(
        conn = conn,
        glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({etl_batch_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_geo_2017_20xx',
                           'Number rows compared to most recent run BY YEAR', 
                           'PASS', 
                           {Sys.time()}, 
                           'Row counts matched (maximum etl_batch_id shown, range {etl_batch_id_min}:{etl_batch_id_max})')",
                 max_yr = as.numeric(years[length(years)]),
                 .con = conn))
    }
  }
  
  #### END COMPARISONS TO PREVIOUS DATA ####
  
  
  
  #### RUN GENERAL QA TASKS ####
  #### RANGE OF YEARS ####
  min_yr <- min(row_count_yr$date_of_birth_year)
  max_yr <- max(row_count_yr$date_of_birth_year)
  count_yr <- length(unique(row_count_yr$date_of_birth_year))
  
  if (min_yr != 2017 | max_yr != as.numeric(years[length(years)]) | count_yr != length(years)) {
    dbGetQuery(
      conn = conn,
      glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({etl_batch_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_geo_2017_20xx',
                           'Range of years in the data', 
                           'FAIL', 
                           {Sys.time()}, 
                           'There were {count_yr} years included in the data with a 
                           minimum year of {min_yr} and maximum year of {max_yr} ",
                           "(should be {length(years)} years from 2017-20xx) ",
                           "(maximum etl_batch_id shown, range {etl_batch_id_min}:{etl_batch_id_max})')",
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
                   VALUES ({etl_batch_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_geo_2017_20xx',
                           'Range of years in the data', 
                           'PASS', 
                           {Sys.time()}, 
                           'There were {count_yr} years included in the data with a ",
                            "minimum year of {min_yr} and maximum year of {max_yr} (as expected) ",
                            "(maximum etl_batch_id shown, range {etl_batch_id_min}:{etl_batch_id_max})')",
               max_yr = as.numeric(years[length(years)]),
               .con = conn))
  }
  
  #### DUPLICATE CERT NUMBERS ####
  cert_count <- as.numeric(dbGetQuery(conn, 
                                      "SELECT COUNT (DISTINCT birth_cert_encrypt) 
                                            FROM load_raw.bir_wa_geo_2017_20xx"))
  
  if (cert_count != row_count_tot) {
    dbGetQuery(
      conn = conn,
      glue_sql("INSERT INTO metadata.qa_bir
                  (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({etl_batch_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_geo_2017_20xx',
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
                   VALUES ({etl_batch_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_geo_2017_20xx',
                           'Distinct cert numbers', 
                           'PASS', 
                           {Sys.time()}, 
                           'The number of distinct birth certificates matched the number of rows ({cert_count})')",
               max_yr = as.numeric(years[length(years)]),
               .con = conn))
    
  }
  
  
  #### NUMBER OF CERTS PER YEAR ####
  row_count_yr_outlier <- row_count_yr %>%
    filter(count <25000 | count > 40000)
  
  if (nrow(row_count_yr_outlier) >= 1) {
    dbGetQuery(
      conn = conn,
      glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({etl_batch_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_geo_2017_20xx',
                           'Number rows BY YEAR', 
                           'FAIL', 
                           {Sys.time()}, 
                           'Row counts fell outside the expected range (25-40k) for the following years: ",
                            "{row_count_yr_outlier$date_of_birth_year*} (maximum etl_batch_id shown, range ",
                            "{etl_batch_id_min}:{etl_batch_id_max})')",
               max_yr = as.numeric(years[length(years)]),
               .con = conn))
    
    stop(glue("Some row counts fell outside expected range (25-40k). Check metadata.qa_bir for details"))
  } else {
    dbGetQuery(
      conn = conn,
      glue_sql("INSERT INTO metadata.qa_bir
                   (etl_batch_id, last_run, date_min, date_max, table_name, 
                   qa_item, qa_result, qa_date, note) 
                   VALUES ({etl_batch_id_max}, 
                           NULL,
                           '2017-01-01',
                           '{max_yr}-12-31',
                           'load_raw.bir_wa_geo_2017_20xx',
                           'Number rows BY YEAR', 
                           'PASS', 
                           {Sys.time()}, 
                           'The number of rows fell in the expected range (25-40k) for all years ",
                           "(maximum etl_batch_id shown, range {etl_batch_id_min}:{etl_batch_id_max})')",
               max_yr = as.numeric(years[length(years)]),
               .con = conn))
  }
  
  
  
  #### LOAD VALUES TO QA_VALUES TABLE ####
  ### Overall row counts
  load_sql <- glue_sql("INSERT INTO metadata.qa_bir_values
                             (table_name, date_min, date_max, qa_item, 
                               qa_value, qa_date, note) 
                             VALUES ('load_raw.bir_wa_geo_2017_20xx',
                                     '2017-01-01',
                                     '{max_yr}-12-31',
                                     'row_count', 
                                     {row_count_tot}, 
                                     {Sys.time()}, 
                                     'overall')",
                       max_yr = as.numeric(years[length(years)]),
                       .con = conn)
  
  dbGetQuery(conn = conn, load_sql)
  
  
  ### Row counts by year
  if (length(years) > 1) {
      lapply(years, function(x) {
    
    row_count_yr_value <- as.numeric(row_count_yr$count[row_count_yr$date_of_birth_year == x])
    row_year_value <- as.numeric(x)
    
    load_sql <- glue_sql("INSERT INTO metadata.qa_bir_values
                             (table_name, date_min, date_max, qa_item, 
                               qa_value, qa_date, note) 
                             VALUES ('load_raw.bir_wa_geo_2017_20xx',
                                     '{x}-01-01',
                                     '{x}-12-31',
                                     'row_count', 
                                     {row_count_yr_value}, 
                                     {Sys.time()}, 
                                     '{row_year_value}')",
                         .con = conn)
    
    dbGetQuery(conn = conn, load_sql)
    
  })
    
  }
 
  message("All QA tasks complete. See metadata.qa_bir and metadata.qa_bir_values tables")
   
}